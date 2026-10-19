package redis

import (
	"context"
	"distributed-queue/pkg/config"
	"distributed-queue/pkg/redis/metrics"
	"fmt"
	"strconv"
	"sync"
	"time"

	redisclient "github.com/redis/go-redis/v9"
)

// Delivery is one physical broker delivery. TaskID is the logical job ID;
// DeliveryID and Generation identify a particular durable publication.
type Delivery struct {
	StreamID   string
	TaskID     string
	DeliveryID string
	Generation int
}

type RedisStore struct {
	rdb      *redisclient.Client
	cursorMu sync.Mutex
	cursors  map[string]string
}

func NewRedisStore(ctx context.Context, cfg config.RedisConfig, _ time.Duration) (*RedisStore, error) {
	rdb := redisclient.NewClient(&redisclient.Options{
		Addr: cfg.Address, Password: cfg.Password, DB: cfg.DB, Protocol: 2,
	})
	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close() // Preserve the connection error if cleanup also fails.
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}
	return &RedisStore{rdb: rdb, cursors: make(map[string]string)}, nil
}

func (s *RedisStore) Close() error { return s.rdb.Close() }

func streamKey(queueName string) string { return queueName + ":stream" }
func groupName(queueName string) string { return queueName + ":workers" }
func dlqKey(queueName string) string    { return queueName + ":dlq" }

func (s *RedisStore) EnsureConsumerGroup(ctx context.Context, queueName string) error {
	err := s.rdb.XGroupCreateMkStream(ctx, streamKey(queueName), groupName(queueName), "0").Err()
	if err != nil && !isBusyGroup(err) {
		metrics.StoreErrors.WithLabelValues(queueName, "EnsureConsumerGroup").Inc()
		return fmt.Errorf("create stream consumer group: %w", err)
	}
	return nil
}

func isBusyGroup(err error) bool {
	return err != nil && len(err.Error()) >= len("BUSYGROUP") && err.Error()[:len("BUSYGROUP")] == "BUSYGROUP"
}

// PublishDelivery appends an immutable delivery record. If the relay crashes
// before recording its outbox row as delivered, it can append this same record
// again; claim-time generation checks make that safe.
func (s *RedisStore) PublishDelivery(ctx context.Context, queueName, taskID, deliveryID string, generation int) error {
	_, err := s.rdb.XAdd(ctx, &redisclient.XAddArgs{
		Stream: streamKey(queueName),
		Values: map[string]any{
			"task_id": taskID, "delivery_id": deliveryID, "generation": strconv.Itoa(generation),
		},
	}).Result()
	if err != nil {
		metrics.StoreErrors.WithLabelValues(queueName, "PublishDelivery").Inc()
		return fmt.Errorf("append delivery to stream: %w", err)
	}
	metrics.TasksEnqueued.WithLabelValues(queueName).Inc()
	return nil
}

// ConsumeDelivery atomically adds a Stream entry to the consumer group's PEL.
// Unlike the old BLMOVE + HSET implementation, a crash cannot create an
// untracked inflight delivery.
func (s *RedisStore) ConsumeDelivery(ctx context.Context, queueName, consumer string) (Delivery, error) {
	streams, err := s.rdb.XReadGroup(ctx, &redisclient.XReadGroupArgs{
		Group: groupName(queueName), Consumer: consumer,
		Streams: []string{streamKey(queueName), ">"}, Count: 1, Block: time.Second,
	}).Result()
	if err == redisclient.Nil || len(streams) == 0 || len(streams[0].Messages) == 0 {
		return Delivery{}, nil
	}
	if err != nil {
		metrics.StoreErrors.WithLabelValues(queueName, "ConsumeDelivery").Inc()
		return Delivery{}, fmt.Errorf("read stream delivery: %w", err)
	}
	delivery, err := parseDelivery(streams[0].Messages[0])
	if err != nil {
		metrics.StoreErrors.WithLabelValues(queueName, "ConsumeDelivery").Inc()
		return Delivery{}, err
	}
	metrics.TasksConsumed.WithLabelValues(queueName).Inc()
	return delivery, nil
}

func (s *RedisStore) AcknowledgeDelivery(ctx context.Context, queueName, streamID string) error {
	_, err := s.rdb.XAck(ctx, streamKey(queueName), groupName(queueName), streamID).Result()
	if err != nil {
		metrics.StoreErrors.WithLabelValues(queueName, "AcknowledgeDelivery").Inc()
		return fmt.Errorf("ack stream delivery %s: %w", streamID, err)
	}
	metrics.TasksAcknowledged.WithLabelValues(queueName).Inc()
	return nil
}

// TouchDelivery resets PEL idle time while the worker is healthy. It is
// supplementary to the Postgres lease, which remains the fencing authority.
func (s *RedisStore) TouchDelivery(ctx context.Context, queueName, consumer, streamID string) error {
	_, err := s.rdb.XClaim(ctx, &redisclient.XClaimArgs{
		Stream: streamKey(queueName), Group: groupName(queueName), Consumer: consumer,
		MinIdle: 0, Messages: []string{streamID},
	}).Result()
	if err != nil {
		metrics.StoreErrors.WithLabelValues(queueName, "TouchDelivery").Inc()
		return fmt.Errorf("renew stream delivery %s: %w", streamID, err)
	}
	return nil
}

// ClaimIdleDeliveries transfers idle PEL entries to the janitor. The cursor is
// retained per queue so successive batches scan the full PEL before wrapping.
// The caller must still consult Postgres before acknowledging or replacing a
// delivery.
func (s *RedisStore) ClaimIdleDeliveries(ctx context.Context, queueName string, minIdle time.Duration, count int64) ([]Delivery, error) {
	s.cursorMu.Lock()
	defer s.cursorMu.Unlock()
	start := s.cursors[queueName]
	if start == "" {
		start = "0-0"
	}

	messages, next, err := s.rdb.XAutoClaim(ctx, &redisclient.XAutoClaimArgs{
		Stream: streamKey(queueName), Group: groupName(queueName), Consumer: "janitor",
		MinIdle: minIdle, Start: start, Count: count,
	}).Result()
	if err == redisclient.Nil {
		return nil, nil
	}
	if err != nil {
		metrics.StoreErrors.WithLabelValues(queueName, "ClaimIdleDeliveries").Inc()
		return nil, fmt.Errorf("claim idle stream deliveries: %w", err)
	}
	if next == "0-0" {
		delete(s.cursors, queueName)
	} else {
		s.cursors[queueName] = next
	}
	deliveries := make([]Delivery, 0, len(messages))
	for _, message := range messages {
		delivery, err := parseDelivery(message)
		if err != nil {
			metrics.StoreErrors.WithLabelValues(queueName, "ClaimIdleDeliveries").Inc()
			continue
		}
		deliveries = append(deliveries, delivery)
	}
	return deliveries, nil
}

func (s *RedisStore) MoveToDeadLetterQueue(ctx context.Context, queueName string, delivery Delivery) error {
	_, err := s.rdb.XAdd(ctx, &redisclient.XAddArgs{Stream: dlqKey(queueName), Values: map[string]any{
		"task_id": delivery.TaskID, "delivery_id": delivery.DeliveryID,
		"generation": strconv.Itoa(delivery.Generation),
	}}).Result()
	if err != nil {
		metrics.StoreErrors.WithLabelValues(queueName, "MoveToDeadLetterQueue").Inc()
		return fmt.Errorf("append delivery to DLQ: %w", err)
	}
	metrics.TasksDeadLettered.WithLabelValues(queueName).Inc()
	return nil
}

func (s *RedisStore) CollectDepths(ctx context.Context, queueName string) error {
	streamLen, err := s.rdb.XLen(ctx, streamKey(queueName)).Result()
	if err != nil {
		return fmt.Errorf("read stream depth: %w", err)
	}
	pending, err := s.rdb.XPending(ctx, streamKey(queueName), groupName(queueName)).Result()
	if err != nil && err != redisclient.Nil {
		return fmt.Errorf("read pending stream depth: %w", err)
	}
	dlqLen, err := s.rdb.XLen(ctx, dlqKey(queueName)).Result()
	if err != nil && err != redisclient.Nil {
		return fmt.Errorf("read DLQ depth: %w", err)
	}
	metrics.QueueDepth.WithLabelValues(queueName).Set(float64(streamLen))
	metrics.InflightDepth.WithLabelValues(queueName).Set(float64(pending.Count))
	metrics.DelayedDepth.WithLabelValues(queueName).Set(0)
	metrics.DLQDepth.WithLabelValues(queueName).Set(float64(dlqLen))
	return nil
}

func parseDelivery(message redisclient.XMessage) (Delivery, error) {
	taskID, ok := message.Values["task_id"].(string)
	if !ok || taskID == "" {
		return Delivery{}, fmt.Errorf("stream entry %s has no task_id", message.ID)
	}
	deliveryID, ok := message.Values["delivery_id"].(string)
	if !ok || deliveryID == "" {
		return Delivery{}, fmt.Errorf("stream entry %s has no delivery_id", message.ID)
	}
	generationText, ok := message.Values["generation"].(string)
	if !ok {
		return Delivery{}, fmt.Errorf("stream entry %s has no generation", message.ID)
	}
	generation, err := strconv.Atoi(generationText)
	if err != nil {
		return Delivery{}, fmt.Errorf("stream entry %s has invalid generation: %w", message.ID, err)
	}
	return Delivery{StreamID: message.ID, TaskID: taskID, DeliveryID: deliveryID, Generation: generation}, nil
}
