package redis

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	redisclient "github.com/redis/go-redis/v9"
)

func TestParseDelivery(t *testing.T) {
	delivery, err := parseDelivery(redisclient.XMessage{ID: "42-0", Values: map[string]any{
		"task_id": "task-1", "delivery_id": "delivery-1", "generation": "3",
	}})
	if err != nil {
		t.Fatalf("parse delivery: %v", err)
	}
	if delivery.StreamID != "42-0" || delivery.TaskID != "task-1" || delivery.DeliveryID != "delivery-1" || delivery.Generation != 3 {
		t.Fatalf("unexpected delivery: %#v", delivery)
	}
}

func TestClaimIdleDeliveriesAdvancesCursor(t *testing.T) {
	if os.Getenv("RUN_REDIS_INTEGRATION") != "1" {
		t.Skip("set RUN_REDIS_INTEGRATION=1 and TEST_REDIS_ADDRESS to run Redis integration tests")
	}
	addr := os.Getenv("TEST_REDIS_ADDRESS")
	if addr == "" {
		t.Fatal("TEST_REDIS_ADDRESS is required when RUN_REDIS_INTEGRATION=1")
	}

	ctx := context.Background()
	rdb := redisclient.NewClient(&redisclient.Options{Addr: addr})
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Fatalf("connect Redis: %v", err)
	}
	queueName := fmt.Sprintf("queue:cursor-test:%d:stream", time.Now().UnixNano())
	store := &RedisStore{rdb: rdb, cursors: make(map[string]string)}
	t.Cleanup(func() {
		_ = rdb.Del(ctx, streamKey(queueName)).Err()
		_ = rdb.Close()
	})

	for i := 0; i < 3; i++ {
		if err := store.PublishDelivery(ctx, queueName, fmt.Sprintf("task-%d", i), fmt.Sprintf("delivery-%d", i), 0); err != nil {
			t.Fatalf("publish delivery %d: %v", i, err)
		}
	}
	if err := store.EnsureConsumerGroup(ctx, queueName); err != nil {
		t.Fatalf("create consumer group: %v", err)
	}
	if _, err := rdb.XReadGroup(ctx, &redisclient.XReadGroupArgs{
		Group: groupName(queueName), Consumer: "stopped-worker",
		Streams: []string{streamKey(queueName), ">"}, Count: 3,
	}).Result(); err != nil {
		t.Fatalf("create pending deliveries: %v", err)
	}

	first, err := store.ClaimIdleDeliveries(ctx, queueName, 0, 1)
	if err != nil || len(first) != 1 {
		t.Fatalf("first claim = %#v, %v; want one delivery", first, err)
	}
	second, err := store.ClaimIdleDeliveries(ctx, queueName, 0, 1)
	if err != nil || len(second) != 1 {
		t.Fatalf("second claim = %#v, %v; want one delivery", second, err)
	}
	if second[0].StreamID == first[0].StreamID {
		t.Fatalf("cursor repeated stream entry %s", first[0].StreamID)
	}
}

func TestParseDeliveryRejectsInvalidGeneration(t *testing.T) {
	_, err := parseDelivery(redisclient.XMessage{ID: "42-0", Values: map[string]any{
		"task_id": "task-1", "delivery_id": "delivery-1", "generation": "not-an-int",
	}})
	if err == nil {
		t.Fatal("expected invalid generation error")
	}
}

func TestCatalogRouteKeys(t *testing.T) {
	const streamName = "queue:reference.echo:v1:r1:stream"
	if got := streamKey(streamName); got != streamName {
		t.Errorf("stream key = %q, want %q", got, streamName)
	}
	if got := groupName(streamName); got != "queue:reference.echo:v1:r1:workers" {
		t.Errorf("group name = %q, want %q", got, "queue:reference.echo:v1:r1:workers")
	}
	if got := dlqKey(streamName); got != "queue:reference.echo:v1:r1:dlq" {
		t.Errorf("DLQ key = %q, want %q", got, "queue:reference.echo:v1:r1:dlq")
	}
}

func TestConsumeDeliveryReturnsReadError(t *testing.T) {
	rdb := redisclient.NewClient(&redisclient.Options{})
	if err := rdb.Close(); err != nil {
		t.Fatalf("close client: %v", err)
	}

	store := &RedisStore{rdb: rdb}
	_, err := store.ConsumeDelivery(
		context.Background(), "queue:test:stream", "consumer",
	)
	if err == nil {
		t.Fatal("expected read error from closed client")
	}
}
