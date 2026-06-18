package redis

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisStore struct {
	rdb     *redis.Client
	holdTTL time.Duration
}

func NewRedisStore(ctx context.Context, cfg Config, holdTTL time.Duration) (*RedisStore, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.Address,
		Password: cfg.Password,
		DB:       cfg.DB,
		Protocol: 2,
	})
	if err := rdb.Ping(ctx).Err(); err != nil {
		rdb.Close()
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}
	fmt.Printf("Connected to Redis at %s, DB: %d\n", cfg.Address, cfg.DB)
	return &RedisStore{
		rdb:     rdb,
		holdTTL: holdTTL,
	}, nil
}

func (s *RedisStore) Close() error {
	return s.rdb.Close()
}

func (s *RedisStore) EnqueueTask(ctx context.Context, queueName string, taskId string) error {
	err := s.rdb.LPush(ctx, queueName, taskId).Err()
	if err != nil {
		return fmt.Errorf("failed to enqueue task %s: %w", taskId, err)
	}
	return nil
}

func (s *RedisStore) ConsumeTask(ctx context.Context, queueName string) (string, error) {
	inflightKey := queueName + ":inflight"
	timeHashKey := queueName + ":timestamps"

	taskId, err := s.rdb.BLMove(ctx, queueName, inflightKey, "RIGHT", "LEFT", 0).Result()
	if err == redis.Nil {
		return "", nil // No task available
	}
	if err != nil {
		return "", fmt.Errorf("failed to consume task vis brpoplpush: %w", err)
	}
	//To solve TimeStamp difference between different machines, we use Redis server time instead of local time

	redisTime, err := s.rdb.Time(ctx).Result()
	if err != nil {
		return taskId, nil //fallback gracefully if we fail to get Redis server time
	}
	now := redisTime.Unix()
	err = s.rdb.HSet(ctx, timeHashKey, taskId, strconv.FormatInt(now, 10)).Err()
	if err != nil {
		return taskId, nil
	}
	return taskId, nil
}

func (s *RedisStore) AcknowledgeTask(ctx context.Context, queueName string, taskId string) error {
	inflightKey := queueName + ":inflight"
	timeHashKey := queueName + ":timestamps"

	pipe := s.rdb.TxPipeline()
	pipe.LRem(ctx, inflightKey, 1, taskId)
	pipe.HDel(ctx, timeHashKey, taskId)
	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to acknowledge task %s: %w", taskId, err)
	}
	return nil
}

func (s *RedisStore) SweepAbondonedTasks(ctx context.Context, queueName string) error {
	inflightKey := queueName + ":inflight"
	timeHashKey := queueName + ":timestamps"

	timestamps, err := s.rdb.HGetAll(ctx, timeHashKey).Result()
	if err != nil {
		return fmt.Errorf("failed to get task timestamps: %w", err)
	}
	redisTime, err := s.rdb.Time(ctx).Result()
	if err != nil {
		return fmt.Errorf("failed to get Redis server time: %w", err)
	}
	now := redisTime.Unix()
	for taskId, tsStr := range timestamps {
		startTime, err := strconv.ParseInt(tsStr, 10, 64)
		if err != nil {
			continue
		}
		if now-startTime > int64(s.holdTTL.Seconds()) {
			slog.Warn("Task %s has been held for too long, moving it back to the queue", "taskId", taskId)
			pipe := s.rdb.TxPipeline()
			pipe.LRem(ctx, inflightKey, 1, taskId)
			pipe.HDel(ctx, timeHashKey, taskId)
			pipe.LPush(ctx, queueName, taskId)
			_, err := pipe.Exec(ctx)
			if err != nil {
				return fmt.Errorf("failed to sweep abandoned task %s: %w", taskId, err)
			}
		}
	}
	return nil
}
