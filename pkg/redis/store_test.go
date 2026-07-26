package redis

import (
	"testing"

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

func TestParseDeliveryRejectsInvalidGeneration(t *testing.T) {
	_, err := parseDelivery(redisclient.XMessage{ID: "42-0", Values: map[string]any{
		"task_id": "task-1", "delivery_id": "delivery-1", "generation": "not-an-int",
	}})
	if err == nil {
		t.Fatal("expected invalid generation error")
	}
}
