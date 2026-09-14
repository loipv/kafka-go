package kafka

import (
	"fmt"
	"testing"
	"time"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func TestCommitOffsets(t *testing.T) {
	msgs := []*Message{
		{Topic: "t", Partition: 0, Offset: 5},
		{Topic: "t", Partition: 0, Offset: 9},
		{Topic: "t", Partition: 1, Offset: 3},
		{Topic: "t", Partition: 2, Offset: 0}, // Kafka's first offset must not be dropped
	}
	got := commitOffsets(msgs)
	// map iteration order is random — compare as a set. (Keyed by
	// topic-string+partition: ckafka.TopicPartition holds a *string, which
	// would compare pointer identity and make every lookup miss.)
	byKey := map[string]ckafka.Offset{}
	for _, tp := range got {
		if tp.Topic == nil {
			t.Fatal("commitOffsets returned a nil Topic")
		}
		byKey[fmt.Sprintf("%s/%d", *tp.Topic, tp.Partition)] = tp.Offset
	}
	if len(byKey) != 3 {
		t.Fatalf("commitOffsets produced %d partitions, want 3 (deduped): %+v", len(byKey), got)
	}
	if o := byKey["t/0"]; o != ckafka.Offset(10) {
		t.Errorf("partition 0 offset = %v, want 10 (max 9 + 1)", o)
	}
	if o := byKey["t/1"]; o != ckafka.Offset(4) {
		t.Errorf("partition 1 offset = %v, want 4 (max 3 + 1)", o)
	}
	if o := byKey["t/2"]; o != ckafka.Offset(1) {
		t.Errorf("partition 2 offset = %v, want 1 (offset 0 must be stored as 0+1)", o)
	}
}

func TestCommitOffsetsEmpty(t *testing.T) {
	if got := commitOffsets(nil); len(got) != 0 {
		t.Errorf("commitOffsets(nil) = %+v, want empty", got)
	}
}

func TestBlockBackoff(t *testing.T) {
	r := &RetryConfig{InitialInterval: time.Second, Multiplier: 2, MaxInterval: 30 * time.Second}
	tests := []struct {
		blocks int
		want   time.Duration
	}{
		{1, time.Second},
		{2, 2 * time.Second},
		{3, 4 * time.Second},
		{6, 30 * time.Second}, // 32s would exceed the cap
		{20, 30 * time.Second},
	}
	for _, tt := range tests {
		if got := blockBackoff(tt.blocks, r); got != tt.want {
			t.Errorf("blockBackoff(%d) = %v, want %v", tt.blocks, got, tt.want)
		}
	}
	// zero-value RetryConfig must not panic and must fall back to defaults
	if got := blockBackoff(1, nil); got <= 0 {
		t.Errorf("blockBackoff(1, nil) = %v, want positive default", got)
	}
}

func TestRetryBudget(t *testing.T) {
	// defaults: 3 retries, 1s initial, 2x => 1+2+4 = 7s
	if got, want := retryBudget(nil), 7*time.Second; got != want {
		t.Errorf("retryBudget(nil) = %v, want %v", got, want)
	}
	// MaxInterval caps each delay. 3 sleeps (1+2 would be followed by a capped
	// 3): 1+2+3 = 6s. (The brief's "1+2+3+3 = 9s" miscounted four sleeps for
	// three retries — executeWithRetry sleeps only between attempts.)
	r := &RetryConfig{MaxRetries: 3, InitialInterval: time.Second, Multiplier: 2, MaxInterval: 3 * time.Second}
	if got, want := retryBudget(r), 6*time.Second; got != want {
		t.Errorf("retryBudget(capped) = %v, want %v", got, want)
	}
}
