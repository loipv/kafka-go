// Command rebalance demonstrates a consumer that keeps per-partition state
// across rebalances: it flushes buffers when partitions are revoked and
// commits offsets manually.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/loipv/kafka-go/kafka"
)

// PartitionState holds state for a specific partition
type PartitionState struct {
	Partition int32
	Buffer    []string
	mu        sync.Mutex
}

// PartitionManager manages per-partition state
type PartitionManager struct {
	states map[string]*PartitionState // key: "topic-partition"
	mu     sync.RWMutex
}

func partitionKey(topic string, partition int32) string {
	return fmt.Sprintf("%s-%d", topic, partition)
}

func NewPartitionManager() *PartitionManager {
	return &PartitionManager{
		states: make(map[string]*PartitionState),
	}
}

func (pm *PartitionManager) GetOrCreate(topic string, partition int32) *PartitionState {
	key := partitionKey(topic, partition)
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if state, ok := pm.states[key]; ok {
		return state
	}

	state := &PartitionState{
		Partition: partition,
		Buffer:    make([]string, 0),
	}
	pm.states[key] = state
	return state
}

func (pm *PartitionManager) FlushPartition(topic string, partition int32) {
	key := partitionKey(topic, partition)
	pm.mu.Lock()
	state, ok := pm.states[key]
	pm.mu.Unlock()

	if ok {
		state.mu.Lock()
		if len(state.Buffer) > 0 {
			log.Printf("Flushing %d items for partition %d", len(state.Buffer), partition)
			// In real application: flush to database, etc.
			state.Buffer = make([]string, 0)
		}
		state.mu.Unlock()
	}
}

func (pm *PartitionManager) RemovePartition(topic string, partition int32) {
	key := partitionKey(topic, partition)
	pm.mu.Lock()
	delete(pm.states, key)
	pm.mu.Unlock()
}

func main() {
	// Per-partition buffers, flushed when a partition is revoked
	partitionManager := NewPartitionManager()

	consumer, err := kafka.NewConsumer(
		kafka.ConsumerWithBrokers("localhost:9092"),
		kafka.ConsumerWithGroupID("rebalance-aware-consumer"),
		kafka.ConsumerWithTopics("orders"),

		// No background commits: the handler calls Commit itself. (The
		// library still commits stored offsets when partitions are revoked.)
		kafka.ConsumerWithAutoCommit(false),

		// Cooperative sticky only revokes the partitions that move.
		kafka.ConsumerWithPartitionAssignor(kafka.AssignorCooperativeSticky),

		// Runs before the library applies each assignment, so keep it short.
		kafka.ConsumerWithRebalanceCallback(func(event kafka.RebalanceEvent) error {
			switch event.Type {
			case kafka.RebalanceAssigned:
				log.Println("=== PARTITIONS ASSIGNED ===")
				for _, tp := range event.Partitions {
					log.Printf("  + Assigned: %s [%d] @ offset %d",
						tp.Topic, tp.Partition, tp.Offset)

					// Initialize state for new partition
					partitionManager.GetOrCreate(tp.Topic, tp.Partition)

					// Example: Load checkpoint from external store
					// checkpoint := loadCheckpoint(tp.Topic, tp.Partition)
				}
				log.Println("===========================")

			case kafka.RebalanceRevoked:
				log.Println("=== PARTITIONS REVOKED ===")
				for _, tp := range event.Partitions {
					log.Printf("  - Revoked: %s [%d]", tp.Topic, tp.Partition)

					// Flush any pending data before losing partition
					partitionManager.FlushPartition(tp.Topic, tp.Partition)

					// Clean up state for revoked partition
					partitionManager.RemovePartition(tp.Topic, tp.Partition)

					// Example: Save checkpoint to external store
					// saveCheckpoint(tp.Topic, tp.Partition, currentOffset)
				}
				log.Println("===========================")
			}
			return nil
		}),
	)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}

	consumer.OnMessage(func(_ context.Context, msg *kafka.Message) error {
		// Get partition state
		state := partitionManager.GetOrCreate(msg.Topic, msg.Partition)

		// Buffer message (example: batch writes)
		state.mu.Lock()
		state.Buffer = append(state.Buffer, string(msg.Value))
		bufferSize := len(state.Buffer)
		state.mu.Unlock()

		log.Printf("Received: topic=%s partition=%d offset=%d key=%s (buffer=%d)",
			msg.Topic, msg.Partition, msg.Offset, string(msg.Key), bufferSize)

		// Flush when the buffer is full, then commit: the offset moves past
		// this message only once its data is durably handled.
		if bufferSize >= 10 {
			partitionManager.FlushPartition(msg.Topic, msg.Partition)
			return consumer.Commit(msg)
		}

		return nil
	})

	// Cancel the context on SIGINT/SIGTERM so Start returns.
	ctx, cancel := context.WithCancel(context.Background())
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("Shutting down...")
		cancel()
	}()

	log.Println("Starting rebalance-aware consumer... press Ctrl+C to stop")

	if err := consumer.Start(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Printf("Consumer error: %v", err)
	}

	// Close after Start returns (also after an error).
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	if err := consumer.Close(shutdownCtx); err != nil {
		log.Printf("Error closing consumer: %v", err)
	}

	log.Println("Consumer stopped")
}
