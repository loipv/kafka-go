package main

import (
	"context"
	"encoding/json"
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

func NewPartitionManager() *PartitionManager {
	return &PartitionManager{
		states: make(map[string]*PartitionState),
	}
}

func (pm *PartitionManager) GetOrCreate(topic string, partition int32) *PartitionState {
	key := topic + "-" + string(rune(partition))
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
	key := topic + "-" + string(rune(partition))
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
	key := topic + "-" + string(rune(partition))
	pm.mu.Lock()
	delete(pm.states, key)
	pm.mu.Unlock()
}

func main() {
	// Partition manager to handle per-partition state
	partitionManager := NewPartitionManager()

	// Create Kafka consumer with rebalance callback
	consumer, err := kafka.NewConsumer(
		kafka.ConsumerWithBrokers("localhost:9092"),
		kafka.WithGroupID("rebalance-aware-consumer"),
		kafka.WithTopics("orders"),

		// Disable auto-commit for manual offset control
		kafka.WithAutoCommit(false),

		// Use cooperative sticky for smoother rebalancing
		kafka.WithPartitionAssignor(kafka.AssignorCooperativeSticky),

		// Logging
		kafka.ConsumerWithLogLevel(kafka.LogLevelInfo),

		// Rebalance callback for partition management
		kafka.WithRebalanceCallback(func(event kafka.RebalanceEvent) error {
			switch event.Type {
			case "assigned":
				log.Println("=== PARTITIONS ASSIGNED ===")
				for _, tp := range event.Partitions {
					log.Printf("  + Assigned: %s [%d] @ offset %d",
						tp.Topic, tp.Partition, tp.Offset)

					// Initialize state for new partition
					partitionManager.GetOrCreate(tp.Topic, tp.Partition)

					// Example: Load checkpoint from external store
					// checkpoint := loadCheckpoint(tp.Topic, tp.Partition)
					// consumer.Seek(tp, checkpoint)
				}
				log.Println("===========================")

			case "revoked":
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

	// Register message handler
	consumer.Handle(func(ctx context.Context, msg *kafka.Message) error {
		// Get partition state
		state := partitionManager.GetOrCreate(msg.Topic, msg.Partition)

		// Buffer message (example: batch writes)
		state.mu.Lock()
		state.Buffer = append(state.Buffer, string(msg.Value))
		bufferSize := len(state.Buffer)
		state.mu.Unlock()

		log.Printf("Received: topic=%s partition=%d offset=%d key=%s (buffer=%d)",
			msg.Topic, msg.Partition, msg.Offset, string(msg.Key), bufferSize)

		// Flush when buffer is full
		if bufferSize >= 10 {
			partitionManager.FlushPartition(msg.Topic, msg.Partition)
		}

		return nil
	})

	// Setup graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("Shutting down...")
		cancel()
	}()

	// Start consuming
	log.Println("Starting rebalance-aware consumer...")
	log.Println("This consumer uses rebalance callbacks to:")
	log.Println("  - Initialize resources when partitions are assigned")
	log.Println("  - Flush buffers and cleanup when partitions are revoked")
	log.Println("")
	log.Println("Press Ctrl+C to stop")

	if err := consumer.Start(ctx); err != nil && err != context.Canceled {
		log.Fatalf("Consumer error: %v", err)
	}

	// Close consumer
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	if err := consumer.Close(shutdownCtx); err != nil {
		log.Printf("Error closing consumer: %v", err)
	}

	log.Println("Consumer stopped")
}

// Example: Checkpoint management functions (implement based on your storage)
func loadCheckpoint(topic string, partition int32) int64 {
	// Load from Redis, database, or file
	// return storedOffset
	return -1 // -1 means use Kafka's stored offset
}

func saveCheckpoint(topic string, partition int32, offset int64) {
	// Save to Redis, database, or file
	data := map[string]interface{}{
		"topic":     topic,
		"partition": partition,
		"offset":    offset,
		"timestamp": time.Now().Format(time.RFC3339),
	}
	jsonData, _ := json.Marshal(data)
	log.Printf("Saving checkpoint: %s", string(jsonData))
}

