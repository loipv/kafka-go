package kafka

import (
	"context"
	"fmt"
	"time"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// HealthChecker provides health check functionality for Kafka.
// It owns one long-lived connection (a librdkafka producer handle plus the
// admin client derived from it) instead of dialing a fresh admin client per
// check — and that one connection carries SSL/SASL via the connConfig seam.
type HealthChecker struct {
	producer     *ckafka.Producer
	admin        *ckafka.AdminClient
	timeout      time.Duration
	ownsProducer bool
}

// NewHealthChecker creates a health checker that owns its producer.
func NewHealthChecker(opts ...ProducerOption) (*HealthChecker, error) {
	cfg := newDefaultProducerConfig()
	for _, opt := range opts {
		opt(cfg)
	}
	if len(cfg.Brokers) == 0 {
		return nil, ErrBrokersRequired
	}
	cfgMap := buildProducerConfig(cfg)
	p, err := ckafka.NewProducer(&cfgMap)
	if err != nil {
		return nil, fmt.Errorf("create health producer: %w", err)
	}
	admin, err := ckafka.NewAdminClientFromProducer(p)
	if err != nil {
		p.Close()
		return nil, fmt.Errorf("create health admin client: %w", err)
	}
	return &HealthChecker{
		producer:     p,
		admin:        admin,
		timeout:      10 * time.Second,
		ownsProducer: true,
	}, nil
}

// NewHealthCheckerFromProducer derives a health checker from an existing
// producer — no extra connections. Close() does NOT close the given producer.
func NewHealthCheckerFromProducer(p *Producer) (*HealthChecker, error) {
	admin, err := ckafka.NewAdminClientFromProducer(p.producer)
	if err != nil {
		return nil, fmt.Errorf("create health admin client: %w", err)
	}
	return &HealthChecker{
		producer:     p.producer,
		admin:        admin,
		timeout:      10 * time.Second,
		ownsProducer: false,
	}, nil
}

// Close releases the health checker's resources. It is a no-op for checkers
// derived via NewHealthCheckerFromProducer.
func (h *HealthChecker) Close() error {
	if !h.ownsProducer {
		return nil // derived — the producer belongs to the caller
	}
	h.admin.Close() // no-op for handles derived from a producer
	h.producer.Close()
	return nil
}

// SetTimeout sets the health check timeout
func (h *HealthChecker) SetTimeout(timeout time.Duration) {
	h.timeout = timeout
}

// deadlineTimeout clamps the health check timeout to the context deadline.
func (h *HealthChecker) deadlineTimeout(ctx context.Context) time.Duration {
	timeout := h.timeout
	if deadline, ok := ctx.Deadline(); ok {
		if remaining := time.Until(deadline); remaining < timeout {
			timeout = remaining
		}
	}
	return timeout
}

// lagForPartition returns consumer lag for one partition: high watermark
// minus committed offset. A negative committed offset means "nothing
// committed yet" and a committed offset beyond the watermark means log
// truncation — both report zero, not negative lag.
func lagForPartition(high, committed int64) int64 {
	if committed < 0 {
		return 0
	}
	if l := high - committed; l > 0 {
		return l
	}
	return 0
}

// Check performs a basic health check
func (h *HealthChecker) Check(ctx context.Context) *HealthResult {
	// Check if context is already cancelled
	select {
	case <-ctx.Done():
		return &HealthResult{
			Status: HealthStatusDown,
			Error:  ctx.Err().Error(),
		}
	default:
	}

	metadata, err := h.admin.GetMetadata(nil, true, int(h.deadlineTimeout(ctx).Milliseconds()))
	if err != nil {
		return &HealthResult{
			Status: HealthStatusDown,
			Error:  err.Error(),
		}
	}

	// Check if we have at least one broker
	if len(metadata.Brokers) == 0 {
		return &HealthResult{
			Status: HealthStatusDown,
			Error:  "no brokers available",
		}
	}

	return &HealthResult{
		Status: HealthStatusUp,
		Details: map[string]any{
			"brokers":       len(metadata.Brokers),
			"topics":        len(metadata.Topics),
			"originatingId": metadata.OriginatingBroker.ID,
		},
	}
}

// CheckBrokers checks broker connectivity
func (h *HealthChecker) CheckBrokers(ctx context.Context) *HealthResult {
	// Check if context is already cancelled
	select {
	case <-ctx.Done():
		return &HealthResult{
			Status: HealthStatusDown,
			Error:  ctx.Err().Error(),
		}
	default:
	}

	metadata, err := h.admin.GetMetadata(nil, true, int(h.deadlineTimeout(ctx).Milliseconds()))
	if err != nil {
		return &HealthResult{
			Status: HealthStatusDown,
			Error:  err.Error(),
		}
	}

	brokerInfos := make([]map[string]any, 0, len(metadata.Brokers))
	for _, broker := range metadata.Brokers {
		brokerInfos = append(brokerInfos, map[string]any{
			"id":   broker.ID,
			"host": broker.Host,
			"port": broker.Port,
		})
	}

	return &HealthResult{
		Status: HealthStatusUp,
		Details: map[string]any{
			"brokers":     brokerInfos,
			"brokerCount": len(metadata.Brokers),
		},
	}
}

// CheckConsumerLag checks consumer lag for a specific consumer group
func (h *HealthChecker) CheckConsumerLag(ctx context.Context, groupID string, maxLag int64) *HealthResult {
	// Check if context is already cancelled
	select {
	case <-ctx.Done():
		return &HealthResult{
			Status: HealthStatusDown,
			Error:  ctx.Err().Error(),
		}
	default:
	}

	// Get consumer group offsets
	groups, err := h.admin.ListConsumerGroups(ctx)
	if err != nil {
		return &HealthResult{
			Status: HealthStatusDown,
			Error:  err.Error(),
		}
	}

	// Check if group exists
	groupFound := false
	for _, g := range groups.Valid {
		if g.GroupID == groupID {
			groupFound = true
			break
		}
	}

	if !groupFound {
		return &HealthResult{
			Status: HealthStatusDown,
			Error:  fmt.Sprintf("consumer group not found: %s", groupID),
			Details: map[string]any{
				"groupId": groupID,
			},
		}
	}

	// Describe consumer groups to get member information
	describeResult, err := h.admin.DescribeConsumerGroups(ctx, []string{groupID})
	if err != nil {
		return &HealthResult{
			Status: HealthStatusDown,
			Error:  err.Error(),
		}
	}

	if len(describeResult.ConsumerGroupDescriptions) == 0 {
		return &HealthResult{
			Status: HealthStatusDown,
			Error:  "no group description found",
			Details: map[string]any{
				"groupId": groupID,
			},
		}
	}

	groupDesc := describeResult.ConsumerGroupDescriptions[0]

	// Get committed offsets
	offsetResult, err := h.admin.ListConsumerGroupOffsets(ctx, []ckafka.ConsumerGroupTopicPartitions{
		{Group: groupID},
	})
	if err != nil {
		return &HealthResult{
			Status: HealthStatusDown,
			Error:  err.Error(),
		}
	}

	// Calculate total lag: per-partition high watermark minus committed offset
	var totalLag int64
	var lagDetails []map[string]any
	for _, groupOffsets := range offsetResult.ConsumerGroupsTopicPartitions {
		for _, tp := range groupOffsets.Partitions {
			if tp.Offset < 0 || tp.Topic == nil {
				continue
			}
			_, high, err := h.producer.QueryWatermarkOffsets(*tp.Topic, tp.Partition, int(h.timeout.Milliseconds()))
			if err != nil {
				return &HealthResult{
					Status: HealthStatusDown,
					Error:  fmt.Sprintf("query watermark for %s[%d]: %v", *tp.Topic, tp.Partition, err),
					Details: map[string]any{
						"groupId":   groupID,
						"topic":     *tp.Topic,
						"partition": tp.Partition,
					},
				}
			}
			lag := lagForPartition(high, int64(tp.Offset))
			totalLag += lag
			lagDetails = append(lagDetails, map[string]any{
				"topic":     *tp.Topic,
				"partition": tp.Partition,
				"committed": int64(tp.Offset),
				"high":      high,
				"lag":       lag,
			})
		}
	}

	// Determine health status based on lag
	status := HealthStatusUp
	if totalLag > maxLag {
		status = HealthStatusDown
	}

	return &HealthResult{
		Status: status,
		Details: map[string]any{
			"groupId":     groupID,
			"state":       groupDesc.State.String(),
			"memberCount": len(groupDesc.Members),
			"lag":         totalLag,
			"maxLag":      maxLag,
			"partitions":  lagDetails,
		},
	}
}

// CheckTopic checks if a topic exists and is accessible
func (h *HealthChecker) CheckTopic(ctx context.Context, topic string) *HealthResult {
	// Check if context is already cancelled
	select {
	case <-ctx.Done():
		return &HealthResult{
			Status: HealthStatusDown,
			Error:  ctx.Err().Error(),
		}
	default:
	}

	metadata, err := h.admin.GetMetadata(&topic, false, int(h.deadlineTimeout(ctx).Milliseconds()))
	if err != nil {
		return &HealthResult{
			Status: HealthStatusDown,
			Error:  err.Error(),
			Details: map[string]any{
				"topic": topic,
			},
		}
	}

	topicMeta, ok := metadata.Topics[topic]
	if !ok {
		return &HealthResult{
			Status: HealthStatusDown,
			Error:  fmt.Sprintf("topic not found: %s", topic),
			Details: map[string]any{
				"topic": topic,
			},
		}
	}

	if topicMeta.Error.Code() != ckafka.ErrNoError {
		return &HealthResult{
			Status: HealthStatusDown,
			Error:  topicMeta.Error.String(),
			Details: map[string]any{
				"topic": topic,
			},
		}
	}

	partitionInfos := make([]map[string]any, 0, len(topicMeta.Partitions))
	for _, p := range topicMeta.Partitions {
		partitionInfos = append(partitionInfos, map[string]any{
			"id":       p.ID,
			"leader":   p.Leader,
			"replicas": len(p.Replicas),
			"isrs":     len(p.Isrs),
		})
	}

	return &HealthResult{
		Status: HealthStatusUp,
		Details: map[string]any{
			"topic":          topic,
			"partitionCount": len(topicMeta.Partitions),
			"partitions":     partitionInfos,
		},
	}
}
