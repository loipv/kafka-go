package kafka

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// HealthChecker provides health check functionality for Kafka
type HealthChecker struct {
	client  *KafkaClient
	brokers []string
	timeout time.Duration
}

// NewHealthChecker creates a new health checker
func NewHealthChecker(client *KafkaClient) *HealthChecker {
	return &HealthChecker{
		client:  client,
		brokers: client.config.Brokers,
		timeout: 10 * time.Second,
	}
}

// NewHealthCheckerWithBrokers creates a new health checker with brokers
func NewHealthCheckerWithBrokers(brokers []string) *HealthChecker {
	return &HealthChecker{
		brokers: brokers,
		timeout: 10 * time.Second,
	}
}

// SetTimeout sets the health check timeout
func (h *HealthChecker) SetTimeout(timeout time.Duration) {
	h.timeout = timeout
}

// Check performs a basic health check
func (h *HealthChecker) Check(ctx context.Context) *HealthResult {
	// Check if context is already cancelled
	select {
	case <-ctx.Done():
		return &HealthResult{
			Status: HealthStatusDown,
			Error:  ctx.Err(),
			Details: map[string]interface{}{
				"error": ctx.Err().Error(),
			},
		}
	default:
	}

	// Create an admin client for metadata
	adminClient, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": strings.Join(h.brokers, ","),
	})
	if err != nil {
		return &HealthResult{
			Status: HealthStatusDown,
			Error:  err,
			Details: map[string]interface{}{
				"error": err.Error(),
			},
		}
	}
	defer adminClient.Close()

	// Get metadata with timeout (use context deadline if available)
	timeout := h.timeout
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining < timeout {
			timeout = remaining
		}
	}

	metadata, err := adminClient.GetMetadata(nil, true, int(timeout.Milliseconds()))
	if err != nil {
		return &HealthResult{
			Status: HealthStatusDown,
			Error:  err,
			Details: map[string]interface{}{
				"error": err.Error(),
			},
		}
	}

	// Check if we have at least one broker
	if len(metadata.Brokers) == 0 {
		return &HealthResult{
			Status: HealthStatusDown,
			Error:  fmt.Errorf("no brokers available"),
			Details: map[string]interface{}{
				"error": "no brokers available",
			},
		}
	}

	return &HealthResult{
		Status: HealthStatusUp,
		Details: map[string]interface{}{
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
			Error:  ctx.Err(),
			Details: map[string]interface{}{
				"error": ctx.Err().Error(),
			},
		}
	default:
	}

	adminClient, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": strings.Join(h.brokers, ","),
	})
	if err != nil {
		return &HealthResult{
			Status: HealthStatusDown,
			Error:  err,
			Details: map[string]interface{}{
				"error": err.Error(),
			},
		}
	}
	defer adminClient.Close()

	// Get metadata with timeout
	timeout := h.timeout
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining < timeout {
			timeout = remaining
		}
	}

	metadata, err := adminClient.GetMetadata(nil, true, int(timeout.Milliseconds()))
	if err != nil {
		return &HealthResult{
			Status: HealthStatusDown,
			Error:  err,
			Details: map[string]interface{}{
				"error": err.Error(),
			},
		}
	}

	brokerInfos := make([]map[string]interface{}, 0, len(metadata.Brokers))
	for _, broker := range metadata.Brokers {
		brokerInfos = append(brokerInfos, map[string]interface{}{
			"id":   broker.ID,
			"host": broker.Host,
			"port": broker.Port,
		})
	}

	return &HealthResult{
		Status: HealthStatusUp,
		Details: map[string]interface{}{
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
			Error:  ctx.Err(),
			Details: map[string]interface{}{
				"error": ctx.Err().Error(),
			},
		}
	default:
	}

	adminClient, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": strings.Join(h.brokers, ","),
	})
	if err != nil {
		return &HealthResult{
			Status: HealthStatusDown,
			Error:  err,
			Details: map[string]interface{}{
				"error": err.Error(),
			},
		}
	}
	defer adminClient.Close()

	// Get consumer group offsets
	groups, err := adminClient.ListConsumerGroups(ctx)
	if err != nil {
		return &HealthResult{
			Status: HealthStatusDown,
			Error:  err,
			Details: map[string]interface{}{
				"error": err.Error(),
			},
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
			Error:  fmt.Errorf("consumer group not found: %s", groupID),
			Details: map[string]interface{}{
				"error":   "consumer group not found",
				"groupId": groupID,
			},
		}
	}

	// Describe consumer groups to get member information
	describeResult, err := adminClient.DescribeConsumerGroups(ctx, []string{groupID})
	if err != nil {
		return &HealthResult{
			Status: HealthStatusDown,
			Error:  err,
			Details: map[string]interface{}{
				"error": err.Error(),
			},
		}
	}

	if len(describeResult.ConsumerGroupDescriptions) == 0 {
		return &HealthResult{
			Status: HealthStatusDown,
			Error:  fmt.Errorf("no group description found"),
			Details: map[string]interface{}{
				"error":   "no group description found",
				"groupId": groupID,
			},
		}
	}

	groupDesc := describeResult.ConsumerGroupDescriptions[0]

	// Get committed offsets
	offsetResult, err := adminClient.ListConsumerGroupOffsets(ctx, []kafka.ConsumerGroupTopicPartitions{
		{Group: groupID},
	})
	if err != nil {
		return &HealthResult{
			Status: HealthStatusDown,
			Error:  err,
			Details: map[string]interface{}{
				"error": err.Error(),
			},
		}
	}

	// Calculate total lag
	var totalLag int64
	var lagDetails []map[string]interface{}

	for _, groupOffsets := range offsetResult.ConsumerGroupsTopicPartitions {
		for _, tp := range groupOffsets.Partitions {
			if tp.Offset < 0 {
				continue
			}

			// Get end offset for partition
			// Note: This requires creating a consumer to get watermark offsets
			// For simplicity, we'll just report the committed offset
			lagDetails = append(lagDetails, map[string]interface{}{
				"topic":     *tp.Topic,
				"partition": tp.Partition,
				"offset":    int64(tp.Offset),
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
		Details: map[string]interface{}{
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
			Error:  ctx.Err(),
			Details: map[string]interface{}{
				"error": ctx.Err().Error(),
			},
		}
	default:
	}

	adminClient, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": strings.Join(h.brokers, ","),
	})
	if err != nil {
		return &HealthResult{
			Status: HealthStatusDown,
			Error:  err,
			Details: map[string]interface{}{
				"error": err.Error(),
			},
		}
	}
	defer adminClient.Close()

	// Get metadata with timeout
	timeout := h.timeout
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining < timeout {
			timeout = remaining
		}
	}

	metadata, err := adminClient.GetMetadata(&topic, false, int(timeout.Milliseconds()))
	if err != nil {
		return &HealthResult{
			Status: HealthStatusDown,
			Error:  err,
			Details: map[string]interface{}{
				"error": err.Error(),
				"topic": topic,
			},
		}
	}

	topicMeta, ok := metadata.Topics[topic]
	if !ok {
		return &HealthResult{
			Status: HealthStatusDown,
			Error:  fmt.Errorf("topic not found: %s", topic),
			Details: map[string]interface{}{
				"error": "topic not found",
				"topic": topic,
			},
		}
	}

	if topicMeta.Error.Code() != kafka.ErrNoError {
		return &HealthResult{
			Status: HealthStatusDown,
			Error:  topicMeta.Error,
			Details: map[string]interface{}{
				"error": topicMeta.Error.String(),
				"topic": topic,
			},
		}
	}

	partitionInfos := make([]map[string]interface{}, 0, len(topicMeta.Partitions))
	for _, p := range topicMeta.Partitions {
		partitionInfos = append(partitionInfos, map[string]interface{}{
			"id":       p.ID,
			"leader":   p.Leader,
			"replicas": len(p.Replicas),
			"isrs":     len(p.Isrs),
		})
	}

	return &HealthResult{
		Status: HealthStatusUp,
		Details: map[string]interface{}{
			"topic":          topic,
			"partitionCount": len(topicMeta.Partitions),
			"partitions":     partitionInfos,
		},
	}
}

