package kafka

import (
	"context"
	"fmt"
	"time"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// HealthChecker runs health checks against a Kafka cluster: broker
// reachability (Check), topic availability (CheckTopic) and consumer group lag
// (CheckConsumerLag). It owns one long-lived connection (a librdkafka producer
// handle plus the admin client derived from it) instead of dialing a fresh
// admin client per check — and that one connection carries SSL/SASL via the
// connConfig seam. Create one with NewHealthChecker and release it with Close.
//
// The context's deadline bounds each check; without a deadline a 10-second
// default applies to each broker query. A context that is already cancelled
// makes the check report HealthStatusDown. The checks never return a nil
// result; every failure is reported as a DOWN HealthResult.
type HealthChecker struct {
	producer *ckafka.Producer
	admin    *ckafka.AdminClient
}

// NewHealthChecker creates a health checker that owns its connection. It takes
// the ProducerOptions that describe the connection: ProducerWithBrokers
// (required, else ErrBrokersRequired), ProducerWithClientID, ProducerWithSSL,
// ProducerWithSASL, ProducerWithRawConfig, the two timeouts and
// ProducerWithLogger, with the same defaults as NewProducer. Options that only
// shape produced messages have no effect on a health checker. The checker
// never asks the broker to auto-create a topic (allow.auto.create.topics is
// forced to false, whatever ProducerWithRawConfig says).
func NewHealthChecker(opts ...ProducerOption) (*HealthChecker, error) {
	cfg := newDefaultProducerConfig()
	for _, opt := range opts {
		opt(cfg)
	}
	if len(cfg.Brokers) == 0 {
		return nil, ErrBrokersRequired
	}
	warnPlaintextSASL(cfg.Logger, cfg.conn())
	cfgMap := buildProducerConfig(cfg)
	// A producer handle asks the broker to auto-create the topics it requests
	// metadata for; a probe must never create what it checks. Set after the
	// Raw merge so raw config cannot turn it back on.
	cfgMap["allow.auto.create.topics"] = false
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
		producer: p,
		admin:    admin,
	}, nil
}

// Close releases the health checker's connection. The checker must not be used
// afterwards, and Close must not be called while a check is running.
func (h *HealthChecker) Close() error {
	h.admin.Close() // no-op for handles derived from a producer
	h.producer.Close()
	return nil
}

// defaultHealthTimeout bounds a broker query when the context has no deadline.
const defaultHealthTimeout = 10 * time.Second

// deadlineTimeout is how long a broker query may take: the time left until
// the context deadline, or defaultHealthTimeout when ctx has none.
func (h *HealthChecker) deadlineTimeout(ctx context.Context) time.Duration {
	if deadline, ok := ctx.Deadline(); ok {
		return time.Until(deadline)
	}
	return defaultHealthTimeout
}

// queryTimeoutMs is how long one broker query may take, in milliseconds. It
// returns the context's error once ctx is done, and context.DeadlineExceeded
// when no positive time remains (librdkafka must never get a non-positive
// timeout).
func (h *HealthChecker) queryTimeoutMs(ctx context.Context) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	ms := int(h.deadlineTimeout(ctx).Milliseconds())
	if ms <= 0 {
		return 0, context.DeadlineExceeded
	}
	return ms, nil
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

// Check reports whether the cluster is reachable: it fetches cluster metadata
// and is UP when at least one broker answered. When UP, Details holds
// "brokers" (the number of brokers), "topics" (the number of topics in the
// metadata) and "originatingId" (the id of the broker that answered). When
// DOWN, Error says why (a metadata failure, a cancelled context, or no brokers
// available). The context's deadline bounds the metadata request; the
// 10-second default applies without one.
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

	timeoutMs, err := h.queryTimeoutMs(ctx)
	if err != nil {
		return &HealthResult{
			Status: HealthStatusDown,
			Error:  err.Error(),
		}
	}
	metadata, err := h.admin.GetMetadata(nil, true, timeoutMs)
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

// CheckConsumerLag reports whether the consumer group groupID has fallen
// behind. Total lag is the sum over partitions of high watermark minus
// committed offset; partitions with no committed offset count as zero. The
// result is DOWN when the total lag exceeds maxLag, when the group does not
// exist (Error "consumer group not found"), or when a broker query fails or
// the context ends. The context's deadline bounds the whole check; without a
// deadline each watermark query gets the 10-second default (the group describe
// and offset listing then use the admin client's own default timeouts).
//
// Details holds "groupId", "state" (the group state), "memberCount", "lag" (the
// total), "maxLag" and "partitions", a list of maps with "topic", "partition",
// "committed", "high" and "lag" for every partition with a committed offset.
// DOWN results carry Error and, depending on the case, Details: none when the
// context is already done or the group describe or the committed-offset listing
// fails; only "groupId" when no group description came back, the group does not
// exist or reports an error, or the context ends during the watermark queries;
// "groupId", "topic" and "partition" when a watermark query fails. A DOWN
// caused by lag has an empty Error and the full Details above.
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

	// A group the broker has never seen comes back as a Dead description
	// (Redpanda reports no error code); Kafka may instead answer GROUP_ID_NOT_FOUND.
	if groupDesc.State == ckafka.ConsumerGroupStateDead || groupDesc.Error.Code() == ckafka.ErrGroupIDNotFound {
		return &HealthResult{
			Status: HealthStatusDown,
			Error:  fmt.Sprintf("consumer group not found: %s", groupID),
			Details: map[string]any{
				"groupId": groupID,
			},
		}
	}
	if groupDesc.Error.Code() != ckafka.ErrNoError {
		return &HealthResult{
			Status: HealthStatusDown,
			Error:  groupDesc.Error.Error(),
			Details: map[string]any{
				"groupId": groupID,
			},
		}
	}

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
			// Re-clamp per query so the ctx deadline bounds the whole check.
			timeoutMs, err := h.queryTimeoutMs(ctx)
			if err != nil {
				return &HealthResult{
					Status: HealthStatusDown,
					Error:  err.Error(),
					Details: map[string]any{
						"groupId": groupID,
					},
				}
			}
			_, high, err := h.producer.QueryWatermarkOffsets(*tp.Topic, tp.Partition, timeoutMs)
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

// CheckTopic reports whether topic exists and is usable: it is UP when the
// topic's metadata is present without an error. When UP, Details holds "topic",
// "partitionCount" and "partitions", a list of maps with "id", "leader" (the
// leader broker id), "replicas" (the replica count) and "isrs" (the in-sync
// replica count) for each partition. When DOWN (metadata failure, unknown topic
// or topic error), Error says why and Details holds "topic". The check never
// creates the topic, even on a broker with auto.create.topics.enable: a
// missing topic stays missing and is reported DOWN. The context's deadline
// bounds the metadata request; the 10-second default applies without one.
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

	timeoutMs, err := h.queryTimeoutMs(ctx)
	if err != nil {
		return &HealthResult{
			Status: HealthStatusDown,
			Error:  err.Error(),
			Details: map[string]any{
				"topic": topic,
			},
		}
	}
	metadata, err := h.admin.GetMetadata(&topic, false, timeoutMs)
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
