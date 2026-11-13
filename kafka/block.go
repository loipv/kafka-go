package kafka

import (
	"context"
	"time"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// blockState is the per-partition blocked bookkeeping. offset is the lowest
// blocked offset seen — a forward seek must never overwrite it (queued
// messages can re-block with a higher offset, which would strand the
// original). Entries survive resumes: blocks escalate until the message
// finally parks (clearBlockedOnPark) or the partition is revoked.
type blockState struct {
	retryAt time.Time
	blocks  int
	offset  int64
	resumed bool
}

// blockMessages pauses the affected partitions, seeks them back to the
// failed messages' offsets, and records an escalating retry-at deadline.
// The poll loop keeps running — other partitions flow normally.
// ponytail: per-poll linear scan of c.blocked — fine for hundreds of
// partitions; index by time if it ever isn't.
func (c *Consumer) blockMessages(_ context.Context, msgs []*Message) {
	if len(msgs) == 0 {
		return
	}
	minOff := map[TopicPartition]int64{}
	for _, m := range msgs {
		tp := TopicPartition{Topic: m.Topic, Partition: m.Partition}
		if o, ok := minOff[tp]; !ok || m.Offset < o {
			minOff[tp] = m.Offset
		}
	}
	now := time.Now()
	var paused []ckafka.TopicPartition
	c.blockMu.Lock()
	for tp, off := range minOff {
		bs := c.blocked[tp]
		if bs.blocks == 0 || off < bs.offset {
			bs.offset = off
		}
		bs.blocks++
		bs.resumed = false
		bs.retryAt = now.Add(blockBackoff(bs.blocks, c.retry))
		c.blocked[tp] = bs
		topic := tp.Topic
		paused = append(paused, ckafka.TopicPartition{Topic: &topic, Partition: tp.Partition, Offset: ckafka.Offset(bs.offset)})
		c.logger.Warn("partition blocked — message unparkable",
			"topic", tp.Topic, "partition", tp.Partition, "offset", bs.offset,
			"blocks", bs.blocks, "retryIn", blockBackoff(bs.blocks, c.retry))
	}
	c.blockMu.Unlock()
	c.metrics.setBlocked(c.blockedSnapshot())
	if err := c.consumer.Pause(paused); err != nil {
		c.logger.Error("pause failed for blocked partition (next pass re-attempts)", "error", err)
	}
	for _, tp := range paused {
		if err := c.consumer.Seek(tp, 5000); err != nil {
			t := ""
			if tp.Topic != nil {
				t = *tp.Topic
			}
			c.logger.Error("seek failed for blocked partition",
				"topic", t, "partition", tp.Partition, "offset", tp.Offset, "error", err)
		}
	}
}

// resumeBlocked resumes partitions whose backoff elapsed. Called each poll
// pass. Skips partitions while the user-level Pause() is active. Entries are
// NOT deleted here: blocks must keep escalating across cycles until the
// message finally parks (clearBlockedOnPark) — deleting on resume would reset
// the backoff to InitialInterval and make a poison partition retry flat-out
// forever.
func (c *Consumer) resumeBlocked() {
	if c.paused.Load() {
		return
	}
	now := time.Now()
	var toResume []ckafka.TopicPartition
	c.blockMu.Lock()
	for tp, bs := range c.blocked {
		if !bs.resumed && now.After(bs.retryAt) {
			bs.resumed = true
			c.blocked[tp] = bs
			topic := tp.Topic
			toResume = append(toResume, ckafka.TopicPartition{Topic: &topic, Partition: tp.Partition})
		}
	}
	c.blockMu.Unlock()
	if len(toResume) > 0 {
		if err := c.consumer.Resume(toResume); err != nil {
			c.logger.Error("resume failed for blocked partition (next pass re-attempts)", "error", err)
			// Roll the resumed flag back so the next pass really does
			// re-attempt: leaving it set would park the partition until a
			// rebalance drops the blocked state.
			c.blockMu.Lock()
			for _, tp := range toResume {
				if tp.Topic == nil {
					continue
				}
				key := TopicPartition{Topic: *tp.Topic, Partition: tp.Partition}
				if bs, ok := c.blocked[key]; ok {
					bs.resumed = false
					c.blocked[key] = bs
				}
			}
			c.blockMu.Unlock()
		}
	}
}

// resumablePartitions returns the assigned partitions that a user-level
// Resume may resume: every partition except those blocked and still waiting
// out their backoff.
func (c *Consumer) resumablePartitions(assigned []ckafka.TopicPartition) []ckafka.TopicPartition {
	c.blockMu.Lock()
	defer c.blockMu.Unlock()
	out := make([]ckafka.TopicPartition, 0, len(assigned))
	for _, p := range assigned {
		if p.Topic != nil {
			if bs, ok := c.blocked[TopicPartition{Topic: *p.Topic, Partition: p.Partition}]; ok && !bs.resumed {
				continue
			}
		}
		out = append(out, p)
	}
	return out
}

// clearBlockedOnPark drops the blocked state for a partition once its message
// parked at or past the blocked offset — a success resets the escalation.
func (c *Consumer) clearBlockedOnPark(msg *Message) {
	tp := TopicPartition{Topic: msg.Topic, Partition: msg.Partition}
	cleared := false
	c.blockMu.Lock()
	if bs, ok := c.blocked[tp]; ok && msg.Offset >= bs.offset {
		delete(c.blocked, tp)
		cleared = true
	}
	c.blockMu.Unlock()
	if cleared {
		c.metrics.setBlocked(c.blockedSnapshot())
	}
}

// blockedSnapshot returns the currently blocked partitions, in no particular
// order. Offset is left zero.
func (c *Consumer) blockedSnapshot() []TopicPartition {
	c.blockMu.Lock()
	defer c.blockMu.Unlock()
	out := make([]TopicPartition, 0, len(c.blocked))
	for tp := range c.blocked {
		out = append(out, tp)
	}
	return out
}

// dropBlockedFor drops blocked-partition bookkeeping for the given partitions
// (called on revoke — a revoked partition is no longer ours to resume).
func (c *Consumer) dropBlockedFor(ps []ckafka.TopicPartition) {
	c.blockMu.Lock()
	for _, tp := range ps {
		if tp.Topic == nil {
			continue
		}
		delete(c.blocked, TopicPartition{Topic: *tp.Topic, Partition: tp.Partition})
	}
	c.blockMu.Unlock()
	c.metrics.setBlocked(c.blockedSnapshot())
}

// blockBackoff is the escalating-with-cap wait before a blocked partition is
// resumed: InitialInterval doubling per consecutive block of the same message,
// capped at MaxInterval, reset when the message finally parks.
func blockBackoff(blocks int, r RetryConfig) time.Duration {
	initial, maxInterval := r.InitialInterval, r.MaxInterval
	d := initial
	for i := 1; i < blocks && d < maxInterval; i++ {
		d *= 2
	}
	if d > maxInterval {
		d = maxInterval
	}
	return d
}
