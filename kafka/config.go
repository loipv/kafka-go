package kafka

import (
	"strings"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// connConfig is the connection/auth configuration shared by every
// librdkafka handle in this package. It exists so the DLQ producer, the DLQ
// retry consumer, and the health checker can never again forget SSL/SASL.
type connConfig struct {
	Brokers  []string
	ClientID string
	SSL      bool
	SASL     *SASLConfig
	// Raw is merged last — the escape hatch for librdkafka keys this
	// library does not model (e.g. queued.max.messages.kbytes).
	Raw map[string]any
}

// configMap returns the shared connection/auth keys. ckafka.ConfigMap is a
// plain map — direct assignment cannot fail, so there are no SetKey errors
// to ignore.
func (cc connConfig) configMap() ckafka.ConfigMap {
	cm := ckafka.ConfigMap{
		"bootstrap.servers": strings.Join(cc.Brokers, ","),
	}
	if cc.ClientID != "" {
		cm["client.id"] = cc.ClientID
	}
	switch {
	case cc.SASL != nil && cc.SSL:
		cm["security.protocol"] = "sasl_ssl"
	case cc.SASL != nil:
		cm["security.protocol"] = "sasl_plaintext"
	case cc.SSL:
		cm["security.protocol"] = "ssl"
	}
	if cc.SASL != nil {
		cm["sasl.mechanism"] = cc.SASL.Mechanism
		cm["sasl.username"] = cc.SASL.Username
		cm["sasl.password"] = cc.SASL.Password
	}
	for k, v := range cc.Raw {
		cm[k] = v
	}
	return cm
}

func (c *ProducerConfig) conn() connConfig {
	return connConfig{Brokers: c.Brokers, ClientID: c.ClientID, SSL: c.SSL, SASL: c.SASL, Raw: c.Raw}
}

func (c *ConsumerConfig) conn() connConfig {
	return connConfig{Brokers: c.Brokers, SSL: c.SSL, SASL: c.SASL, Raw: c.Raw}
}

func buildProducerConfig(c *ProducerConfig) ckafka.ConfigMap {
	cm := c.conn().configMap()
	cm["acks"] = int(c.Acks)
	if c.Compression != CompressionNone {
		cm["compression.type"] = getCompressionName(c.Compression)
	}
	if c.Idempotent {
		cm["enable.idempotence"] = true
	}
	if c.ConnectionTimeout > 0 {
		cm["socket.connection.setup.timeout.ms"] = int(c.ConnectionTimeout.Milliseconds())
	}
	if c.RequestTimeout > 0 {
		cm["request.timeout.ms"] = int(c.RequestTimeout.Milliseconds())
	}
	if c.Retry != nil {
		cm["retries"] = c.Retry.MaxRetries
		cm["retry.backoff.ms"] = int(c.Retry.InitialInterval.Milliseconds())
		if c.Retry.MaxInterval > 0 {
			cm["retry.backoff.max.ms"] = int(c.Retry.MaxInterval.Milliseconds())
		}
	}
	return cm
}

func buildConsumerConfig(c *ConsumerConfig) ckafka.ConfigMap {
	cm := c.conn().configMap()
	cm["group.id"] = c.GroupID
	cm["auto.offset.reset"] = getOffsetReset(c.FromBeginning)
	cm["enable.auto.commit"] = c.AutoCommit
	if c.SessionTimeout > 0 {
		cm["session.timeout.ms"] = int(c.SessionTimeout.Milliseconds())
	}
	if c.HeartbeatInterval > 0 {
		cm["heartbeat.interval.ms"] = int(c.HeartbeatInterval.Milliseconds())
	}
	if c.AutoCommitInterval > 0 {
		cm["auto.commit.interval.ms"] = int(c.AutoCommitInterval.Milliseconds())
	}
	if c.PartitionAssignor != "" {
		cm["partition.assignment.strategy"] = string(c.PartitionAssignor)
	}
	return cm
}
