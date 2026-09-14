//go:build integration

// T3 integration suite: the three proofs a MockCluster cannot provide —
// SASL auth end to end (#1), cooperative rebalance with two real consumers
// (#5), and real consumer lag (#4). Runs against a Redpanda testcontainer
// with SASL enabled; Docker required:
//
//	go test -tags=integration -timeout=15m -run TestIntegration ./kafka/...
//
// Shared helpers (waitFor, uniqueGroupName) come from testhelpers_test.go,
// which is untagged and therefore compiled under this tag too. Patterns from
// other untagged test files (fastAtLeastOnce, recvMessages, committedOffset)
// are replicated here instead of imported: those files are test code, not
// the shared-helper file.

package kafka

import (
	"context"
	"errors"
	"log"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
)

var rp *redpanda.Container

func TestMain(m *testing.M) {
	ctx := context.Background()
	c, err := redpanda.Run(ctx, "redpandadata/redpanda:v24.2.7",
		redpanda.WithEnableSASL(),
		redpanda.WithAutoCreateTopics(),
		redpanda.WithNewServiceAccount("admin", "secret"),
		redpanda.WithSuperusers("admin"),
	)
	if err != nil {
		log.Fatalf("redpanda: %v", err)
	}
	rp = c
	code := m.Run()
	_ = testcontainers.TerminateContainer(c)
	os.Exit(code)
}

// saslConfig matches the container's service account. Redpanda defaults to
// SCRAM-SHA-256 — PLAIN fails for the wrong reason.
func saslConfig() *SASLConfig {
	return &SASLConfig{Mechanism: "SCRAM-SHA-256", Username: "admin", Password: "secret"}
}

func seedBroker(t *testing.T) string {
	t.Helper()
	b, err := rp.KafkaSeedBroker(context.Background())
	if err != nil {
		t.Fatalf("KafkaSeedBroker: %v", err)
	}
	return b
}

// integTopic is the MockCluster-free twin of testhelpers' uniqueTopic: a
// per-run lowercase topic name (the container auto-creates on first produce;
// explicit partition counts come from createTopic).
func integTopic(t *testing.T) string {
	t.Helper()
	return strings.ToLower(strings.ReplaceAll(t.Name(), "/", "_")) +
		"_" + strconv.FormatInt(time.Now().UnixNano(), 36)
}

// createTopic creates a topic with an explicit partition count via an admin
// client that carries SASL through the same connConfig seam as the library's
// own handles.
func createTopic(t *testing.T, brokers, name string, partitions int) {
	t.Helper()
	cm := connConfig{Brokers: []string{brokers}, SASL: saslConfig()}.configMap()
	admin, err := ckafka.NewAdminClient(&cm)
	if err != nil {
		t.Fatalf("admin: %v", err)
	}
	defer admin.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := admin.CreateTopics(ctx,
		[]ckafka.TopicSpecification{{Topic: name, NumPartitions: partitions, ReplicationFactor: 1}},
		ckafka.SetAdminRequestTimeout(10*time.Second)); err != nil {
		t.Fatalf("CreateTopic(%s, %d partitions): %v", name, partitions, err)
	}
}

// fastIntegration mirrors the untagged suite's fastAtLeastOnce option set:
// quick retry cycles and a short auto-commit interval so committed offsets
// move inside waitFor deadlines.
func fastIntegration(extra ...ConsumerOption) []ConsumerOption {
	opts := []ConsumerOption{
		ConsumerWithRetry(&RetryConfig{MaxRetries: 1, InitialInterval: 10 * time.Millisecond}),
		ConsumerWithAutoCommit(true),
		ConsumerWithAutoCommitInterval(200 * time.Millisecond),
		ConsumerWithFromBeginning(true),
	}
	return append(opts, extra...)
}

// recvFrom reads n messages from ch before the deadline. The handler-side
// channel is buffered, so the poll loop never blocks on the test.
func recvFrom(t *testing.T, ch <-chan *Message, n int, d time.Duration) []*Message {
	t.Helper()
	deadline := time.Now().Add(d)
	var out []*Message
	for time.Now().Before(deadline) && len(out) < n {
		select {
		case m := <-ch:
			out = append(out, m)
		case <-time.After(20 * time.Millisecond):
		}
	}
	if len(out) < n {
		t.Fatalf("received %d messages, want %d within %v", len(out), n, d)
	}
	return out
}

// saslCommitted is the SASL-speaking twin of the untagged suite's
// committedOffset helper.
func saslCommitted(t *testing.T, brokers, group, topic string, partition int32) int64 {
	t.Helper()
	cm := connConfig{Brokers: []string{brokers}, SASL: saslConfig()}.configMap()
	admin, err := ckafka.NewAdminClient(&cm)
	if err != nil {
		t.Fatalf("admin: %v", err)
	}
	defer admin.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	res, err := admin.ListConsumerGroupOffsets(ctx, []ckafka.ConsumerGroupTopicPartitions{{Group: group,
		Partitions: []ckafka.TopicPartition{{Topic: &topic, Partition: partition}}}})
	if err != nil || len(res.ConsumerGroupsTopicPartitions) == 0 {
		return -1
	}
	for _, g := range res.ConsumerGroupsTopicPartitions {
		for _, tp := range g.Partitions {
			if tp.Partition == partition {
				return int64(tp.Offset)
			}
		}
	}
	return -1
}

// TestIntegrationSASL_ProduceConsumeDLQHealth is proof #1: every client the
// library owns — producer, consumer, the DLQ's internal producer, and the
// health checker — authenticates against a broker that rejects
// unauthenticated clients. On v1.0.0 with auth on, none of this path works.
func TestIntegrationSASL_ProduceConsumeDLQHealth(t *testing.T) {
	seed := seedBroker(t)

	// --- produce + consume round-trip over SASL ---
	rtTopic := integTopic(t) // auto-created on first produce
	p, err := NewProducer(ProducerWithBrokers(seed), ProducerWithSASL(saslConfig()))
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	defer p.Close()

	rtGroup := uniqueGroupName(t)
	got := make(chan *Message, 4)
	rc, err := NewConsumer(fastIntegration(
		ConsumerWithBrokers(seed), ConsumerWithSASL(saslConfig()),
		ConsumerWithGroupID(rtGroup), ConsumerWithTopics(rtTopic),
	)...)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	rc.OnMessage(func(_ context.Context, m *Message) error { got <- m; return nil })
	rctx, rcancel := context.WithCancel(context.Background())
	defer rcancel()
	go rc.Start(rctx)
	defer rc.Close(context.Background())

	sent := &Message{
		Key:     []byte("sasl-key"),
		Value:   []byte("sasl-value"),
		Headers: Headers{"h1": []byte("v1"), "h2": []byte("v2")},
	}
	pctx, pcancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer pcancel()
	if err := p.Produce(pctx, rtTopic, sent); err != nil {
		t.Fatalf("Produce over SASL: %v", err)
	}
	msg := recvFrom(t, got, 1, 30*time.Second)[0]
	if string(msg.Key) != string(sent.Key) || string(msg.Value) != string(sent.Value) {
		t.Errorf("round-trip key/value = %q/%q, want %q/%q", msg.Key, msg.Value, sent.Key, sent.Value)
	}
	for k, v := range sent.Headers {
		if string(msg.Headers[k]) != string(v) {
			t.Errorf("header %q = %q, want %q", k, msg.Headers[k], v)
		}
	}
	if msg.Topic != rtTopic {
		t.Errorf("Topic = %q, want %q", msg.Topic, rtTopic)
	}

	// --- failing handler parks the message on a SASL-protected DLQ ---
	srcTopic, dlqTopic := integTopic(t), integTopic(t)
	dlqGroup := uniqueGroupName(t)
	handlerErr := errors.New("handler always fails")
	failer, err := NewConsumer(fastIntegration(
		ConsumerWithBrokers(seed), ConsumerWithSASL(saslConfig()),
		ConsumerWithGroupID(dlqGroup), ConsumerWithTopics(srcTopic),
		ConsumerWithDLQ(&DLQConfig{Topic: dlqTopic, IncludeErrorInfo: true,
			MaxRetries: 1, RetryDelay: 10 * time.Millisecond, RetryBackoffMultiplier: 1}),
	)...)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	failer.OnMessage(func(context.Context, *Message) error { return handlerErr })
	fctx, fcancel := context.WithCancel(context.Background())
	defer fcancel()
	go failer.Start(fctx)
	defer failer.Close(context.Background())

	// Fresh deadline: the round-trip above may have consumed most of pctx's
	// budget waiting for its consumer to join.
	dctx, dcancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer dcancel()
	if err := p.Produce(dctx, srcTopic, &Message{Value: []byte("poison")}); err != nil {
		t.Fatalf("Produce to DLQ source: %v", err)
	}

	// Read the DLQ copy with a raw SASL consumer.
	scm := connConfig{Brokers: []string{seed}, SASL: saslConfig()}.configMap()
	scm["group.id"] = uniqueGroupName(t)
	scm["auto.offset.reset"] = "earliest"
	scratch, err := ckafka.NewConsumer(&scm)
	if err != nil {
		t.Fatalf("scratch consumer: %v", err)
	}
	defer scratch.Close()
	if err := scratch.Subscribe(dlqTopic, nil); err != nil {
		t.Fatalf("subscribe dlq: %v", err)
	}
	var dlqMsg *ckafka.Message
	deadline := time.Now().Add(30 * time.Second)
	for dlqMsg == nil && time.Now().Before(deadline) {
		if m, err := scratch.ReadMessage(200 * time.Millisecond); err == nil {
			dlqMsg = m
		}
	}
	if dlqMsg == nil {
		t.Fatal("message never arrived on the SASL-protected DLQ topic")
	}
	hdrs := map[string][]byte{}
	for _, h := range dlqMsg.Headers {
		hdrs[h.Key] = h.Value
	}
	if got := string(hdrs["x-dlq-original-topic"]); got != srcTopic {
		t.Errorf("x-dlq-original-topic = %q, want %q", got, srcTopic)
	}
	if msg := string(hdrs["x-dlq-error-message"]); !strings.Contains(msg, handlerErr.Error()) {
		t.Errorf("x-dlq-error-message = %q, want it to contain %q", msg, handlerErr.Error())
	}
	if got := string(hdrs["x-dlq-handler-retry-count"]); got != "2" { // fastIntegration pins MaxRetries=1
		t.Errorf("x-dlq-handler-retry-count = %q, want %q", got, "2")
	}
	if string(dlqMsg.Value) != "poison" {
		t.Errorf("dlq value = %q, want the original payload", dlqMsg.Value)
	}

	// Confirmed DLQ produce parks the message: the source offset advances.
	waitFor(t, 20*time.Second, func() bool {
		return saslCommitted(t, seed, dlqGroup, srcTopic, 0) >= 1
	}, "source offset never advanced past the DLQ-parked message")

	// --- health checks authenticate ---
	hc, err := NewHealthChecker(ProducerWithBrokers(seed), ProducerWithSASL(saslConfig()))
	if err != nil {
		t.Fatalf("NewHealthChecker: %v", err)
	}
	defer hc.Close()
	hc.SetTimeout(5 * time.Second)
	if res := hc.Check(context.Background()); res.Status != HealthStatusUp {
		t.Errorf("Check() over SASL = %s (%s), want UP", res.Status, res.Error)
	}
}

// eventRecorder tracks the rebalance callback's net assigned partitions
// (assigned minus revoked) for one consumer.
type eventRecorder struct {
	mu  sync.Mutex
	net int
}

func (r *eventRecorder) callback(ev RebalanceEvent) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	switch ev.Type {
	case "assigned":
		r.net += len(ev.Partitions)
	case "revoked":
		r.net -= len(ev.Partitions)
	}
	return nil
}

func (r *eventRecorder) assigned() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.net
}

// captureHandler records Warn+ log lines. A failed rebalance operation —
// v1.0.0's eager Assign() under the cooperative protocol — surfaces here as
// a read/rebalance error at Warn or above.
type captureHandler struct {
	mu  sync.Mutex
	buf []string
}

func (h *captureHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *captureHandler) Handle(_ context.Context, r slog.Record) error {
	if r.Level >= slog.LevelWarn {
		var b strings.Builder
		b.WriteString(r.Message)
		r.Attrs(func(a slog.Attr) bool {
			b.WriteString(" ")
			b.WriteString(a.Key)
			b.WriteString("=")
			b.WriteString(a.Value.String())
			return true
		})
		h.mu.Lock()
		h.buf = append(h.buf, b.String())
		h.mu.Unlock()
	}
	return nil
}

func (h *captureHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *captureHandler) WithGroup(string) slog.Handler      { return h }

// rebalanceFailures returns Warn+ lines mentioning rebalance/assign — the
// "partitions assigned/revoked" Info logs never match.
func (h *captureHandler) rebalanceFailures() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []string
	for _, line := range h.buf {
		l := strings.ToLower(line)
		if strings.Contains(l, "rebalance") || strings.Contains(l, "assign") {
			out = append(out, line)
		}
	}
	return out
}

// TestIntegrationCooperativeRebalance is proof #5: two consumers share a
// 6-partition topic under the cooperative-sticky assignor; stopping one
// grows the survivor's assignment — and the growth is real, confirmed
// against librdkafka's own Assignment(). v1.0.0 called the eager Assign()
// under the cooperative protocol, which the broker rejects, so the takeover
// could never complete.
func TestIntegrationCooperativeRebalance(t *testing.T) {
	seed := seedBroker(t)
	topic := integTopic(t)
	createTopic(t, seed, topic, 6)
	group := uniqueGroupName(t)

	newCoopConsumer := func(rec *eventRecorder, logs *captureHandler) *Consumer {
		c, err := NewConsumer(fastIntegration(
			ConsumerWithBrokers(seed), ConsumerWithSASL(saslConfig()),
			ConsumerWithGroupID(group), ConsumerWithTopics(topic),
			ConsumerWithPartitionAssignor(AssignorCooperativeSticky),
			ConsumerWithRebalanceCallback(rec.callback),
			ConsumerWithLogger(slog.New(logs)),
		)...)
		if err != nil {
			t.Fatalf("NewConsumer: %v", err)
		}
		c.OnMessage(func(context.Context, *Message) error { return nil })
		return c
	}

	recA, recB := &eventRecorder{}, &eventRecorder{}
	logsA, logsB := &captureHandler{}, &captureHandler{}
	a := newCoopConsumer(recA, logsA)
	b := newCoopConsumer(recB, logsB)

	ctxA, cancelA := context.WithCancel(context.Background())
	defer cancelA()
	go a.Start(ctxA)
	// Idempotent after the explicit mid-test Close: the CAS makes the second
	// call a no-op, so this is pure failure-path insurance.
	defer a.Close(context.Background())
	ctxB, cancelB := context.WithCancel(context.Background())
	defer cancelB()
	go b.Start(ctxB)
	defer b.Close(context.Background())

	// Both consumers join and the 6 partitions are split between them.
	waitFor(t, 60*time.Second, func() bool {
		na, nb := recA.assigned(), recB.assigned()
		return na > 0 && nb > 0 && na+nb == 6
	}, "initial cooperative assignment never split 6 partitions across both consumers")

	// Stop consumer A: the survivor must take over A's partitions.
	cancelA()
	if err := a.Close(context.Background()); err != nil {
		t.Fatalf("close first consumer: %v", err)
	}
	waitFor(t, 90*time.Second, func() bool {
		if recB.assigned() != 6 {
			return false
		}
		// Ground truth: the handle only reports partitions that were actually
		// (incrementally) assigned by the broker.
		partitions, err := b.consumer.Assignment()
		return err == nil && len(partitions) == 6
	}, "survivor's assignment never grew to all 6 partitions after the other consumer left")

	for name, logs := range map[string]*captureHandler{"consumer a": logsA, "consumer b": logsB} {
		if fails := logs.rebalanceFailures(); len(fails) > 0 {
			t.Errorf("%s logged rebalance failures: %v", name, fails)
		}
	}
}

// TestIntegrationConsumerLag is proof #4: real consumer lag. 100 produced,
// 10 consumed and explicitly committed on a single-partition topic →
// CheckConsumerLag(maxLag=50) must report DOWN with lag exactly 90 — on
// v1.0.0 the check could never return DOWN.
func TestIntegrationConsumerLag(t *testing.T) {
	seed := seedBroker(t)
	topic := integTopic(t)
	createTopic(t, seed, topic, 1) // single partition: lag arithmetic is exact
	group := uniqueGroupName(t)

	const total, consumedN = 100, 10

	p, err := NewProducer(ProducerWithBrokers(seed), ProducerWithSASL(saslConfig()))
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	defer p.Close()
	batch := make([]*Message, total)
	for i := range batch {
		batch[i] = &Message{Value: []byte("v")}
	}
	pctx, pcancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer pcancel()
	if err := p.ProduceBatch(pctx, topic, batch); err != nil {
		t.Fatalf("ProduceBatch: %v", err)
	}

	c, err := NewConsumer(
		ConsumerWithBrokers(seed), ConsumerWithSASL(saslConfig()),
		ConsumerWithGroupID(group), ConsumerWithTopics(topic),
		ConsumerWithFromBeginning(true),
		ConsumerWithAutoCommit(false), // only the explicitly committed 10 count
	)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	got := make(chan *Message, total)
	c.OnMessage(func(_ context.Context, m *Message) error { got <- m; return nil })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Start(ctx)
	defer c.Close(context.Background())

	first := recvFrom(t, got, consumedN, 30*time.Second)
	if err := c.Commit(first...); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	hc, err := NewHealthChecker(ProducerWithBrokers(seed), ProducerWithSASL(saslConfig()))
	if err != nil {
		t.Fatalf("NewHealthChecker: %v", err)
	}
	defer hc.Close()

	var lagged *HealthResult
	waitFor(t, 60*time.Second, func() bool {
		lagged = hc.CheckConsumerLag(context.Background(), group, 50)
		return lagged.Status == HealthStatusDown
	}, "CheckConsumerLag(maxLag=50) never reported DOWN with 90 messages of committed lag")
	if lag, ok := lagged.Details["lag"].(int64); !ok || lag != 90 {
		t.Errorf("lag = %v, want 90 (100 high watermark - 10 committed)", lagged.Details["lag"])
	}
	if res := hc.CheckConsumerLag(context.Background(), group, 200); res.Status != HealthStatusUp {
		t.Errorf("CheckConsumerLag(maxLag=200) = %s (%s), want UP with lag 90", res.Status, res.Error)
	}
}
