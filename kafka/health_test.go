package kafka

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestHealth_CheckAgainstMockCluster(t *testing.T) {
	skipIfShort(t)
	mc := newMockCluster(t)
	hc, err := NewHealthChecker(ProducerWithBrokers(mc.BootstrapServers()))
	if err != nil {
		t.Fatalf("NewHealthChecker: %v", err)
	}
	defer hc.Close()

	// Each check gets a 2s deadline so DOWN is detected well inside waitFor.
	check := func() *HealthResult {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		return hc.Check(ctx)
	}

	if res := check(); res.Status != HealthStatusUp {
		t.Fatalf("Check() = %s (%s), want UP", res.Status, res.Error)
	}

	// MockCluster broker ids start at 1 (not 0).
	if err := mc.SetBrokerDown(1); err != nil {
		t.Fatalf("SetBrokerDown: %v", err)
	}
	waitFor(t, 20*time.Second, func() bool {
		return check().Status == HealthStatusDown
	}, "Check() never reported DOWN with the only broker down")
}

func TestHealth_LagForPartition(t *testing.T) {
	tests := []struct {
		name            string
		high, committed int64
		want            int64
	}{
		{"lag is high minus committed", 100, 90, 10},
		{"no committed offset means no lag", 100, -1, 0},
		{"committed at high means zero", 100, 100, 0},
		{"committed beyond high clamps to zero", 50, 60, 0},
		{"empty partition", 0, 0, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := lagForPartition(tt.high, tt.committed); got != tt.want {
				t.Errorf("lagForPartition(%d, %d) = %d, want %d", tt.high, tt.committed, got, tt.want)
			}
		})
	}
}

func TestHealth_QueryTimeoutMs(t *testing.T) {
	h := &HealthChecker{} // zero value: defaultHealthTimeout (10s) when ctx has no deadline

	t.Run("cancelled context returns its error", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := h.queryTimeoutMs(ctx); !errors.Is(err, context.Canceled) {
			t.Errorf("err = %v, want context.Canceled", err)
		}
	})
	t.Run("expired deadline returns a deadline error", func(t *testing.T) {
		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
		defer cancel()
		if _, err := h.queryTimeoutMs(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("err = %v, want context.DeadlineExceeded", err)
		}
	})
	t.Run("no deadline uses the health timeout", func(t *testing.T) {
		ms, err := h.queryTimeoutMs(context.Background())
		if err != nil || ms != 10000 {
			t.Errorf("= (%d, %v), want (10000, nil)", ms, err)
		}
	})
	t.Run("shorter deadline clamps the timeout", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		ms, err := h.queryTimeoutMs(ctx)
		if err != nil || ms <= 0 || ms > 2000 {
			t.Errorf("= (%d, %v), want 0 < ms <= 2000 and nil", ms, err)
		}
	})
}

func TestHealth_CheckTopic(t *testing.T) {
	skipIfShort(t)
	mc := newMockCluster(t)
	topic := uniqueTopic(t, mc, 3)
	hc, err := NewHealthChecker(ProducerWithBrokers(mc.BootstrapServers()))
	if err != nil {
		t.Fatalf("NewHealthChecker: %v", err)
	}
	defer hc.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	res := hc.CheckTopic(ctx, topic)
	if res.Status != HealthStatusUp {
		t.Fatalf("CheckTopic(%s) = %s (%s), want UP", topic, res.Status, res.Error)
	}
	if got := res.Details["partitionCount"]; got != 3 {
		t.Errorf("Details[partitionCount] = %v, want 3", got)
	}
}

func TestHealth_CancelledContext(t *testing.T) {
	skipIfShort(t)
	mc := newMockCluster(t)
	hc, err := NewHealthChecker(ProducerWithBrokers(mc.BootstrapServers()))
	if err != nil {
		t.Fatalf("NewHealthChecker: %v", err)
	}
	defer hc.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for name, res := range map[string]*HealthResult{
		"Check":            hc.Check(ctx),
		"CheckTopic":       hc.CheckTopic(ctx, "any"),
		"CheckConsumerLag": hc.CheckConsumerLag(ctx, "any-group", 0),
	} {
		if res.Status != HealthStatusDown || res.Error == "" {
			t.Errorf("%s with a cancelled context = %s (%q), want DOWN with an error", name, res.Status, res.Error)
		}
	}
}

// Probing a topic must never create it: the checker's metadata request runs on
// a producer handle, where librdkafka would otherwise ask for auto-creation.
func TestHealth_CheckTopicMissingTopicIsDown(t *testing.T) {
	skipIfShort(t)
	mc := newMockCluster(t)
	hc, err := NewHealthChecker(ProducerWithBrokers(mc.BootstrapServers()))
	if err != nil {
		t.Fatalf("NewHealthChecker: %v", err)
	}
	defer hc.Close()

	topic := uniqueGroupName(t) // a name no test created
	for i := range 2 {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		res := hc.CheckTopic(ctx, topic)
		cancel()
		if res.Status != HealthStatusDown || res.Error == "" {
			t.Fatalf("CheckTopic(missing) call %d = %s (%q), want DOWN with an error", i+1, res.Status, res.Error)
		}
	}
}

// expiredDeadlineCtx reports a deadline that has passed while Err is still
// nil: the window between a context's deadline and its timer firing.
type expiredDeadlineCtx struct{ context.Context }

func (expiredDeadlineCtx) Deadline() (time.Time, bool) {
	return time.Now().Add(-time.Millisecond), true
}

// Check and CheckTopic must never hand librdkafka a non-positive timeout: with
// no time left they report DOWN with the deadline error and skip the query (the
// zero-value checker has no client, so a query would panic).
func TestHealth_NoTimeLeftIsDownWithoutQuery(t *testing.T) {
	h := &HealthChecker{}
	ctx := expiredDeadlineCtx{context.Background()}
	for name, check := range map[string]func() *HealthResult{
		"Check":      func() *HealthResult { return h.Check(ctx) },
		"CheckTopic": func() *HealthResult { return h.CheckTopic(ctx, "orders") },
	} {
		t.Run(name, func(t *testing.T) {
			res := check()
			if res.Status != HealthStatusDown || res.Error != context.DeadlineExceeded.Error() {
				t.Errorf("%s = %s (%q), want DOWN with %q", name, res.Status, res.Error, context.DeadlineExceeded)
			}
		})
	}
}
