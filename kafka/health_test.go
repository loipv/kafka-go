package kafka

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestLagForPartition(t *testing.T) {
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

func TestQueryTimeoutMs(t *testing.T) {
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
