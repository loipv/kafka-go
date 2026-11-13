package kafka

import (
	"testing"
	"time"
)

func TestNormalizeRetry(t *testing.T) {
	def := RetryConfig{MaxRetries: 3, InitialInterval: time.Second, MaxInterval: 30 * time.Second, Multiplier: 2}
	if got := normalizeRetry(nil); got != def {
		t.Errorf("normalizeRetry(nil) = %+v, want %+v", got, def)
	}
	if got := normalizeRetry(&RetryConfig{}); got != def {
		t.Errorf("normalizeRetry(zero) = %+v, want defaults", got)
	}
	got := normalizeRetry(&RetryConfig{MaxRetries: 5, InitialInterval: 10 * time.Millisecond, SkipOnMaxRetries: true})
	want := RetryConfig{MaxRetries: 5, InitialInterval: 10 * time.Millisecond, MaxInterval: 30 * time.Second, Multiplier: 2, SkipOnMaxRetries: true}
	if got != want {
		t.Errorf("normalizeRetry(partial) = %+v, want %+v", got, want)
	}
}
