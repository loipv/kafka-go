package kafka

import "testing"

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
