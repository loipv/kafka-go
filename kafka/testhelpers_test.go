package kafka

import (
	"strconv"
	"strings"
	"testing"
	"time"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func skipIfShort(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("mock-broker test")
	}
}

func newMockCluster(t *testing.T) *ckafka.MockCluster {
	t.Helper()
	mc, err := ckafka.NewMockCluster(1)
	if err != nil {
		t.Fatalf("NewMockCluster: %v", err)
	}
	t.Cleanup(mc.Close)
	return mc
}

func uniqueTopic(t *testing.T, mc *ckafka.MockCluster, partitions int) string {
	t.Helper()
	name := strings.ToLower(strings.ReplaceAll(t.Name(), "/", "_")) +
		"_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	if err := mc.CreateTopic(name, partitions, 1); err != nil {
		t.Fatalf("CreateTopic(%s): %v", name, err)
	}
	return name
}

// waitFor polls cond until deadline. Fixed sleeps are future flakes.
func waitFor(t *testing.T, d time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal(msg)
}
