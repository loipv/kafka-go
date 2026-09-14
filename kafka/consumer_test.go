package kafka

import (
	"context"
	"errors"
	"testing"
)

func TestStartRequiresHandler(t *testing.T) {
	skipIfShort(t) // no broker needed, but keeps policy uniform
	c, err := NewConsumer(ConsumerWithBrokers("localhost:9092"),
		ConsumerWithGroupID("g"), ConsumerWithTopics("t"))
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	defer c.Close(context.Background())
	if err := c.Start(context.Background()); err == nil {
		t.Fatal("Start() = nil with no handler registered, want error")
	}
}

func TestInvokeHandlerDispatch(t *testing.T) {
	tests := []struct {
		name   string
		setup  func(*Consumer)
		wantBy string // which handler must fire
	}{
		{"message handler wins", func(c *Consumer) {
			c.messageHandler = func(context.Context, *Message) error { return errors.New("m") }
			c.batchHandler = func(context.Context, []*Message) error { return errors.New("b") }
		}, "m"},
		{"batch handler wraps single message", func(c *Consumer) {
			c.batchHandler = func(_ context.Context, msgs []*Message) error {
				if len(msgs) != 1 {
					t.Errorf("batch handler got %d msgs, want 1", len(msgs))
				}
				return errors.New("b")
			}
		}, "b"},
		{"grouped handler wraps one group", func(c *Consumer) {
			c.groupedBatchHandler = func(_ context.Context, groups []GroupedBatch) error {
				if len(groups) != 1 || len(groups[0].Messages) != 1 {
					t.Errorf("grouped handler got %+v, want 1 group/1 msg", groups)
				}
				return errors.New("g")
			}
		}, "g"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Consumer{}
			tt.setup(c)
			if err := c.invokeHandler(context.Background(), &Message{Key: []byte("k")}); err == nil || err.Error() != tt.wantBy {
				t.Errorf("invokeHandler() = %v, want %q", err, tt.wantBy)
			}
		})
	}
}
