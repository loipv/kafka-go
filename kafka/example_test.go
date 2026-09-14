package kafka_test

import (
	"context"
	"fmt"

	"github.com/loipv/kafka-go/kafka"
)

// ExampleNewProducer constructs a producer and sends one confirmed message
// (Produce waits for the delivery report). The example has no // Output:
// comment on purpose: the produce result depends on a live broker, so godoc
// treats it as compile-checked only.
func ExampleNewProducer() {
	p, err := kafka.NewProducer(
		kafka.ProducerWithBrokers("localhost:9092"),
		kafka.ProducerWithAcks(kafka.AcksAll),
	)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer p.Close()

	err = p.Produce(context.Background(), "orders", &kafka.Message{
		Key:   []byte("order-1"),
		Value: []byte(`{"id":"order-1"}`),
	})
	fmt.Println(err)
}
