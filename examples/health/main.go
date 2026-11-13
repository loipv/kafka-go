// Command health serves HTTP endpoints reporting Kafka cluster, topic, and
// consumer-lag health.
package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/loipv/kafka-go/kafka"
)

// writeResult sends result as JSON, with 503 unless the check is UP.
func writeResult(w http.ResponseWriter, result *kafka.HealthResult) {
	w.Header().Set("Content-Type", "application/json")
	if result.Status != kafka.HealthStatusUp {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	_ = json.NewEncoder(w).Encode(result)
}

func main() {
	// The health checker owns one connection, built from producer options.
	healthChecker, err := kafka.NewHealthChecker(kafka.ProducerWithBrokers("localhost:9092"))
	if err != nil {
		log.Fatalf("Failed to create health checker: %v", err)
	}
	defer func() { _ = healthChecker.Close() }()

	// Each check is bounded by its context deadline (10s if there is none),
	// so give every call a timeout.

	// Cluster reachability: is at least one broker answering?
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()

		writeResult(w, healthChecker.Check(ctx))
	})

	// Topic health: does the topic exist, with its partitions and leaders?
	http.HandleFunc("/health/topic", func(w http.ResponseWriter, r *http.Request) {
		topic := r.URL.Query().Get("topic")
		if topic == "" {
			http.Error(w, "topic query param required", http.StatusBadRequest)
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()

		writeResult(w, healthChecker.CheckTopic(ctx, topic))
	})

	// Consumer lag: DOWN when the group's total lag exceeds the limit.
	http.HandleFunc("/health/lag", func(w http.ResponseWriter, r *http.Request) {
		groupID := r.URL.Query().Get("group")
		if groupID == "" {
			http.Error(w, "group query param required", http.StatusBadRequest)
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()

		// Max acceptable lag: 1000 messages
		writeResult(w, healthChecker.CheckConsumerLag(ctx, groupID, 1000))
	})

	// Readiness endpoint (for Kubernetes)
	http.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()

		if healthChecker.Check(ctx).Status != kafka.HealthStatusUp {
			http.Error(w, "NOT READY", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("OK"))
	})

	// Liveness endpoint (for Kubernetes): does not depend on Kafka
	http.HandleFunc("/live", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("OK"))
	})

	log.Println("Starting health check server on :8080")
	log.Println("Endpoints:")
	log.Println("  GET /health                    - Kafka cluster health check")
	log.Println("  GET /health/topic?topic=orders - Topic health check")
	log.Println("  GET /health/lag?group=my-group - Consumer lag check")
	log.Println("  GET /ready                     - Kubernetes readiness probe")
	log.Println("  GET /live                      - Kubernetes liveness probe")

	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}
