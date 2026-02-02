package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/loipv/kafka-go/kafka"
)

func main() {
	// Create health checker with brokers
	healthChecker := kafka.NewHealthCheckerWithBrokers([]string{"localhost:9092"})

	// Set custom timeout
	healthChecker.SetTimeout(5 * time.Second)

	// Basic health check endpoint
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()

		result := healthChecker.Check(ctx)

		w.Header().Set("Content-Type", "application/json")
		if result.Status != kafka.HealthStatusUp {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		json.NewEncoder(w).Encode(result)
	})

	// Broker connectivity check endpoint
	http.HandleFunc("/health/brokers", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()

		result := healthChecker.CheckBrokers(ctx)

		w.Header().Set("Content-Type", "application/json")
		if result.Status != kafka.HealthStatusUp {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		json.NewEncoder(w).Encode(result)
	})

	// Topic health check endpoint
	http.HandleFunc("/health/topic", func(w http.ResponseWriter, r *http.Request) {
		topic := r.URL.Query().Get("topic")
		if topic == "" {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{
				"error": "topic query param required",
			})
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()

		result := healthChecker.CheckTopic(ctx, topic)

		w.Header().Set("Content-Type", "application/json")
		if result.Status != kafka.HealthStatusUp {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		json.NewEncoder(w).Encode(result)
	})

	// Consumer lag check endpoint
	http.HandleFunc("/health/lag", func(w http.ResponseWriter, r *http.Request) {
		groupID := r.URL.Query().Get("group")
		if groupID == "" {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{
				"error": "group query param required",
			})
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()

		// Max acceptable lag: 1000 messages
		result := healthChecker.CheckConsumerLag(ctx, groupID, 1000)

		w.Header().Set("Content-Type", "application/json")
		if result.Status != kafka.HealthStatusUp {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		json.NewEncoder(w).Encode(result)
	})

	// Readiness endpoint (for Kubernetes)
	http.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()

		result := healthChecker.Check(ctx)

		if result.Status == kafka.HealthStatusUp {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("OK"))
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte("NOT READY"))
		}
	})

	// Liveness endpoint (for Kubernetes)
	http.HandleFunc("/live", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	log.Println("Starting health check server on :8080")
	log.Println("Endpoints:")
	log.Println("  GET /health          - Basic Kafka health check")
	log.Println("  GET /health/brokers  - Broker connectivity check")
	log.Println("  GET /health/topic?topic=orders - Topic health check")
	log.Println("  GET /health/lag?group=my-group - Consumer lag check")
	log.Println("  GET /ready           - Kubernetes readiness probe")
	log.Println("  GET /live            - Kubernetes liveness probe")

	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}
