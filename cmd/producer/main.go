package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/PraneethaBabburi/analytics-pipeline/internal/models"
	"github.com/go-chi/chi/v5"
	"github.com/segmentio/kafka-go"
)

var writer *kafka.Writer

func main() {
	writer = &kafka.Writer{
		Addr:     kafka.TCP("localhost:9092"),
		Topic:    "analytics-events",
		Balancer: &kafka.LeastBytes{},
	}
	defer writer.Close()

	r := chi.NewRouter()

	r.Post("/events", handleEvent)

	log.Println("Producer running on :8080")
	http.ListenAndServe(":8080", r)
}

func handleEvent(w http.ResponseWriter, r *http.Request) {
	var event models.AnalyticsEvent

	err := json.NewDecoder(r.Body).Decode(&event)
	if err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	if err := event.Validate(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	event.Timestamp = time.Now()
	event.SchemaVersion = "1.0"

	bytes, err := json.Marshal(event)
	if err != nil {
		http.Error(w, "failed to encode event", http.StatusInternalServerError)
		return
	}

	err = writer.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte(event.UserID),
			Value: bytes,
		},
	)
	if err != nil {
		http.Error(w, "failed to publish event", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
	w.Write([]byte("event published"))
}

// func main() {
// 	r := chi.NewRouter()

// 	r.Post("/events", func(w http.ResponseWriter, r *http.Request) {
// 		var event models.AnalyticsEvent
// 		err := json.NewDecoder(r.Body).Decode(&event)
// 		if err != nil {
// 			http.Error(w, "invalid json", http.StatusBadRequest)
// 			return
// 		}
// 		if err := event.Validate(); err != nil {
// 			http.Error(w, err.Error(), http.StatusBadRequest)
// 			return
// 		}
// 		w.Write([]byte("valid event received"))
// 	})

// 	http.ListenAndServe(":8080", r)
// }
