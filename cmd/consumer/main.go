package main

import (
	"context"
	"encoding/json"
	"log"

	"github.com/PraneethaBabburi/analytics-pipeline/internal/models"
	"github.com/segmentio/kafka-go"
)

func main() {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    "analytics-events",
		GroupID:  "analytics-consumer-group",
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})
	defer reader.Close()

	log.Println("Consumer running, waiting for events...")

	for {
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Println("error reading message:", err)
			continue
		}

		var event models.AnalyticsEvent
		err = json.Unmarshal(msg.Value, &event)
		if err != nil {
			log.Println("error parsing event:", err)
			continue
		}

		log.Printf("received event: userID=%s type=%s time=%s",
			event.UserID,
			event.EventType,
			event.Timestamp,
		)
	}
}
