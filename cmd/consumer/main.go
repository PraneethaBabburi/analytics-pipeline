package main

import (
	"context"
	"encoding/json"
	"log"

	"cloud.google.com/go/bigquery"
	"github.com/PraneethaBabburi/analytics-pipeline/internal/models"
	"github.com/segmentio/kafka-go"
)

type EventRow struct {
	UserID        string `bigquery:"user_id"`
	EventType     string `bigquery:"event_type"`
	Timestamp     string `bigquery:"timestamp"`
	SchemaVersion string `bigquery:"schema_version"`
}

var (
	projectID = "analytics-dev-personal"
	datasetID = "analytics"
	tableID   = "events"
)

func main() {
	ctx := context.Background()

	bqClient, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		log.Fatalf("failed to create BigQuery client: %v", err)
	}
	defer bqClient.Close()

	inserter := bqClient.Dataset(datasetID).Table(tableID).Inserter()

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
		msg, err := reader.ReadMessage(ctx)
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

		row := EventRow{
			UserID:        event.UserID,
			EventType:     event.EventType,
			Timestamp:     event.Timestamp.Format("2006-01-02 15:04:05"),
			SchemaVersion: event.SchemaVersion,
		}

		err = inserter.Put(ctx, row)
		if err != nil {
			log.Println("error inserting to BigQuery:", err)
			continue
		}

		log.Printf("inserted to BigQuery: userID=%s type=%s",
			event.UserID,
			event.EventType,
		)
	}
}
