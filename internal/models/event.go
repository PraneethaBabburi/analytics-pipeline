package models

import (
	"errors"
	"time"
)

type AnalyticsEvent struct {
	UserID        string                 `json:"user_id"`
	EventType     string                 `json:"event_type"`
	Timestamp     time.Time              `json:"timestamp"`
	Properties    map[string]interface{} `json:"properties"`
	SchemaVersion string                 `json:"schema_version"`
}

func (e *AnalyticsEvent) Validate() error {
	if e.UserID == "" {
		return errors.New("user_id is required")
	}
	if e.EventType == "" {
		return errors.New("event_type is required")
	}
	return nil
}
