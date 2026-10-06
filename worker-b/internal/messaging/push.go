package messaging

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"github.com/juanmh10/gemma-microservices/worker-b/internal/telemetry"
	"io"
	"log"
	"net/http"
	"regexp"
	"time"

	"github.com/juanmh10/gemma-microservices/worker-b/internal/analysis"
)

type Envelope struct {
	Message struct {
		Data        string            `json:"data"`
		MessageID   string            `json:"messageId"`
		PublishTime string            `json:"publishTime"`
		Attributes  map[string]string `json:"attributes,omitempty"`
		OrderingKey string            `json:"orderingKey,omitempty"`
	} `json:"message"`
	Subscription    string `json:"subscription"`
	DeliveryAttempt int    `json:"deliveryAttempt,omitempty"`
}
type Rejection struct {
	SchemaVersion string `json:"schema_version"`
	MessageID     string `json:"message_id"`
	PayloadSHA256 string `json:"payload_sha256"`
	Status        string `json:"status"`
	Reason        string `json:"reason"`
}

// Push relies on the mandatory Cloud Run IAM invoker check.
type Push struct {
	Engine *Engine
}

var messagePattern = regexp.MustCompile(`^[0-9]{1,64}$`)

func (p *Push) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if r.Method != "POST" || r.URL.Path != "/events" {
		http.Error(w, "not found", 404)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 16<<10))
	var envelope Envelope
	if err != nil || json.Unmarshal(raw, &envelope) != nil || envelope.Subscription != Subscription || !messagePattern.MatchString(envelope.Message.MessageID) {
		http.Error(w, "invalid push envelope", 400)
		return
	}
	data, decodeErr := base64.StdEncoding.DecodeString(envelope.Message.Data)
	var e Event
	if decodeErr != nil || len(data) > MaxEventBytes || analysis.Decode(data, &e) != nil || e.Validate() != nil {
		err = ErrInvalid
	} else {
		ctx = telemetry.Published(ctx, envelope.Message.PublishTime)
		err = p.Engine.Deliver(ctx, e)
	}
	if errors.Is(err, ErrInvalid) {
		receipt := Rejection{Version, envelope.Message.MessageID, analysis.Digest(raw), "rejected", "invalid_event"}
		// Transport deliveryAttempt changes on redelivery; hash only the message,
		// keeping rejection identity stable across retry attempts.
		receipt.PayloadSHA256 = analysis.Digest(analysis.JSON(envelope.Message))
		name := p.Engine.NotificationPrefix + "/rejected/" + envelope.Message.MessageID + ".json"
		if Immutable(ctx, p.Engine.Store, name, receipt) != nil {
			http.Error(w, "receipt unavailable", 503)
			return
		}
		log.Printf("stage=notification status=rejected attempt=%d", envelope.DeliveryAttempt)
		w.WriteHeader(204)
		return
	}
	if errors.Is(err, ErrEmailPermanent) {
		log.Printf("stage=email status=rejected attempt=%d", envelope.DeliveryAttempt)
		w.WriteHeader(204)
		return
	}
	if err != nil {
		log.Printf("stage=notification status=retryable_error attempt=%d", envelope.DeliveryAttempt)
		http.Error(w, "temporarily unavailable", 503)
		return
	}
	log.Printf("analysis_id=%s stage=notification status=processed event_id=%s attempt=%d", e.AnalysisID, e.EventID, envelope.DeliveryAttempt)
	w.WriteHeader(204)
}
