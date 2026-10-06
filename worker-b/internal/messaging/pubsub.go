package messaging

import (
	"context"
	"encoding/base64"
	"errors"
	"time"

	"github.com/juanmh10/gemma-microservices/worker-b/internal/analysis"
	"google.golang.org/api/option"
	pubsub "google.golang.org/api/pubsub/v1"
)

type PubSub struct{ Service *pubsub.Service }

func NewPubSub(ctx context.Context) (*PubSub, error) {
	s, err := pubsub.NewService(ctx, option.WithEndpoint("https://us-central1-pubsub.googleapis.com/"), option.WithQuotaProject(analysis.Project))
	if err != nil {
		return nil, err
	}
	return &PubSub{s}, nil
}
func (p *PubSub) Publish(ctx context.Context, data []byte) (string, error) {
	if len(data) > MaxEventBytes {
		return "", ErrInvalid
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	// Generated REST Do makes one request. No batching, automatic retry or
	// compensating publication is enabled after an ambiguous transport response.
	r, err := p.Service.Projects.Topics.Publish(Topic, &pubsub.PublishRequest{Messages: []*pubsub.PubsubMessage{{Data: base64.StdEncoding.EncodeToString(data), Attributes: map[string]string{"schema_version": Version}}}}).Context(ctx).Do()
	if err != nil || len(r.MessageIds) != 1 || !messagePattern.MatchString(r.MessageIds[0]) {
		return "", errors.New("publish response unavailable")
	}
	return r.MessageIds[0], nil
}
