package nsqpubsub

import (
	"context"
	"fmt"
	"net/url"

	"github.com/nsqio/go-nsq"
	"gocloud.dev/gcerrors"
	"gocloud.dev/pubsub"
	"gocloud.dev/pubsub/batcher"
	"gocloud.dev/pubsub/driver"
)

var (
	_ pubsub.TopicURLOpener = (*URLOpener)(nil)
	_ driver.Topic          = (*topic)(nil)
)

func (o *URLOpener) OpenTopicURL(ctx context.Context, u *url.URL) (*pubsub.Topic, error) {
	pub, err := o.openTopicURL(ctx, u)
	if err != nil {
		return nil, err
	}
	return pubsub.NewTopic(pub, &batcher.Options{}), nil
}

func (o *URLOpener) openTopicURL(ctx context.Context, u *url.URL) (*topic, error) {
	cfg, err := parseURL(u)
	if err != nil {
		return nil, err
	}
	if cfg.NSQD == "" {
		return nil, fmt.Errorf("NSQD address not specified in URL")
	}
	producer, err := nsq.NewProducer(cfg.NSQD, cfg.Config)
	if err != nil {
		return nil, err
	}
	res := &topic{
		producer: producer,
		topic: cfg.Topic,
	}
	return res, nil
}

type topic struct {
	producer *nsq.Producer
	topic string
}

// SendBatch should publish all the messages in ms. It should
// return only after all the messages are sent, an error occurs, or the
// context is done.
//
// Only the Body and (optionally) Metadata fields of the Messages in ms
// will be set by the caller of SendBatch.
//
// If any message in the batch fails to send, SendBatch should return an
// error.
//
// If there is a transient failure, this method should not retry but
// should return an error for which IsRetryable returns true. The
// concrete API takes care of retry logic.
//
// The slice ms should not be retained past the end of the call to
// SendBatch.
//
// SendBatch may be called concurrently from multiple goroutines.
//
// Drivers can control the number of messages sent in a single batch
// and the concurrency of calls to SendBatch via a batcher.Options
// passed to pubsub.NewTopic.
func (t *topic) SendBatch(ctx context.Context, ms []*driver.Message) error {
	for _, msg := range ms {
		data, err := packEnvelope(envelope{
			Header: msg.Metadata,
			Body: msg.Body,
		})
		if err != nil {
			return err
		}
		if err := t.producer.Publish(t.topic, data); err != nil {
			return err
		}
	}
	return nil
}

// IsRetryable should report whether err can be retried.
// err will always be a non-nil error returned from SendBatch.
func (t *topic) IsRetryable(err error) bool {
	return false
}

// As allows drivers to expose driver-specific types.
// See https://gocloud.dev/concepts/as/ for background information.
func (t *topic) As(i interface{}) bool {
	c, ok := i.(**nsq.Producer)
	if !ok {
		return false
	}
	*c = t.producer
	return true
}

// ErrorAs allows drivers to expose driver-specific types for errors.
// See https://gocloud.dev/concepts/as/ for background information.
func (t *topic) ErrorAs(_ error, _ interface{}) bool {
	return false
}

// ErrorCode should return a code that describes the error, which was returned by
// one of the other methods in this interface.
func (t *topic) ErrorCode(err error) gcerrors.ErrorCode {
	if err == nil {
		return gcerrors.OK
	}
	return gcerrors.Unknown
}

// Close cleans up any resources used by the Topic. Once Close is called,
// there will be no method calls to the Topic other than As, ErrorAs, and
// ErrorCode.
func (t *topic) Close() error {
	t.producer.Stop()
	return nil
}
