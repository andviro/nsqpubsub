package nsqpubsub

import (
	"context"
	"fmt"
	"net/url"
	"sync"
	"time"

	"github.com/nsqio/go-nsq"
	"gocloud.dev/gcerrors"
	"gocloud.dev/pubsub"
	"gocloud.dev/pubsub/batcher"
	"gocloud.dev/pubsub/driver"
)

var _ pubsub.SubscriptionURLOpener = (*URLOpener)(nil)

type subscription struct {
	consumer *nsq.Consumer
	msgs     map[string]*nsq.Message
	incoming chan *nsq.Message
	mu       sync.Mutex
}

// ReceiveBatch should return a batch of messages that have queued up
// for the subscription on the server, up to maxMessages.
//
// If there is a transient failure, this method should not retry but
// should return a nil slice and an error. The concrete API will take
// care of retry logic.
//
// If no messages are currently available, this method should block for
// no more than about 1 second. It can return an empty
// slice of messages and no error. ReceiveBatch will be called again
// immediately, so implementations should try to wait for messages for some
// non-zero amount of time before returning zero messages. If the underlying
// service doesn't support waiting, then a time.Sleep can be used.
//
// ReceiveBatch may be called concurrently from multiple goroutines.
//
// Drivers can control the maximum value of maxMessages and the concurrency
// of calls to ReceiveBatch via a batcher.Options passed to
// pubsub.NewSubscription.
func (s *subscription) ReceiveBatch(ctx context.Context, maxMessages int) ([]*driver.Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.consumer.ChangeMaxInFlight(maxMessages)
	defer s.consumer.ChangeMaxInFlight(0)
	var res []*driver.Message
mainloop:
	for i := 0; i < maxMessages; i++ {
		select {
		case msg := <-s.incoming:
			env, err := unpackEnvelope(msg.Body)
			if err != nil {
				return res, err
			}
			id := fmt.Sprintf("%s", msg.ID)
			res = append(res, &driver.Message{
				LoggableID: id,
				AckID:      id,
				Body:       env.Body,
				Metadata:   env.Header,
			})
			s.msgs[id] = msg
		case <-ctx.Done():
			return res, ctx.Err()
		case <-time.After(time.Second):
			break mainloop
		}
	}
	return res, nil
}

// SendAcks should acknowledge the messages with the given ackIDs on
// the server so that they will not be received again for this
// subscription if the server gets the acks before their deadlines.
// This method should return only after all the ackIDs are sent, an
// error occurs, or the context is done.
//
// It is acceptable for SendAcks to be a no-op for drivers that don't
// support message acknowledgement.
//
// Drivers should suppress errors caused by double-acking a message.
//
// SendAcks may be called concurrently from multiple goroutines.
//
// Drivers can control the maximum size of ackIDs and the concurrency
// of calls to SendAcks/SendNacks via a batcher.Options passed to
// pubsub.NewSubscription.
func (s *subscription) SendAcks(ctx context.Context, ackIDs []driver.AckID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, id := range ackIDs {
		ackID, ok := id.(string)
		if !ok {
			return fmt.Errorf("unsupported ID type: %T", id)
		}
		msg, ok := s.msgs[ackID]
		if !ok {
			continue
		}
		msg.Finish()
		delete(s.msgs, ackID)
	}
	return nil
}

// CanNack must return true iff the driver supports Nacking messages.
//
// If CanNack returns false, SendNacks will never be called, and Nack will
// panic if called.
func (s *subscription) CanNack() bool {
	return true
}

// SendNacks should notify the server that the messages with the given ackIDs
// are not being processed by this client, so that they will be received
// again later, potentially by another subscription.
// This method should return only after all the ackIDs are sent, an
// error occurs, or the context is done.
//
// If the service does not suppport nacking of messages, return false from
// CanNack, and SendNacks will never be called.
//
// SendNacks may be called concurrently from multiple goroutines.
//
// Drivers can control the maximum size of ackIDs and the concurrency
// of calls to SendAcks/Nacks via a batcher.Options passed to
// pubsub.NewSubscription.
func (s *subscription) SendNacks(ctx context.Context, ackIDs []driver.AckID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, id := range ackIDs {
		ackID, ok := id.(string)
		if !ok {
			return fmt.Errorf("unsupported ID type: %T", id)
		}
		msg, ok := s.msgs[ackID]
		if !ok {
			continue
		}
		msg.RequeueWithoutBackoff(time.Millisecond)
		delete(s.msgs, ackID)
	}
	return nil
}

// IsRetryable should report whether err can be retried.
// err will always be a non-nil error returned from ReceiveBatch or SendAcks.
func (s *subscription) IsRetryable(err error) bool {
	return false
}

// As converts i to driver-specific types.
// See https://gocloud.dev/concepts/as/ for background information.
func (s *subscription) As(i interface{}) bool {
	c, ok := i.(**nsq.Consumer)
	if !ok {
		return false
	}
	*c = s.consumer
	return true
}

// ErrorAs allows drivers to expose driver-specific types for errors.
// See https://gocloud.dev/concepts/as/ for background information.
func (s *subscription) ErrorAs(_ error, _ interface{}) bool {
	return false
}

// ErrorCode should return a code that describes the error, which was returned by
// one of the other methods in this interface.
func (s *subscription) ErrorCode(err error) gcerrors.ErrorCode {
	if err == nil {
		return gcerrors.OK
	}
	return gcerrors.Unknown
}

// Close cleans up any resources used by the Topic. Once Close is called,
// there will be no method calls to the Topic other than As, ErrorAs, and
// ErrorCode.
func (s *subscription) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.consumer.Stop()
	return nil
}

func (o *URLOpener) openSubscriptionURL(ctx context.Context, u *url.URL) (_ *subscription, rErr error) {
	cfg, err := parseURL(u)
	if err != nil {
		return nil, err
	}
	if cfg.Channel == "" {
		cfg.Channel = "main"
	}
	consumer, err := nsq.NewConsumer(cfg.Topic, cfg.Channel, cfg.Config)
	if err != nil {
		return nil, fmt.Errorf("creating NSQ consumer: %+v", err)
	}
	consumer.ChangeMaxInFlight(0)
	if err := consumer.ConnectToNSQLookupds(cfg.NSQLookupDs); err != nil {
		return nil, fmt.Errorf("connecting to NSQLookupDs: %+v", err)
	}
	res := &subscription{
		consumer: consumer,
		msgs:     make(map[string]*nsq.Message),
		incoming: make(chan *nsq.Message),
	}
	consumer.AddHandler(nsq.HandlerFunc(func(message *nsq.Message) error {
		message.DisableAutoResponse()
		res.incoming <- message
		return nil
	}))
	return res, nil
}

func (o *URLOpener) OpenSubscriptionURL(ctx context.Context, u *url.URL) (*pubsub.Subscription, error) {
	sub, err := o.openSubscriptionURL(ctx, u)
	if err != nil {
		return nil, err
	}
	return pubsub.NewSubscription(sub, &batcher.Options{}, nil), nil
}
