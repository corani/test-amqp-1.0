package client

import (
	"context"
	"time"

	"github.com/Azure/go-amqp"
)

func NewClient() (*Client, error) {
	c, err := amqp.Dial("amqp://localhost")
	if err != nil {
		return nil, err
	}

	s, err := c.NewSession()
	if err != nil {
		_ = c.Close()

		return nil, err
	}

	return &Client{
		session: s,
		client:  c,
	}, nil
}

type Client struct {
	session *amqp.Session
	client  *amqp.Client
}

func (c *Client) Session() *amqp.Session {
	return c.session
}

// Close closes both the client and the session, with a timeout of 5 seconds.
func (c *Client) Close(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	err1 := c.session.Close(ctx)

	err2 := c.client.Close()

	if err1 != nil {
		return err1
	}

	return err2
}

// Sender returns a new sender for the given queue.
func (c *Client) Sender(queue string) (*Sender, error) {
	s, err := c.session.NewSender(
		amqp.LinkTargetAddress(queue),
		amqp.LinkProperty("name", "sender"),
	)
	if err != nil {
		return nil, err
	}

	return &Sender{s}, nil
}

type Sender struct {
	*amqp.Sender
}

// Close closes the sender with a timeout of 5 seconds.
func (s *Sender) Close(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	return s.Sender.Close(ctx)
}

// Reciever retursn a new receiver for the given queue. Note that messages must
// be explicitly accepted (or rejected / released).
func (c *Client) Receiver(queue string) (*Receiver, error) {
	r, err := c.session.NewReceiver(
		amqp.LinkSourceAddress(queue),
		// WARNING: vendored amqp library is patched to allow disabling the
		// auto-accept for messages.
		amqp.LinkAutoAcceptFirst(false),
		amqp.LinkCredit(10),
		amqp.LinkProperty("name", "receiver"),
	)
	if err != nil {
		return nil, err
	}

	return &Receiver{r}, nil
}

type Receiver struct {
	*amqp.Receiver
}

// Close closes the receiver with a timeout of 5 seconds.
func (s *Receiver) Close(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	return s.Receiver.Close(ctx)
}
