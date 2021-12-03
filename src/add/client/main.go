package main

import (
	"context"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/Azure/go-amqp"
	"github.com/google/uuid"
	"github.com/corani/test-amqp-1.0/src/add"
	"github.com/corani/test-amqp-1.0/src/client"
)

type Add struct {
	queue    string
	client   *client.Client
	sender   *client.Sender
	receiver *client.Receiver
	handlers *sync.Map
}

// handleSigterm returns a context that is automatically cancelled when you
// press Ctrl+C in the terminal.
func handleSigterm(ctx context.Context) context.Context {
	ctx, cancel := context.WithCancel(context.Background())

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-c

		cancel()
	}()

	return ctx
}

func (a *Add) send(ctx context.Context, x, y int) error {
	cid := uuid.NewString()

	req := add.AddRequest{
		A: int64(x),
		B: int64(y),
	}

	log.Printf("send %v", cid)

	a.handlers.Store(cid, func(res add.AddResponse) {
		log.Printf("%s: %d + %d = %d", cid, x, y, res.Ans)
	})

	bs, err := req.Encode()
	if err != nil {
		return err
	}

	msg := &amqp.Message{
		Properties: &amqp.MessageProperties{
			CorrelationID: cid,
			ReplyTo:       a.queue,
		},
		Data: [][]byte{bs},
	}

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	return a.sender.Send(ctx, msg)
}

func (a *Add) startReceiver(ctx context.Context) error {
	receiver, err := a.client.Session().NewReceiver(
		amqp.LinkSourceAddress(a.queue),
	)
	if err != nil {
		return err
	}

	go func() {
		var res add.AddResponse

		for {
			msg, err := receiver.Receive(ctx)
			if err != nil {
				log.Printf("Receiver: %v", err)

				return
			}

			cid := msg.Properties.CorrelationID

			handler, ok := a.handlers.LoadAndDelete(cid)
			if ok {
				log.Printf("invoke handler for %v", cid)

				res.Decode(msg.GetData())

				handler.(func(add.AddResponse))(res)
			} else {
				log.Printf("no handler for %v", cid)
			}
		}
	}()

	a.receiver = &client.Receiver{receiver}

	return nil
}

func (a *Add) Close(ctx context.Context) {
	if a.receiver != nil {
		a.receiver.Close(ctx)
	}

	if a.sender != nil {
		a.sender.Close(ctx)
	}

	a.client.Close(ctx)
}

func NewAdder(ctx context.Context) (*Add, error) {
	client, err := client.NewClient()
	if err != nil {
		return nil, err
	}

	sender, err := client.Sender("/queue/add")
	if err != nil {
		return nil, err
	}

	return &Add{
		queue:    "/queue/" + uuid.NewString(),
		client:   client,
		sender:   sender,
		handlers: &sync.Map{},
	}, nil
}

func main() {
	ctx := handleSigterm(context.Background())

	adder, err := NewAdder(ctx)
	if err != nil {
		log.Fatalf("Create Adder: %v", err)
	}
	defer adder.Close(ctx)

	if err := adder.startReceiver(ctx); err != nil {
		log.Fatalf("Start Receiver: %v", err)
	}

	for {
		select {
		case <-time.After(time.Second):
			if err := adder.send(ctx, rand.Intn(100), rand.Intn(100)); err != nil {
				log.Fatalf("Send: %v", err)
			}
		case <-ctx.Done():
			return
		}
	}
}
