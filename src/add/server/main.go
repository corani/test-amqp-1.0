package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/Azure/go-amqp"
	"github.com/corani/test-amqp-1.0/src/add"
	"github.com/corani/test-amqp-1.0/src/client"
)

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

type Server struct {
	client     *client.Client
	receiver   *client.Receiver
	senderAddr string
	sender     *client.Sender
}

func NewServer(ctx context.Context) (*Server, error) {
	client, err := client.NewClient()
	if err != nil {
		return nil, err
	}

	receiver, err := client.Receiver("/queue/add")
	if err != nil {
		return nil, err
	}

	return &Server{
		client:   client,
		receiver: receiver,
	}, nil
}

func (s *Server) Close(ctx context.Context) {
	if s.sender != nil {
		s.sender.Close(ctx)
	}

	if s.receiver != nil {
		s.receiver.Close(ctx)
	}

	s.client.Close(ctx)
}

func (s *Server) GetSender(ctx context.Context, addr string) *client.Sender {
	if s.senderAddr != addr {
		if s.sender != nil {
			s.sender.Close(ctx)
		}

		sender, err := s.client.Sender(addr)
		if err != nil {
			log.Fatalf("Create Sender: %v", err)
		}

		s.senderAddr = addr
		s.sender = sender
	}

	return s.sender
}

func (s *Server) Handle(ctx context.Context, msg *amqp.Message) {
	var (
		req add.AddRequest
		res add.AddResponse
	)

	if err := req.Decode(msg.GetData()); err != nil {
		log.Fatalf("Decode message: %v", err)
	}

	log.Printf("message reply-to: %v, cid: %v",
		msg.Properties.ReplyTo, msg.Properties.CorrelationID)

	res.Ans = req.A + req.B

	bs, _ := res.Encode()

	sender := s.GetSender(ctx, msg.Properties.ReplyTo)

	err := sender.Send(ctx, &amqp.Message{
		Properties: &amqp.MessageProperties{
			CorrelationID: msg.Properties.CorrelationID,
		},
		Data: [][]byte{bs},
	})
	if err != nil {
		log.Fatalf("Send: %v", err)
	}
}

func main() {
	ctx := handleSigterm(context.Background())

	s, err := NewServer(ctx)
	if err != nil {
		log.Fatalf("New Server: %v", err)
	}

	for {
		msg, err := s.receiver.Receive(ctx)
		if err != nil {
			log.Fatalf("Receive message: %v", err)
		}

		if msg.Properties.ReplyTo == "" || msg.Properties.CorrelationID == "" {
			s.receiver.RejectMessage(ctx, msg, nil)

			continue
		}

		s.receiver.AcceptMessage(ctx, msg)

		go s.Handle(ctx, msg)
	}
}
