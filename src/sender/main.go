package main

import (
	"context"
	"log"
	"os"
	"strings"
	"time"

	"github.com/Azure/go-amqp"
	"github.com/corani/test-amqp-1.0/src/client"
)

func main() {
	ctx := context.Background()

	client, err := client.NewClient()
	if err != nil {
		log.Fatalf("Create Client: %v", err)
	}
	defer client.Close(ctx)

	sender, err := client.Sender("/queue/example")
	if err != nil {
		log.Fatalf("Create Sender: %v", err)
	}
	defer sender.Close(ctx)

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	// default message
	msg := "Hello, world"

	// concatenate all commandline arguments into the message
	if len(os.Args) > 1 {
		msg = strings.Join(os.Args[1:], " ")
	}

	err = sender.Send(ctx, amqp.NewMessage([]byte(msg)))
	if err != nil {
		log.Fatalf("Send: %v", err)
	}
}
