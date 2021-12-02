package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"syscall"

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

type keyboard struct{}

// Keyboard disables input buffering so you can read single characters from
// the terminal.
func Keyboard() *keyboard {
	// disable input buffering
	exec.Command("stty", "-F", "/dev/tty", "cbreak", "min", "1").Run()

	return &keyboard{}
}

// Read reads a single byte from the terminal and returns it as a rune.
func (k *keyboard) Read() rune {
	b := make([]byte, 1)

	os.Stdin.Read(b)

	return rune(b[0])
}

func main() {
	ctx := handleSigterm(context.Background())

	k := Keyboard()

	client, err := client.NewClient()
	if err != nil {
		log.Fatalf("Create Client: %v", err)
	}
	defer client.Close(ctx)

	receiver, err := client.Receiver("/queue/example")
	if err != nil {
		log.Fatalf("Create receiver: %v", err)
	}
	defer receiver.Close(ctx)

	for {
		msg, err := receiver.Receive(ctx)
		if err != nil {
			log.Fatalf("Receive message: %v", err)
		}

		fmt.Printf("Received: data=%s, first=%v\n",
			msg.GetData(), msg.Header.FirstAcquirer)

		for ok := false; !ok; {
			fmt.Print("\n(A) Accept, (X) Reject or (R) Release?\n> ")

			switch k.Read() {
			case 'a', 'A':
				receiver.AcceptMessage(ctx, msg)
				ok = true
			case 'x', 'X':
				receiver.RejectMessage(ctx, msg, nil)
				ok = true
			case 'r', 'R':
				receiver.ReleaseMessage(ctx, msg)
				ok = true
			}
		}

		fmt.Println()
	}
}
