# Test project for AMQP-1.0

## Setup

### Start broker
This project uses RabbitMQ with AMQP-1.0 plugin. To get this up and running:

```bash
./docker_build.sh
./docker_run.sh
```

## Usage

### Receiver
To start the receiver:

```bash
go run src/receiver/main.go
```

### Sender
To start the sender:

```bash
go run src/sender/main.go [your message here]
```

## Notes
The `azure/go-amqp` library accepts all messages automatically for mode first. I tried switching
to mode second, but this wasn't accepted by RabbitMQ. I'm sure there's a way to get this working,
but for now I've patched the vendored library, adding a link option `AutoAcceptFirst` that can
be set to false.
