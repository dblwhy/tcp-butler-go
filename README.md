# TCP-Butler

Persistent TCP sessions made simple — reconnect, pooling, request/response, push messages, client + server.

![TCP Butler overview](doc/diagram.png)

## Status

This project is still in **experimental** stage (2026/1/11).

Roadmap
- [ ] Logging interface (in progress)
- [ ] Graceful shutdown
    - how to ensure graceful shutdown for persisted tcp connection? can we prevent a case like we need to force close tcp session (reach graceful timeout) before all the inflight request completes?
- [ ] Add unit tests

## ✨ Features
- Persistent TCP sessions (pooled)
- Automatic reconnect (exponential backoff)
- Round-robin routing across connections
- `SendAndWait` request/response with CorrelationID
- Fire-and-forget messaging
- Server-initiated inbound handling

## 📦 Install

```sh
go get github.com/dblwhy/tcp-butler-go
```

## TCP Server Mode

### Basic

```go
package main

import (
    "context"
	"log"
	"log/slog"

    "github.com/you/tcpbutler"
)

func main() {
    ctx := context.Background()
    decoder := NewMyDecoder()

    handler := func(ctx context.Context, msg tcpbutler.Message) (tcpbutler.Message, error) {
        log.Println("server received:", msg)
        return msg, nil // echo response
    }

    logger := tcpbutler.NewSlogLoggerJSON(slog.LevelDebug)

    if err := tcpbutler.ListenAndServe(ctx, ":9000", decoder, handler, tcpbutler.WithLogger(logger)); err != nil {
        log.Fatal(err)
    }
}
```

### Advanced

Use when you want:
- TLS listener
- PROXY protocol
- socket activation
- cmux shared port
- multiple servers in one process

```go
ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
defer cancel()

ln, _ := net.Listen("tcp", ":9000")

m, err := tcpbutler.NewServerManager(ctx, ln, decoder)
if err != nil { panic(err) }

m.SetInboundHandler(handler)

<-ctx.Done()
m.CloseGracefully()
```

## TCP Client Mode

### Single session
```go
cli, err := tcpbutler.Dial(ctx, "tcp", "localhost:9000", decoder)
if err != nil { log.Fatal(err) }

resp, err := cli.SendAndWait(ctx, request)
```

### Multiple sessions

```go
cli, _ := tcpbutler.DialWithSessions(ctx, "tcp", "localhost:9000", decoder, 4)
```

### With TLS/mTLS

```go
tlsConf := &tls.Config{
    ServerName: "example.com",
    // ...
}

cli, err := tcpbutler.DialTLS(ctx, "tcp", "example.com:443", tlsConf, decoder, 4)
```

### Accept server initiate message

```go
handler := func(ctx context.Context, msg tcpbutler.Message) (tcpbutler.Message, error) {
    log.Println("received:", msg)
    return msg, nil // echo response
}

cli, err := tcpbutler.Dial(ctx, "tcp", "localhost:9000", decoder)
if err != nil { log.Fatal(err) }

cli.SetInboundHandler(handler)

resp, err := cli.SendAndWait(ctx, request)
```

## Configuration options

| Option | Description |
| --- | --- |
| `WithOutboundBuffer(size)` | Size of per-session outbound channel (default 100). |
| `WithMaxDialBackoff(d)` | Maximum reconnect backoff (default 30s). |
| `WithRequestTimeout(d)` | `SendAndWait` timeout before failing (default 5s). |
| `WithLogger(logger)` | Plug in a custom logger (defaults to no-op). |

Pass these options to both `NewClientManager` and `NewServerManager` to tailor behavior per deployment.


## 🧩 FAQ

**Does it reconnect?**

Yes — exponential backoff until success.

**Can servers push events to clients?**

Yes — inbound handler fires for unmatched messages.

**How does correlation work?**

CorrelationID() links outbound requests to inbound responses.

**What if two sessions receive replies?**

Each reply maps via correlation map → delivered automatically.
