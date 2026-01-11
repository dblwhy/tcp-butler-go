package tcpbutler

import (
	"context"
	"fmt"
	"net"
)

// ===== High-level server helpers =====

// ListenAndServe listens on addr and serves TCP-Butler until ctx is canceled.
// This is the easiest entrypoint for server users.
//
// Example:
//
//	handler := func(ctx context.Context, msg tcpbutler.Message) (tcpbutler.Message, error) {
//	    // handle request...
//	    return msg, nil
//	}
//
//	if err := tcpbutler.ListenAndServe(ctx, ":9000", decoder, handler); err != nil {
//	    log.Fatal(err)
//	}
func ListenAndServe(
	ctx context.Context,
	addr string,
	decoder Decoder,
	handler InboundHandler,
	opts ...ManagerOption,
) error {
	if ctx == nil {
		ctx = context.Background()
	}

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	return Serve(ctx, ln, decoder, handler, opts...)
}

// Serve is like ListenAndServe, but takes an existing net.Listener.
// Use this when you need custom listeners (systemd socket activation, TLS
// listeners, PROXY protocol, cmux, custom socket options, etc.).
func Serve(
	ctx context.Context,
	ln net.Listener,
	decoder Decoder,
	handler InboundHandler,
	opts ...ManagerOption,
) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if ln == nil {
		return fmt.Errorf("listener is required")
	}
	if decoder == nil {
		return fmt.Errorf("decoder is required")
	}

	m, err := NewServerManager(ctx, ln, decoder, opts...)
	if err != nil {
		return err
	}
	if handler != nil {
		m.SetInboundHandler(handler)
	}

	// Block until caller cancels.
	<-ctx.Done()

	// Attempt graceful shutdown.
	_ = m.CloseGracefully()
	return ctx.Err()
}
