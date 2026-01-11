package tcpbutler

import (
	"context"
	"crypto/tls"
	"net"
)

// ===== High-level client helpers =====

// Dial creates a client manager connected to a single TCP endpoint.
// It dials using net.Dialer and starts 1 session by default.
func Dial(
	ctx context.Context,
	network, addr string,
	decoder Decoder,
	opts ...ManagerOption,
) (*manager, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	dialer := &net.Dialer{}
	ep := ClientEndpoint{
		Dial: func(ctx context.Context) (net.Conn, error) {
			return dialer.DialContext(ctx, network, addr)
		},
		Decoder:     decoder,
		NumSessions: 1,
	}

	return NewClientManager(ctx, []ClientEndpoint{ep}, opts...)
}

// DialWithSessions dials a single endpoint but opens multiple TCP sessions
// to increase throughput.
func DialWithSessions(
	ctx context.Context,
	network, addr string,
	decoder Decoder,
	numSessions int,
	opts ...ManagerOption,
) (*manager, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if numSessions <= 0 {
		numSessions = 1
	}

	dialer := &net.Dialer{}
	ep := ClientEndpoint{
		Dial: func(ctx context.Context) (net.Conn, error) {
			return dialer.DialContext(ctx, network, addr)
		},
		Decoder:     decoder,
		NumSessions: numSessions,
	}

	return NewClientManager(ctx, []ClientEndpoint{ep}, opts...)
}

// DialTLS dials a TCP endpoint with TLS/mTLS using the provided tls.Config.
func DialTLS(
	ctx context.Context,
	network, addr string,
	tlsConfig *tls.Config,
	decoder Decoder,
	numSessions int,
	opts ...ManagerOption,
) (*manager, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if numSessions <= 0 {
		numSessions = 1
	}

	dialer := &tls.Dialer{
		NetDialer: &net.Dialer{},
		Config:    tlsConfig,
	}

	ep := ClientEndpoint{
		Dial: func(ctx context.Context) (net.Conn, error) {
			return dialer.DialContext(ctx, network, addr)
		},
		Decoder:     decoder,
		NumSessions: numSessions,
	}

	return NewClientManager(ctx, []ClientEndpoint{ep}, opts...)
}
