package tcpbutler

import (
	"context"
	"crypto/tls"
	"net"
	"time"
)

func TCPDial(addr string, timeout time.Duration) DialFunc {
	return func(ctx context.Context) (net.Conn, error) {
		d := net.Dialer{Timeout: timeout}
		return d.DialContext(ctx, "tcp", addr)
	}
}

func TLSDial(addr string, tlsCfg *tls.Config, timeout time.Duration) DialFunc {
	return func(ctx context.Context) (net.Conn, error) {
		d := tls.Dialer{
			NetDialer: &net.Dialer{Timeout: timeout},
			Config:    tlsCfg,
		}
		return d.DialContext(ctx, "tcp", addr)
	}
}
