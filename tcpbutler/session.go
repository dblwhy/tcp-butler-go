package tcpbutler

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"
)

type Response struct {
	Raw []byte
}

type request struct {
	Payload  []byte
	RespCh   chan Response
	ErrCh    chan error
	CorrID   string
	Deadline time.Time
}

type SessionOptions struct {
	Conn           net.Conn
	Decoder        Decoder
	OutboundBuffer int
}

type Decoder interface {
	Decode(net.Conn) (Message, error)
}

type Session struct {
	conn    net.Conn
	decoder Decoder

	// Benefit of having queue
	// - there is always one writer to the TCP socket even receive multiple requests in parallel
	// - can control queue size and return custom error like: ErrBackPressure
	// - can make request async and return fast even network is slow
	outCh chan Message
	done  chan struct{}

	inboundHandler func(*Session, Message)

	logger Logger

	closeOnce sync.Once
}

func NewSession(logger Logger, options SessionOptions, inboundHandler func(*Session, Message)) *Session {
	buffer := options.OutboundBuffer
	if buffer <= 0 {
		buffer = defaultOutboundBuffer
	}

	return &Session{
		logger:         logger,
		conn:           options.Conn,
		decoder:        options.Decoder,
		inboundHandler: inboundHandler,
		outCh:          make(chan Message, buffer),
		done:           make(chan struct{}),
	}
}

func (s *Session) Start(ctx context.Context) error {
	if s.conn == nil || s.decoder == nil {
		return fmt.Errorf("session requires a connection and decoder")
	}

	go s.writeLoop()
	go s.readLoop()

	return nil
}

func (s *Session) writeLoop() {
	defer s.Close()

	for {
		select {
		case <-s.done:
			return
		case msg, ok := <-s.outCh:
			if !ok {
				return
			}

			payload, err := msg.Encode()
			if err != nil {
				s.logger.Warn("failed to encode outbound message", "error", err)
				continue
			}

			// simple write; in production you may need framing, TLS error handling, etc.
			if _, err := s.conn.Write(payload); err != nil {
				s.logger.Error("write failed", "error", err)
				return
			}
		}
	}
}

func (s *Session) readLoop() {
	defer s.Close()

	for {
		msg, err := s.decoder.Decode(s.conn)
		if err != nil {
			s.logger.Warn("read failed", "error", err)
			return
		}
		s.inboundHandler(s, msg)
	}
}

func (s *Session) Close() error {
	var err error

	s.closeOnce.Do(func() {
		close(s.done)
		if s.conn != nil {
			err = s.conn.Close()
			if err != nil {
				s.logger.Info("close error", "error", err)
			} else {
				s.logger.Info("session closed")
			}
		}
	})

	return err
}

func (s *Session) Done() <-chan struct{} {
	return s.done
}
