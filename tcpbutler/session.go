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

	outCh chan Message
	done  chan struct{}

	inboundHandler func(*Session, Message)

	log Logger

	closeOnce sync.Once
}

func NewSession(options SessionOptions, inboundHandler func(*Session, Message)) *Session {
	buffer := options.OutboundBuffer
	if buffer <= 0 {
		buffer = defaultOutboundBuffer
	}

	return &Session{
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
				// log, maybe notify mgr
				continue
			}

			// simple write; in production you may need framing, TLS error handling, etc.
			if _, err := s.conn.Write(payload); err != nil {
				// log + maybe tell manager this session is broken
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
			// log + notify manager session is dead if needed
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
		}
	})

	return err
}

func (s *Session) Done() <-chan struct{} {
	return s.done
}
