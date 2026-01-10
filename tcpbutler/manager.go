package tcpbutler

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultOutboundBuffer = 100
	defaultMaxDialBackoff = 30 * time.Second
	defaultRequestTimeout = 5 * time.Second
	acceptRetryDelay      = 100 * time.Millisecond
)

var (
	ErrSendFailed = fmt.Errorf("failed to send message")
	ErrNoSessions = fmt.Errorf("no active sessions available")
)

type ManagerOption func(*managerOptions)

type managerOptions struct {
	outboundBuffer int
	maxDialBackoff time.Duration
	requestTimeout time.Duration
}

func defaultManagerOptions() managerOptions {
	return managerOptions{
		outboundBuffer: defaultOutboundBuffer,
		maxDialBackoff: defaultMaxDialBackoff,
		requestTimeout: defaultRequestTimeout,
	}
}

func WithOutboundBuffer(size int) ManagerOption {
	return func(cfg *managerOptions) {
		if size > 0 {
			cfg.outboundBuffer = size
		}
	}
}

func WithMaxDialBackoff(d time.Duration) ManagerOption {
	return func(cfg *managerOptions) {
		if d > 0 {
			cfg.maxDialBackoff = d
		}
	}
}

func WithRequestTimeout(d time.Duration) ManagerOption {
	return func(cfg *managerOptions) {
		if d > 0 {
			cfg.requestTimeout = d
		}
	}
}

type InboundHandler func(ctx context.Context, message Message) (resp Message, err error)

type manager struct {
	inflight      sync.Map // making inflight global because responses could come back on any open session (rare though)
	inflightCount int64    // for drain / metrics

	sessions  []*Session
	sessionMu sync.RWMutex

	inboundHandler atomic.Value

	listeners   []net.Listener
	listenersMu sync.Mutex

	cancel    context.CancelFunc
	closeOnce sync.Once

	cfg managerOptions

	sessionCursor uint64
}

type DialFunc func(ctx context.Context) (net.Conn, error)

type ClientEndpoint struct {
	Dial        DialFunc
	Decoder     Decoder
	NumSessions int
}

func NewClientManager(
	ctx context.Context,
	endpoints []ClientEndpoint,
	opts ...ManagerOption,
) (*manager, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("at least one endpoint is required")
	}

	clientCtx, cancel := context.WithCancel(ctx)
	m := newManager(opts...)
	m.cancel = cancel

	for idx, ep := range endpoints {
		if ep.Dial == nil {
			return nil, fmt.Errorf("endpoint %d: dial func is required", idx)
		}
		if ep.Decoder == nil {
			return nil, fmt.Errorf("endpoint %d: decoder is required", idx)
		}
		numSessions := ep.NumSessions
		if numSessions <= 0 {
			numSessions = 1
		}
		for i := 0; i < numSessions; i++ {
			go m.dialLoop(clientCtx, ep.Dial, ep.Decoder)
		}
	}

	return m, nil
}

func NewServerManager(ctx context.Context, ln net.Listener, decoder Decoder, opts ...ManagerOption) (*manager, error) {
	if ln == nil {
		return nil, fmt.Errorf("listener is required")
	}
	if decoder == nil {
		return nil, fmt.Errorf("decoder is required")
	}

	if ctx == nil {
		ctx = context.Background()
	}

	serverCtx, cancel := context.WithCancel(ctx)
	m := newManager(opts...)
	m.cancel = cancel
	m.addListener(ln)

	go m.acceptLoop(serverCtx, ln, decoder)

	return m, nil
}

func newManager(opts ...ManagerOption) *manager {
	cfg := defaultManagerOptions()
	for _, opt := range opts {
		opt(&cfg)
	}

	return &manager{
		sessions: []*Session{},
		cfg:      cfg,
	}
}

func (m *manager) SetInboundHandler(h InboundHandler) {
	m.inboundHandler.Store(h)
}

func (m *manager) Sessions() []*Session {
	m.sessionMu.RLock()
	defer m.sessionMu.RUnlock()

	// prevent from caller to mutate the manager's internal slice but still access to the original sessions
	cp := make([]*Session, len(m.sessions))
	copy(cp, m.sessions)
	return cp
}

func (m *manager) Listeners() []net.Listener {
	m.listenersMu.Lock()
	defer m.listenersMu.Unlock()

	cp := make([]net.Listener, len(m.listeners))
	copy(cp, m.listeners)
	return cp
}

func (m *manager) CloseAll() error {
	m.closeOnce.Do(func() {
		if m.cancel != nil {
			m.cancel() // triggers ctx.Done()
		}
	})

	for _, session := range m.Sessions() {
		session.Close()
	}
	m.sessionMu.Lock()
	m.sessions = nil
	m.sessionMu.Unlock()

	for _, listener := range m.Listeners() {
		listener.Close()
	}
	m.listenersMu.Lock()
	m.listeners = nil
	m.listenersMu.Unlock()

	return nil
}

func (m *manager) SendAndWait(ctx context.Context, msg Message) (Message, error) {
	corrID := msg.CorrelationID()
	inCh := make(chan Message, 1)

	// register inflight
	m.inflight.Store(corrID, inCh)
	atomic.AddInt64(&m.inflightCount, 1)

	defer func() {
		// ensure cleanup
		m.inflight.Delete(corrID)
		atomic.AddInt64(&m.inflightCount, -1)
	}()

	// submit message to appropriate session
	session, err := m.nextSession()
	if err != nil {
		return nil, err
	}

	// enqueue outbound
	select {
	case session.outCh <- msg:
		// ok
	case <-session.Done():
		return nil, ErrSendFailed
	case <-ctx.Done():
		return nil, ErrSendFailed
	}

	// wait for response or timeout
	timeout := m.cfg.requestTimeout
	select {
	case inbound := <-inCh:
		return inbound, nil
	case <-time.After(timeout):
		return nil, fmt.Errorf("timeout waiting for response")
	}
}

// SendNoWait sends a message and does NOT wait for a response.
func (m *manager) SendNoWait(ctx context.Context, msg Message) error {
	session, err := m.nextSession()
	if err != nil {
		return err
	}

	select {
	case session.outCh <- msg:
		return nil
	case <-session.Done():
		return ErrSendFailed
	case <-ctx.Done():
		return ErrSendFailed
	}
}

func (m *manager) dialLoop(ctx context.Context, dial DialFunc, decoder Decoder) {
	backoff := time.Second
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		conn, err := dial(ctx)
		if err != nil {
			if !waitWithContext(ctx, backoff) {
				return
			}
			backoff = nextBackoff(backoff, m.cfg.maxDialBackoff)
			continue
		}
		backoff = time.Second

		session := NewSession(SessionOptions{
			Conn:           conn,
			Decoder:        decoder,
			OutboundBuffer: m.cfg.outboundBuffer,
		}, m.onInboundMessage)
		if err := session.Start(ctx); err != nil {
			conn.Close()
			if !waitWithContext(ctx, backoff) {
				return
			}
			backoff = nextBackoff(backoff, m.cfg.maxDialBackoff)
			continue
		}

		m.addSession(session)

		select {
		case <-ctx.Done():
			session.Close()
			m.removeSession(session)
			return
		case <-session.Done():
			m.removeSession(session)
		}
	}
}

func (m *manager) acceptLoop(ctx context.Context, ln net.Listener, decoder Decoder) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) || ctx.Err() != nil {
				return
			}
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			if !waitWithContext(ctx, acceptRetryDelay) {
				return
			}
			continue
		}

		session := NewSession(SessionOptions{
			Conn:           conn,
			Decoder:        decoder,
			OutboundBuffer: m.cfg.outboundBuffer,
		}, m.onInboundMessage)
		if err := session.Start(ctx); err != nil {
			conn.Close()
			continue
		}

		m.addSession(session)
		m.watchSession(session)
	}
}

func (m *manager) onInboundMessage(sess *Session, msg Message) {
	corrID := msg.CorrelationID()

	if chVal, ok := m.inflight.Load(corrID); ok {
		ch := chVal.(chan Message)
		select {
		case ch <- msg:
			// delivered
		default:
			// receiver too slow or gone; you can log and drop
		}
		return
	}

	// no inflight match: this is server initiated request or unexpected response
	m.handleInboundRequest(sess, msg)
}

func (m *manager) handleInboundRequest(sess *Session, req Message) {
	val := m.inboundHandler.Load()
	if val == nil {
		// No handler registered: drop or log.
		return
	}
	handler := val.(InboundHandler)

	// Don't block read loop
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		resp, err := handler(ctx, req)
		if err != nil {
			// log error
			return
		}
		if resp == nil {
			// Fire-and-forget inbound: process only, no response
			return
		}
		sess.outCh <- resp
	}()
}

func (m *manager) addSession(session *Session) {
	m.sessionMu.Lock()
	m.sessions = append(m.sessions, session)
	m.sessionMu.Unlock()
}

func (m *manager) removeSession(session *Session) {
	m.sessionMu.Lock()
	defer m.sessionMu.Unlock()

	for idx, s := range m.sessions {
		if s == session {
			m.sessions = append(m.sessions[:idx], m.sessions[idx+1:]...)
			break
		}
	}
}

func (m *manager) nextSession() (*Session, error) {
	m.sessionMu.RLock()
	defer m.sessionMu.RUnlock()

	count := len(m.sessions)
	if count == 0 {
		return nil, ErrNoSessions
	}

	start := int(atomic.AddUint64(&m.sessionCursor, 1)-1) % count
	for i := 0; i < count; i++ {
		idx := (start + i) % count
		session := m.sessions[idx]
		select {
		case <-session.Done():
			continue
		default:
			return session, nil
		}
	}

	return nil, ErrNoSessions
}

func (m *manager) addListener(ln net.Listener) {
	m.listenersMu.Lock()
	m.listeners = append(m.listeners, ln)
	m.listenersMu.Unlock()
}

func (m *manager) watchSession(session *Session) {
	go func() {
		<-session.Done()
		m.removeSession(session)
	}()
}

// func (e *Endpoint) startReconnectLoop(dial DialFunc) {
//     go func() {
//         for {
//             // if we have fewer than desired sessions
//             if e.sessionCount() < e.desiredSessions {
//                 conn, err := dial(context.Background())
//                 if err != nil {
//                     time.Sleep(backoffDuration)
//                     continue
//                 }
//                 e.addSession(conn)
//             }
//             time.Sleep(time.Second)
//         }
//     }()
// }
