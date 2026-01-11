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

// ===== Manager options =====

type ManagerOption func(*managerOptions)

type managerOptions struct {
	outboundBuffer int
	maxDialBackoff time.Duration
	requestTimeout time.Duration
	logger         Logger
}

func defaultManagerOptions() managerOptions {
	return managerOptions{
		outboundBuffer: defaultOutboundBuffer,
		maxDialBackoff: defaultMaxDialBackoff,
		requestTimeout: defaultRequestTimeout,
		logger:         NopLogger{},
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

func WithLogger(logger Logger) ManagerOption {
	return func(cfg *managerOptions) {
		if logger != nil {
			cfg.logger = logger
		}
	}
}

// ===== Types =====

type manager struct {
	// inflight responses are keyed by correlation ID so any session can deliver
	inflight      sync.Map // map[CorrelationID]chan Message
	inflightCount int64    // for drain / metrics

	sessions  []*Session
	sessionMu sync.RWMutex

	inboundHandler atomic.Value // stores InboundHandler

	listeners   []net.Listener
	listenersMu sync.Mutex

	cancel    context.CancelFunc
	closeOnce sync.Once

	cfg managerOptions

	logger Logger

	sessionCursor uint64
}

type DialFunc func(ctx context.Context) (net.Conn, error)

type ClientEndpoint struct {
	Dial        DialFunc
	Decoder     Decoder
	NumSessions int
}

type reconnectConfig struct {
	ctx     context.Context
	dial    DialFunc
	decoder Decoder
}

// ===== Constructors (fine-grained control) =====

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
	m.logger.Info("starting client manager with %d endpoints", len(endpoints))

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
		m.logger.Info("endpoint %d configured with %d session(s)", idx, numSessions)
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

	m.logger.Info("starting server manager on %s", ln.Addr())
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
		logger:   cfg.logger,
	}
}

// ===== Public manager API =====

func (m *manager) SetInboundHandler(h InboundHandler) {
	m.inboundHandler.Store(h)
}

func (m *manager) Sessions() []*Session {
	m.sessionMu.RLock()
	defer m.sessionMu.RUnlock()

	// return a copy so caller can't mutate internal slice
	cp := make([]*Session, len(m.sessions))
	copy(cp, m.sessions)
	return cp
}

func (m *manager) Listeners() []net.Listener {
	m.listenersMu.Lock()
	defer m.listenersMu.Unlock()

	// return a copy so caller can't mutate internal slice
	cp := make([]net.Listener, len(m.listeners))
	copy(cp, m.listeners)
	return cp
}

func (m *manager) CloseAll() error {
	m.closeOnce.Do(func() {
		if m.cancel != nil {
			m.cancel() // triggers ctx.Done() in acceptLoop / dialLoop contexts
		}
	})

	m.logger.Info("closing all sessions (%d) and listeners (%d)", len(m.Sessions()), len(m.Listeners()))
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

// SendAndWait sends a message and waits for the matching response by CorrelationID.
func (m *manager) SendAndWait(ctx context.Context, msg Message) (Message, error) {
	if ctx == nil {
		ctx = context.Background()
	}

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
		m.logger.Warn("SendAndWait: no session available for corrID=%s", corrID)
		return nil, err
	}

	// enqueue outbound
	select {
	case session.outCh <- msg:
		// ok
	case <-session.Done():
		m.logger.Warn("SendAndWait: session closed before send corrID=%s", corrID)
		return nil, ErrSendFailed
	case <-ctx.Done():
		m.logger.Warn("SendAndWait: context done before send corrID=%s", corrID)
		return nil, ctx.Err()
	}

	// wait for response, ctx cancel, or timeout
	timeout := m.cfg.requestTimeout
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case inbound := <-inCh:
		return inbound, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
		m.logger.Warn("SendAndWait: timeout waiting for corrID=%s", corrID)
		return nil, fmt.Errorf("timeout waiting for response")
	}
}

// SendNoWait sends a message and does NOT wait for a response.
func (m *manager) SendNoWait(ctx context.Context, msg Message) error {
	if ctx == nil {
		ctx = context.Background()
	}

	session, err := m.nextSession()
	if err != nil {
		m.logger.Warn("SendNoWait: no session available for corrID=%s", msg.CorrelationID())
		return err
	}

	select {
	case session.outCh <- msg:
		return nil
	case <-session.Done():
		m.logger.Warn("SendNoWait: session closed before send corrID=%s", msg.CorrelationID())
		return ErrSendFailed
	case <-ctx.Done():
		m.logger.Warn("SendNoWait: context done before send corrID=%s", msg.CorrelationID())
		return ctx.Err()
	}
}

// ===== Internal loops =====

func (m *manager) dialLoop(ctx context.Context, dial DialFunc, decoder Decoder) {
	backoff := time.Second
	for {
		select {
		case <-ctx.Done():
			m.logger.Info("dial loop exiting due to context cancellation")
			return
		default:
		}

		conn, err := dial(ctx)
		if err != nil {
			m.logger.Warn("dial failed: %v", err)
			if !waitWithContext(ctx, backoff) {
				return
			}
			backoff = nextBackoff(backoff, m.cfg.maxDialBackoff)
			continue
		}
		backoff = time.Second
		m.logger.Info("dial succeeded to %s", conn.RemoteAddr())

		session := NewSession(SessionOptions{
			Conn:           conn,
			Decoder:        decoder,
			OutboundBuffer: m.cfg.outboundBuffer,
		}, m.onInboundMessage)
		session.log = m.logger
		if err := session.Start(ctx); err != nil {
			conn.Close()
			m.logger.Warn("failed to start session: %v", err)
			if !waitWithContext(ctx, backoff) {
				return
			}
			backoff = nextBackoff(backoff, m.cfg.maxDialBackoff)
			continue
		}

		m.addSession(session)
		m.watchSession(session, &reconnectConfig{
			ctx:     ctx,
			dial:    dial,
			decoder: decoder,
		})
		return
	}
}

func (m *manager) acceptLoop(ctx context.Context, ln net.Listener, decoder Decoder) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) || ctx.Err() != nil {
				m.logger.Info("accept loop exiting: %v", err)
				return
			}
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			m.logger.Warn("accept failed: %v", err)
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
			m.logger.Warn("failed to start accepted session: %v", err)
			continue
		}

		m.addSession(session)
		m.watchSession(session, nil)
	}
}

// ===== Inbound handling =====

func (m *manager) onInboundMessage(sess *Session, msg Message) {
	corrID := msg.CorrelationID()

	if chVal, ok := m.inflight.Load(corrID); ok {
		ch := chVal.(chan Message)
		select {
		case ch <- msg:
			// delivered
		default:
			// receiver too slow or gone; you can log and drop
			m.logger.Warn("inflight channel full for corrID=%s, dropping", corrID)
		}
		return
	}

	// no inflight match: this is server-initiated request or unexpected response
	m.handleInboundRequest(sess, msg)
}

func (m *manager) handleInboundRequest(sess *Session, req Message) {
	val := m.inboundHandler.Load()
	if val == nil {
		m.logger.Warn("dropping inbound message with no handler corrID=%s", req.CorrelationID())
		return
	}
	handler := val.(InboundHandler)

	// Don't block read loop
	go func(timeout time.Duration) {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		resp, err := handler(ctx, req)
		if err != nil {
			m.logger.Error("inbound handler error: %v", err)
			return
		}
		if resp == nil {
			// Fire-and-forget inbound: process only, no response
			return
		}

		select {
		case sess.outCh <- resp:
			// ok
		case <-sess.Done():
			// session closed before we could respond
			m.logger.Warn("session closed before responding to inbound corrID=%s", req.CorrelationID())
		}
	}(m.cfg.requestTimeout)
}

// ===== Session management =====

func (m *manager) addSession(session *Session) {
	m.sessionMu.Lock()
	m.sessions = append(m.sessions, session)
	m.sessionMu.Unlock()
	m.logger.Info("session added: %p (total=%d)", session, len(m.sessions))
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

func (m *manager) watchSession(session *Session, reconnect *reconnectConfig) {
	go func() {
		<-session.Done()
		m.removeSession(session)
		m.logger.Info("session closed: %p", session)

		if reconnect == nil {
			return
		}
		if reconnect.ctx == nil || reconnect.ctx.Err() != nil {
			return
		}
		m.logger.Info("attempting reconnect for session slot")
		go m.dialLoop(reconnect.ctx, reconnect.dial, reconnect.decoder)
	}()
}
