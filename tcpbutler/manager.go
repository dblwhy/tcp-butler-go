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
	outboundBuffer  int
	maxDialBackoff  time.Duration
	requestTimeout  time.Duration
	logger          Logger
	gracefulTimeout time.Duration
}

func defaultManagerOptions() managerOptions {
	return managerOptions{
		outboundBuffer:  defaultOutboundBuffer,
		maxDialBackoff:  defaultMaxDialBackoff,
		requestTimeout:  defaultRequestTimeout,
		logger:          NopLogger{},
		gracefulTimeout: 10 * time.Second,
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

func WithGracefulShutdownTimeout(d time.Duration) ManagerOption {
	return func(cfg *managerOptions) {
		if d >= 0 {
			cfg.gracefulTimeout = d
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

	cfg    managerOptions
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
	m.logger.Info("establishing persistent TCP sessions", "endpoints_count", len(endpoints))

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

	m.logger.Info("starting server manager", "address", ln.Addr().String())
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

// ForceCloseAll immediately closes all listeners and sessions.
func (m *manager) ForceCloseAll() error {
	m.shutdownManagerContext()
	m.closeListeners()
	m.closeSessions()
	return nil
}

// CloseGracefully waits for inflight requests to drain before closing sessions.
func (m *manager) CloseGracefully() error {
	m.shutdownManagerContext()
	m.closeListeners()
	if atomic.LoadInt64(&m.inflightCount) > 0 {
		waitCtx, waitCancel := context.WithTimeout(context.Background(), m.cfg.gracefulTimeout)
		defer waitCancel()

		m.logger.Info("waiting for inflight requests to drain", "inflight_count", atomic.LoadInt64(&m.inflightCount))
		if err := m.waitForInflight(waitCtx); err != nil {
			m.logger.Warn("timeout while waiting for inflight", "err", err,
				"inflight_count", atomic.LoadInt64(&m.inflightCount))
		}
	}
	m.closeSessions()
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
		m.logger.Warn("SendAndWait: no session available for the message", "corrID", corrID)
		return nil, err
	}

	// enqueue outbound
	select {
	case session.outCh <- msg:
		// ok
	case <-session.Done():
		m.logger.Warn("SendAndWait: session closed before send", "corrID", corrID)
		return nil, ErrSendFailed
	case <-ctx.Done():
		m.logger.Warn("SendAndWait: context done before send", "corrID", corrID)
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
		m.logger.Warn("SendAndWait: timeout waiting", "corrID", corrID)
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
		m.logger.Warn("SendNoWait: no session available", "corrID", msg.CorrelationID())
		return err
	}

	select {
	case session.outCh <- msg:
		return nil
	case <-session.Done():
		m.logger.Warn("SendNoWait: session closed before send", "corrID", msg.CorrelationID())
		return ErrSendFailed
	case <-ctx.Done():
		m.logger.Warn("SendNoWait: context done before send", "corrID", msg.CorrelationID())
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
			m.logger.Warn("dial failed", "error", err)
			if !waitWithContext(ctx, backoff) {
				return
			}
			backoff = nextBackoff(backoff, m.cfg.maxDialBackoff)
			continue
		}
		backoff = time.Second
		m.logger.Info("dial succeeded", "address", conn.RemoteAddr())

		session := NewSession(SessionOptions{
			Conn:           conn,
			Decoder:        decoder,
			OutboundBuffer: m.cfg.outboundBuffer,
		}, m.onInboundMessage)
		session.log = m.logger
		session.log = m.logger
		if err := session.Start(ctx); err != nil {
			conn.Close()
			m.logger.Warn("failed to start session", "error", err)
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
			m.logger.Warn("accept failed", "error", err)
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
			m.logger.Warn("failed to start accepted session", "error", err)
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
			m.logger.Warn("inflight channel full, dropping message", "corrID", corrID)
		}
		return
	}

	// no inflight match: this is server-initiated request or unexpected response
	m.handleInboundRequest(sess, msg)
}

func (m *manager) handleInboundRequest(sess *Session, req Message) {
	val := m.inboundHandler.Load()
	if val == nil {
		m.logger.Warn("dropping inbound message with no handler", "corrID", req.CorrelationID())
		return
	}
	handler := val.(InboundHandler)

	// Don't block read loop
	go func(timeout time.Duration) {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		resp, err := handler(ctx, req)
		if err != nil {
			m.logger.Error("inbound handler error", "error", err)
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
			m.logger.Warn("session closed before responding to inbound", "corrID", req.CorrelationID())
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
		m.logger.Info("session closed", "session", session)

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

func (m *manager) shutdownManagerContext() {
	m.closeOnce.Do(func() {
		if m.cancel != nil {
			m.cancel()
		}
	})
}

func (m *manager) closeSessions() {
	sessions := m.Sessions()
	if len(sessions) == 0 {
		return
	}
	m.logger.Info("closing all sessions", "sessions_count", len(sessions))

	for _, session := range sessions {
		session.Close()
	}
	m.sessionMu.Lock()
	m.sessions = nil
	m.sessionMu.Unlock()
}

// closeListeners closes all listeners. This only blocks establishing new tcp sessions and will not close existing sessions.
func (m *manager) closeListeners() {
	listeners := m.Listeners()
	if len(listeners) == 0 {
		return
	}
	m.logger.Info("closing all listeners", "listeners_count", len(listeners))

	for _, ln := range listeners {
		ln.Close()
	}
	m.listenersMu.Lock()
	m.listeners = nil
	m.listenersMu.Unlock()
}

func (m *manager) waitForInflight(ctx context.Context) error {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		if atomic.LoadInt64(&m.inflightCount) == 0 {
			return nil
		}
		select {
		case <-ctx.Done(): // timeout
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
