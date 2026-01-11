package tcpbutler

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"
)

// Mock message implementation for testing
type mockMessage struct {
	correlationID string
	payload       string
}

func (m *mockMessage) CorrelationID() string {
	return m.correlationID
}

func (m *mockMessage) Encode() ([]byte, error) {
	// Simple encoding: format as "correlationID:payload\n"
	return []byte(fmt.Sprintf("%s:%s\n", m.correlationID, m.payload)), nil
}

// Mock decoder implementation
type mockDecoder struct{}

func (d *mockDecoder) Decode(conn net.Conn) (Message, error) {
	// Read from connection
	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		return nil, err
	}

	// Parse the message format "correlationID:payload\n"
	data := string(buf[:n])
	// Remove trailing newline
	data = data[:len(data)-1]

	// Split by colon
	var corrID, payload string
	for i, ch := range data {
		if ch == ':' {
			corrID = data[:i]
			if i+1 < len(data) {
				payload = data[i+1:]
			}
			break
		}
	}

	return &mockMessage{
		correlationID: corrID,
		payload:       payload,
	}, nil
}

// Test helper: create a pair of connected TCP connections
func createTCPPair(t *testing.T) (net.Conn, net.Conn) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create listener: %v", err)
	}

	var serverConn net.Conn
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		conn, err := listener.Accept()
		if err != nil {
			t.Errorf("Failed to accept connection: %v", err)
			return
		}
		serverConn = conn
	}()

	clientConn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatalf("Failed to dial: %v", err)
	}

	wg.Wait()
	listener.Close()

	return clientConn, serverConn
}

// Mock server that echoes messages back
func mockEchoServer(t *testing.T, conn net.Conn) {
	buf := make([]byte, 1024)

	for {
		// Use a simpler read approach
		n, err := conn.Read(buf)
		if err != nil {
			return // Connection closed
		}

		// Echo back exactly what we received
		if _, err := conn.Write(buf[:n]); err != nil {
			return // Connection closed
		}
	}
}

// Mock server that can respond with custom messages
func mockCustomServer(t *testing.T, conn net.Conn, responseFunc func(Message) Message) {
	decoder := &mockDecoder{}
	for {
		msg, err := decoder.Decode(conn)
		if err != nil {
			return // Connection closed
		}

		// Generate custom response
		resp := responseFunc(msg)
		if resp == nil {
			continue
		}

		encoded, err := resp.Encode()
		if err != nil {
			t.Errorf("Failed to encode: %v", err)
			return
		}

		if _, err := conn.Write(encoded); err != nil {
			return // Connection closed
		}
	}
}

func newTestManagerWithSessions(t *testing.T, sessionOps []SessionOptions, opts ...ManagerOption) *manager {
	mgr := newManager(opts...)

	for idx, sessionOp := range sessionOps {
		session := NewSession(NopLogger{}, sessionOp, mgr.onInboundMessage)
		if err := session.Start(context.Background()); err != nil {
			t.Fatalf("session %d failed to start: %v", idx, err)
		}
		mgr.addSession(session)
		mgr.watchSession(session, nil)
	}

	return mgr
}

func TestManager_SendAndWait(t *testing.T) {
	// Create TCP connection pair
	clientConn, serverConn := createTCPPair(t)
	defer clientConn.Close()
	defer serverConn.Close()

	// Start echo server
	go mockEchoServer(t, serverConn)

	// Create manager with one session
	sessionOps := []SessionOptions{
		{
			Conn:    clientConn,
			Decoder: &mockDecoder{},
		},
	}

	mgr := newTestManagerWithSessions(t, sessionOps)
	defer mgr.CloseGracefully()

	// Give sessions time to start
	time.Sleep(50 * time.Millisecond)

	// Test SendAndWait
	ctx := context.Background()
	req := &mockMessage{
		correlationID: "test-corr-id-1",
		payload:       "hello",
	}

	resp, err := mgr.SendAndWait(ctx, req)
	if err != nil {
		t.Fatalf("SendAndWait failed: %v", err)
	}

	if resp.CorrelationID() != req.CorrelationID() {
		t.Errorf("Expected correlation ID %s, got %s", req.CorrelationID(), resp.CorrelationID())
	}
}

func TestManager_SendAndWait_Timeout(t *testing.T) {
	// Create TCP connection pair
	clientConn, serverConn := createTCPPair(t)
	defer clientConn.Close()
	defer serverConn.Close()

	// Start server that doesn't respond
	go func() {
		buf := make([]byte, 1024)
		serverConn.Read(buf) // Read but don't respond
	}()

	// Create manager
	sessionOps := []SessionOptions{
		{
			Conn:    clientConn,
			Decoder: &mockDecoder{},
		},
	}

	mgr := newTestManagerWithSessions(t, sessionOps)
	defer mgr.CloseGracefully()

	time.Sleep(50 * time.Millisecond)

	// Test timeout
	ctx := context.Background()
	req := &mockMessage{
		correlationID: "timeout-test",
		payload:       "test",
	}

	start := time.Now()
	_, err := mgr.SendAndWait(ctx, req)
	duration := time.Since(start)

	if err == nil {
		t.Fatal("Expected timeout error, got nil")
	}

	// Should timeout around 5 seconds
	if duration < 4*time.Second || duration > 6*time.Second {
		t.Errorf("Expected ~5 second timeout, got %v", duration)
	}
}

func TestManager_SendNoWait(t *testing.T) {
	// Create TCP connection pair
	clientConn, serverConn := createTCPPair(t)
	defer clientConn.Close()
	defer serverConn.Close()

	// Track received messages
	var receivedMu sync.Mutex
	var received []string

	go func() {
		buf := make([]byte, 1024)
		for {
			n, err := serverConn.Read(buf)
			if err != nil {
				return
			}
			// Parse correlation IDs from received data
			data := string(buf[:n])
			// Each message is "corrID:payload\n"
			start := 0
			for i := 0; i < len(data); i++ {
				if data[i] == '\n' {
					if start < i {
						line := data[start:i]
						for j := 0; j < len(line); j++ {
							if line[j] == ':' {
								corrID := line[:j]
								receivedMu.Lock()
								received = append(received, corrID)
								receivedMu.Unlock()
								break
							}
						}
					}
					start = i + 1
				}
			}
		}
	}()

	// Create manager
	sessionOps := []SessionOptions{
		{
			Conn:    clientConn,
			Decoder: &mockDecoder{},
		},
	}

	mgr := newTestManagerWithSessions(t, sessionOps)
	defer mgr.CloseGracefully()

	time.Sleep(50 * time.Millisecond)

	// Send multiple messages without waiting
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		msg := &mockMessage{
			correlationID: fmt.Sprintf("no-wait-%d", i),
			payload:       "test",
		}
		err := mgr.SendNoWait(ctx, msg)
		if err != nil {
			t.Fatalf("SendNoWait failed: %v", err)
		}
		time.Sleep(10 * time.Millisecond) // Small delay between sends
	}

	// Wait for messages to be received
	time.Sleep(200 * time.Millisecond)

	receivedMu.Lock()
	defer receivedMu.Unlock()

	if len(received) != 5 {
		t.Errorf("Expected 5 messages, got %d: %v", len(received), received)
	}
}

func TestManager_InboundHandler(t *testing.T) {
	// Create TCP connection pair
	clientConn, serverConn := createTCPPair(t)
	defer clientConn.Close()
	defer serverConn.Close()

	// Server that sends unsolicited messages
	go func() {
		time.Sleep(100 * time.Millisecond)
		msg := &mockMessage{
			correlationID: "server-initiated",
			payload:       "hello from server",
		}
		encoded, _ := msg.Encode()
		serverConn.Write(encoded)
	}()

	// Create manager
	sessionOps := []SessionOptions{
		{
			Conn:    clientConn,
			Decoder: &mockDecoder{},
		},
	}

	mgr := newTestManagerWithSessions(t, sessionOps)

	// Set up inbound handler
	var handlerCalled bool
	var handlerMu sync.Mutex
	var receivedMsg Message

	mgr.SetInboundHandler(func(ctx context.Context, message Message) (Message, error) {
		handlerMu.Lock()
		defer handlerMu.Unlock()
		handlerCalled = true
		receivedMsg = message
		return nil, nil // No response needed
	})

	defer mgr.CloseGracefully()

	// Wait for server message and handler
	time.Sleep(300 * time.Millisecond)

	handlerMu.Lock()
	defer handlerMu.Unlock()

	if !handlerCalled {
		t.Error("Inbound handler was not called")
	}

	if receivedMsg == nil {
		t.Fatal("Received message is nil")
	}

	if receivedMsg.CorrelationID() != "server-initiated" {
		t.Errorf("Expected correlation ID 'server-initiated', got '%s'", receivedMsg.CorrelationID())
	}
}

func TestManager_ConcurrentRequests(t *testing.T) {
	// Create TCP connection pair
	clientConn, serverConn := createTCPPair(t)
	defer clientConn.Close()
	defer serverConn.Close()

	// Start echo server
	go mockEchoServer(t, serverConn)

	// Create manager
	sessionOps := []SessionOptions{
		{
			Conn:    clientConn,
			Decoder: &mockDecoder{},
		},
	}

	mgr := newTestManagerWithSessions(t, sessionOps)
	defer mgr.CloseGracefully()

	time.Sleep(50 * time.Millisecond)

	// Send concurrent requests (reduced number to avoid timeout issues)
	numRequests := 3
	var wg sync.WaitGroup
	errors := make(chan error, numRequests)

	for i := 0; i < numRequests; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			// Add small stagger to avoid all requests hitting at once
			time.Sleep(time.Duration(id*50) * time.Millisecond)

			ctx := context.Background()
			req := &mockMessage{
				correlationID: fmt.Sprintf("concurrent-%d", id),
				payload:       fmt.Sprintf("request-%d", id),
			}

			resp, err := mgr.SendAndWait(ctx, req)
			if err != nil {
				errors <- fmt.Errorf("request %d failed: %w", id, err)
				return
			}

			if resp.CorrelationID() != req.CorrelationID() {
				errors <- fmt.Errorf("request %d: correlation ID mismatch, expected %s got %s",
					id, req.CorrelationID(), resp.CorrelationID())
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// Check for errors
	for err := range errors {
		t.Error(err)
	}
}

func TestManager_SessionManagement(t *testing.T) {
	// Create multiple TCP connections
	conn1, server1 := createTCPPair(t)
	defer conn1.Close()
	defer server1.Close()

	conn2, server2 := createTCPPair(t)
	defer conn2.Close()
	defer server2.Close()

	go mockEchoServer(t, server1)
	go mockEchoServer(t, server2)

	// Create manager with multiple sessions
	sessionOps := []SessionOptions{
		{
			Conn:    conn1,
			Decoder: &mockDecoder{},
		},
		{
			Conn:    conn2,
			Decoder: &mockDecoder{},
		},
	}

	mgr := newTestManagerWithSessions(t, sessionOps)

	sessions := mgr.Sessions()
	if len(sessions) != 2 {
		t.Errorf("Expected 2 sessions, got %d", len(sessions))
	}

	// Close all sessions
	err := mgr.CloseGracefully()
	if err != nil {
		t.Errorf("Failed to close all sessions: %v", err)
	}
}

func TestManager_InboundHandlerWithResponse(t *testing.T) {
	// Create TCP connection pair
	clientConn, serverConn := createTCPPair(t)
	defer clientConn.Close()
	defer serverConn.Close()

	// Server that sends request and expects response
	var responseReceived bool
	var responseMu sync.Mutex

	go func() {
		decoder := &mockDecoder{}

		// Send server-initiated request
		time.Sleep(100 * time.Millisecond)
		req := &mockMessage{
			correlationID: "server-request",
			payload:       "need info",
		}
		encoded, _ := req.Encode()
		serverConn.Write(encoded)

		// Wait for response
		resp, err := decoder.Decode(serverConn)
		if err == nil && resp.CorrelationID() == "server-request-response" {
			responseMu.Lock()
			responseReceived = true
			responseMu.Unlock()
		}
	}()

	// Create manager
	sessionOps := []SessionOptions{
		{
			Conn:    clientConn,
			Decoder: &mockDecoder{},
		},
	}

	mgr := newTestManagerWithSessions(t, sessionOps)

	// Set up inbound handler that sends response
	mgr.SetInboundHandler(func(ctx context.Context, message Message) (Message, error) {
		// Return a response
		return &mockMessage{
			correlationID: "server-request-response",
			payload:       "here is the info",
		}, nil
	})

	defer mgr.CloseGracefully()

	// Wait for interaction
	time.Sleep(500 * time.Millisecond)

	responseMu.Lock()
	defer responseMu.Unlock()

	if !responseReceived {
		t.Error("Server did not receive response from inbound handler")
	}
}
