package gocql

import (
	"log"
	"net"
	"testing"
	"time"
)

func setupTestTcpServer(t *testing.T) (net.Listener, net.IP, int) {
	srvr, err := net.Listen("tcp4", "0.0.0.0:0")
	assertNil(t, "error when creating tcp server", err)
	testHost, testPort, err := parseIPPort(srvr.Addr().String())
	log.Printf("using host=%s and port=%d for test tcp server (err=%v)", testHost.String(), testPort, err)
	return srvr, testHost, testPort
}

func tcpConnWaitAndClose(t *testing.T, srvr net.Listener, tchan chan<- error) {
	conn, err := srvr.Accept()
	if conn != nil {
		log.Printf("connection received from addr=%+v", conn.RemoteAddr())
		conn.Close()
	}
	tchan <- err
}

func testConnErrorHandler(t *testing.T) ConnErrorHandler {
	return connErrorHandlerFn(func(conn *Conn, err error, closed bool) {
		t.Errorf("in connection handler: %v", err)
	})
}

func assertConnectionEventually(t *testing.T, delay time.Duration, tchan <-chan error) {
	select {
	case err := <-tchan:
		if err != nil {
			t.Errorf("accepting connection: %v", err)
		}
	case <-time.After(delay):
		t.Error("timed out waiting for connection")
	}
}

func TestSession_connect_WithNoTranslator(t *testing.T) {
	srvr, testHost, testPort := setupTestTcpServer(t)
	defer srvr.Close()

	session := createTestSession()
	defer session.Close()

	tchan := make(chan error)
	defer close(tchan)
	go tcpConnWaitAndClose(t, srvr, tchan)

	session.connect(srvr.Addr().String(), testConnErrorHandler(t), &HostInfo{
		peer: testHost.String(),
		port: testPort,
	})

	assertConnectionEventually(t, 500*time.Millisecond, tchan)
}

func TestSession_connect_WithTranslator(t *testing.T) {
	srvr, testHost, testPort := setupTestTcpServer(t)
	defer srvr.Close()

	session := createTestSession()
	defer session.Close()
	session.cfg.AddressTranslator = staticAddressTranslator(testHost, testPort)

	tchan := make(chan error)
	defer close(tchan)
	go tcpConnWaitAndClose(t, srvr, tchan)

	// the provided address will be translated
	session.connect("10.10.10.10:5432", testConnErrorHandler(t), &HostInfo{
		peer: "10.10.10.10",
		port: 5432,
	})

	assertConnectionEventually(t, 500*time.Millisecond, tchan)
}
