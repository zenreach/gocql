package gocql

import (
	"testing"
	"net"
	"time"
	"log"
)

func TestSession_connect_WithNoTranslator(t *testing.T) {
	srvr, err := net.Listen("tcp", ":0")
	assertNil(t, "error when creating tcp server", err)
	defer srvr.Close()

	testHost, testPort, err := parseIPPort(srvr.Addr().String())
	log.Printf("using host=%s and port=%d for test tcp server (err=%v)", testHost.String(), testPort, err)

	session := createTestSession()
	defer session.Close()

	tchan := make(chan error)
	defer close(tchan)
	go func(t *testing.T, srvr net.Listener, cchan chan<- error) {
		conn, err := srvr.Accept()
		if conn != nil {
			log.Printf("connection received from addr=%+v", conn.RemoteAddr())
			conn.Close()
		}
		cchan <- err
	}(t, srvr, tchan)

	errHandler := connErrorHandlerFn(func (conn *Conn, err error, closed bool) {
		t.Errorf("in connection handler: %v", err)
	})

	session.connect(srvr.Addr().String(), errHandler, &HostInfo{
		peer: testHost.String(),
		port: testPort,
	})

	select {
	case err := <-tchan:
		if err != nil {
			t.Errorf("accepting connection: %v", err)
		}
	case <-time.After(100*time.Millisecond):
		t.Error("timed out waiting for connection")
	}
}

func TestSession_connect_WithTranslator(t *testing.T) {
	srvr, err := net.Listen("tcp", ":0")
	assertNil(t, "error when creating tcp server", err)
	defer srvr.Close()

	testHost, testPort, err := parseIPPort(srvr.Addr().String())
	log.Printf("using host=%s and port=%d for test tcp server (err=%v)", testHost.String(), testPort, err)

	session := createTestSession()
	defer session.Close()
	session.cfg.AddressTranslator = staticAddressTranslator(testHost, testPort)

	tchan := make(chan error)
	defer close(tchan)
	go func(t *testing.T, srvr net.Listener, cchan chan<- error) {
		conn, err := srvr.Accept()
		if conn != nil {
			log.Printf("connection received from addr=%+v", conn.RemoteAddr())
			conn.Close()
		}
		cchan <- err
	}(t, srvr, tchan)

	errHandler := connErrorHandlerFn(func (conn *Conn, err error, closed bool) {
		t.Errorf("in connection handler: %v", err)
	})

	session.connect("10.10.10.10:5432", errHandler, &HostInfo{
		peer: "10.10.10.10",
		port: 5432,
	})

	select {
	case err := <-tchan:
		if err != nil {
			t.Errorf("accepting connection: %v", err)
		}
	case <-time.After(100*time.Millisecond):
		t.Error("timed out waiting for connection")
	}
}
