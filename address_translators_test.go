package gocql

import (
	"testing"
	"net"
)

func TestIdentityAddressTranslator_NilAddrAndZeroPort(t *testing.T) {
	var tr AddressTranslator = IdentityTranslator()
	addr, port := tr.Translate(nil, 0)

	if addr != nil {
		t.Fatalf("expected address to be nil, got '%v'", addr)
	} else if port != 0 {
		t.Fatalf("expected port to be 0, got %d", port)
	}
}

func TestIdentityAddressTranslator_HostProvided(t *testing.T) {
	var tr AddressTranslator = IdentityTranslator()
	addr, port := tr.Translate(net.ParseIP("10.1.2.3"), 9042)

	if addr.String() != "10.1.2.3" {
		t.Fatalf("expected address to be '10.1.2.3', got '%v'", addr)
	} else if port != 9042 {
		t.Fatalf("expected port to be 9042, got %d", port)
	}
}

