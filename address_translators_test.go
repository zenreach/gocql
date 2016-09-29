package gocql

import "testing"

func TestIdentityAddressTranslator_EmptyHostAndZeroPort(t *testing.T) {
	var tr AddressTranslator = IdentityTranslator()
	host, port := tr.Translate("", 0)

	if host != "" {
		t.Fatalf("expected host to be empty string, got '%v'", host)
	} else if port != 0 {
		t.Fatalf("expected port to be 0, got %d", port)
	}
}

func TestIdentityAddressTranslator_HostProvided(t *testing.T) {
	var tr AddressTranslator = IdentityTranslator()
	host, port := tr.Translate("10.1.2.3", 9042)

	if host != "10.1.2.3" {
		t.Fatalf("expected host to be '10.1.2.3', got '%v'", host)
	} else if port != 9042 {
		t.Fatalf("expected port to be 9042, got %d", port)
	}
}

