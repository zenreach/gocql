package gocql

import (
	"net"
)

// AddressTranslator provides a way translate node addresses (and ports) when one is
// discovered or received as a node event. This is especially useful in ec2 (when
// using the EC2MultiRegionAddressTranslator) to translate public IPs to private IPs
// when possible.
type AddressTranslator interface {
	// Translate will translate the provided address and/or port when a host is discovered.
	Translate(addr net.IP, port int) (net.IP, int)
}

type AddressTranslatorFunc func(addr net.IP, port int) (net.IP, int)

func (fn AddressTranslatorFunc) Translate(addr net.IP, port int) (net.IP, int) {
	return fn(addr, port)
}

// IdentityTranslator will do nothing but return what it was provided. It is essentially a no-op.
func IdentityTranslator() AddressTranslator {
	return AddressTranslatorFunc(func(addr net.IP, port int) (net.IP, int) {
		return addr, port
	})
}
