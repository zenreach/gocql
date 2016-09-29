package gocql

import (
	"log"
	"net"
)

// AddressTranslator provides a way translate node addresses (and ports) when one is
// discovered or received as a node event. This is especially useful in ec2 (when
// using the EC2MultiRegionAddressTranslator) to translate public IPs to private IPs
// when possible.
type AddressTranslator interface {
	// Translate will translate the provided address and/or port when a host is discovered.
	Translate(addr string, port int) (string, int)
}

type AddressTranslatorFunc func(addr string, port int) (string, int)

func (fn AddressTranslatorFunc) Translate(addr string, port int) (string, int) {
	return fn(addr, port)
}

// IdentityTranslator will do nothing but return what it was provided. It is essentially a no-op.
func IdentityTranslator() AddressTranslator {
	return AddressTranslatorFunc(func(addr string, port int) (string, int) {
		return addr, port
	})
}

// EC2MultiRegionAddressTranslator will do a reverse DNS lookup on the address provided,
// then do a DNS lookup on the first hostname found in order to potentially get the
// private IP of the given address. If no results are found in either lookup,
// the given address is returned. Port is always returned unchanged. Build with the
// gocql_debug tag to see the address translation at work.
func EC2MultiRegionAddressTranslator() AddressTranslator {
	return AddressTranslatorFunc(func(addr string, port int) (string, int) {
		hosts, err := net.LookupAddr(addr)

		if err != nil || len(hosts) < 1 {
			return addr, port
		}

		addresses, err := net.LookupHost(hosts[0])
		if err != nil || len(addresses) < 1 {
			return addr, port
		}

		if gocqlDebug {
			log.Printf("gocql: translated ec2 address from '%s' to '%s'\n", addr, addresses[0])
		}
		return addresses[0], port
	})
}
