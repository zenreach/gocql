// +build ec2_test

package gocql

import "testing"

// Notice: if these tests are failing for you, then you most likely need to actually edit your /etc/hosts
// file and add the following:
//   10.10.220.253   ec2.test.host.gocql
//   10.10.220.254   ec2.test.host.gocql
//
// This is unfortunate, but at the moment, Go does not provide any mechanism for configuring a resolver.

func TestEC2MultiRegionAddressTranslator_NoHostsFound_ReturnsProvidedArgs(t *testing.T) {
	host, port := EC2MultiRegionAddressTranslator().Translate("10.10.220.255", 9042)
	assertEqual(t, "ec2 translated host", "10.10.220.255", host)
	assertEqual(t, "ec2 translated port", 9042, port)
}

func TestEC2MultiRegionAddressTranslator_Translates(t *testing.T) {
	host, port := EC2MultiRegionAddressTranslator().Translate("10.10.220.254", 9042)
	assertEqual(t, "ec2 translated host", "10.10.220.253", host)
	assertEqual(t, "ec2 translated port", 9042, port)
}
