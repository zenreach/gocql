// Copyright (c) 2012 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gocql

import (
	"errors"
	"time"
	"net"
	"strconv"
	"log"
)

// PoolConfig configures the connection pool used by the driver, it defaults to
// using a round-robin host selection policy and a round-robin connection selection
// policy for each host.
type PoolConfig struct {
	// HostSelectionPolicy sets the policy for selecting which host to use for a
	// given query (default: RoundRobinHostPolicy())
	HostSelectionPolicy HostSelectionPolicy
}

func (p PoolConfig) buildPool(session *Session) *policyConnPool {
	return newPolicyConnPool(session)
}

// ClusterConfig is a struct to configure the default cluster implementation
// of gocoql. It has a variety of attributes that can be used to modify the
// behavior to fit the most common use cases. Applications that require a
// different setup must implement their own cluster.
type ClusterConfig struct {
	Hosts             []string          // addresses for the initial connections
	CQLVersion        string            // CQL version (default: 3.0.0)
	ProtoVersion      int               // version of the native protocol (default: 2)
	Timeout           time.Duration     // connection timeout (default: 600ms)
	Port              int               // port (default: 9042)
	Keyspace          string            // initial keyspace (optional)
	NumConns          int               // number of connections per host (default: 2)
	Consistency       Consistency       // default consistency level (default: Quorum)
	Compressor        Compressor        // compression algorithm (default: nil)
	Authenticator     Authenticator     // authenticator (default: nil)
	RetryPolicy       RetryPolicy       // Default retry policy to use for queries (default: 0)
	SocketKeepalive   time.Duration     // The keepalive period to use, enabled if > 0 (default: 0)
	MaxPreparedStmts  int               // Sets the maximum cache size for prepared statements globally for gocql (default: 1000)
	MaxRoutingKeyInfo int               // Sets the maximum cache size for query info about statements for each session (default: 1000)
	PageSize          int               // Default page size to use for created sessions (default: 5000)
	SerialConsistency SerialConsistency // Sets the consistency for the serial part of queries, values can be either SERIAL or LOCAL_SERIAL (default: unset)
	SslOpts           *SslOptions
	DefaultTimestamp  bool // Sends a client side timestamp for all requests which overrides the timestamp at which it arrives at the server. (default: true, only enabled for protocol 3 and above)
	// PoolConfig configures the underlying connection pool, allowing the
	// configuration of host selection and connection selection policies.
	PoolConfig PoolConfig

	// If not zero, gocql attempt to reconnect known DOWN nodes in every ReconnectSleep.
	ReconnectInterval time.Duration

	// The maximum amount of time to wait for schema agreement in a cluster after
	// receiving a schema change frame. (deault: 60s)
	MaxWaitSchemaAgreement time.Duration

	// HostFilter will filter all incoming events for host, any which don't pass
	// the filter will be ignored. If set will take precedence over any options set
	// via Discovery
	HostFilter HostFilter

	// AddressTranslator will translate addresses found on peer discovery and/or
	// node change events.
	AddressTranslator AddressTranslator

	// If IgnorePeerAddr is true and the address in system.peers does not match
	// the supplied host by either initial hosts or discovered via events then the
	// host will be replaced with the supplied address.
	//
	// For example if an event comes in with host=10.0.0.1 but when looking up that
	// address in system.local or system.peers returns 127.0.0.1, the peer will be
	// set to 10.0.0.1 which is what will be used to connect to.
	IgnorePeerAddr bool

	// If DisableInitialHostLookup then the driver will not attempt to get host info
	// from the system.peers table, this will mean that the driver will connect to
	// hosts supplied and will not attempt to lookup the hosts information, this will
	// mean that data_centre, rack and token information will not be available and as
	// such host filtering and token aware query routing will not be available.
	DisableInitialHostLookup bool

	// Configure events the driver will register for
	Events struct {
		// disable registering for status events (node up/down)
		DisableNodeStatusEvents bool
		// disable registering for topology events (node added/removed/moved)
		DisableTopologyEvents bool
		// disable registering for schema events (keyspace/table/function removed/created/updated)
		DisableSchemaEvents bool
	}

	// DisableSkipMetadata will override the internal result metadata cache so that the driver does not
	// send skip_metadata for queries, this means that the result will always contain
	// the metadata to parse the rows and will not reuse the metadata from the prepared
	// statement.
	//
	// See https://issues.apache.org/jira/browse/CASSANDRA-10786
	DisableSkipMetadata bool

	// internal config for testing
	disableControlConn bool
}

// NewCluster generates a new config for the default cluster implementation.
func NewCluster(hosts ...string) *ClusterConfig {
	cfg := &ClusterConfig{
		Hosts:                  hosts,
		CQLVersion:             "3.0.0",
		ProtoVersion:           2,
		Timeout:                600 * time.Millisecond,
		Port:                   9042,
		NumConns:               2,
		Consistency:            Quorum,
		MaxPreparedStmts:       defaultMaxPreparedStmts,
		MaxRoutingKeyInfo:      1000,
		PageSize:               5000,
		DefaultTimestamp:       true,
		MaxWaitSchemaAgreement: 60 * time.Second,
		ReconnectInterval:      60 * time.Second,
	}
	return cfg
}

// CreateSession initializes the cluster based on this config and returns a
// session object that can be used to interact with the database.
func (cfg *ClusterConfig) CreateSession() (*Session, error) {
	return NewSession(*cfg)
}

// translateHostPort is a helper method that will use the given AddressTranslator
// if defined, to translate the given host:port (addr) into a new host:port string.
// If no AddressTranslator or if an error occurs, the given host:port will be returned.
func (cfg *ClusterConfig) translateHostPort(hostPort string) (string) {
	if cfg.AddressTranslator == nil {
		return hostPort
	}
	addr, port, err := parseIPPort(hostPort)
	if err != nil {
		return hostPort
	}
	newAddr, newPort := cfg.AddressTranslator.Translate(addr, port)
	if gocqlDebug {
		log.Printf("gocql: translating address '%s' to '%s:%d'", hostPort, newAddr.String(), newPort)
	}
	return net.JoinHostPort(newAddr.String(), strconv.Itoa(newPort))
}

func parseIPPort(ipPort string) (net.IP, int, error) {
	addr, portStr, err := net.SplitHostPort(ipPort)
	if err != nil {
		return nil, 0, err
	}
	port, _ := strconv.Atoi(portStr)
	return net.ParseIP(addr), port, nil
}

var (
	ErrNoHosts              = errors.New("no hosts provided")
	ErrNoConnectionsStarted = errors.New("no connections were made when creating the session")
	ErrHostQueryFailed      = errors.New("unable to populate Hosts")
)
