package gocql

import (
	"net"
	"sync"
	"testing"
	"time"
)

func TestEventDebounce(t *testing.T) {
	const eventCount = 150
	wg := &sync.WaitGroup{}
	wg.Add(1)

	eventsSeen := 0
	debouncer := newEventDeouncer("testDebouncer", func(events []frame) {
		defer wg.Done()
		eventsSeen += len(events)
	})
	defer debouncer.stop()

	for i := 0; i < eventCount; i++ {
		debouncer.debounce(&statusChangeEventFrame{
			change: "UP",
			host:   net.IPv4(127, 0, 0, 1),
			port:   9042,
		})
	}

	wg.Wait()
	if eventCount != eventsSeen {
		t.Fatalf("expected to see %d events but got %d", eventCount, eventsSeen)
	}
}

func TestHandleNodeEvent_TopicChange_TranslatesAddress(t *testing.T) {
	defer (func (d time.Duration) { nodeUpDelayDuration = d })(nodeUpDelayDuration)
	nodeUpDelayDuration = 0
	session := createTestSession()
	defer session.Close()
	session.cfg.AddressTranslator = staticAddressTranslator(net.ParseIP("127.10.10.220"), 5432)
	assertEqual(t, "ring host length", 0, len(session.ring.hostList))

	session.handleNodeEvent([]frame{
		&topologyChangeEventFrame{
			frameHeader: frameHeader{
				version:       protoVersion4,
				flags:         0,
				stream:        0,
				op:            opEvent,
				length:        0,
				customPayload: make(map[string][]byte),
			},
			change: "NEW_NODE",
			host:   net.IPv4(10, 10, 220, 1),
			port:   9042,
		},
	})
	assertEqual(t, "new ring host length", 1, len(session.ring.hosts))
	assertEqual(t, "translated host and port", 5432, session.ring.hosts["127.10.10.220"].port)
}

func TestHandleNodeEvent_StatusChange_TranslatesAddress(t *testing.T) {
	defer (func (d time.Duration) { nodeUpDelayDuration = d })(nodeUpDelayDuration)
	nodeUpDelayDuration = 0
	session := createTestSession()
	defer session.Close()
	session.cfg.AddressTranslator = staticAddressTranslator(net.ParseIP("127.10.10.220"), 5432)
	assertEqual(t, "ring host length", 0, len(session.ring.hostList))

	session.handleNodeEvent([]frame{
		&statusChangeEventFrame{
			frameHeader: frameHeader{
				version:       protoVersion4,
				flags:         0,
				stream:        0,
				op:            opEvent,
				length:        0,
				customPayload: make(map[string][]byte),
			},
			change: "UP",
			host:   net.IPv4(10, 10, 220, 1),
			port:   9042,
		},
	})
	assertEqual(t, "new ring host length", 1, len(session.ring.hosts))
	assertEqual(t, "translated host and port", 5432, session.ring.hosts["127.10.10.220"].port)
}
