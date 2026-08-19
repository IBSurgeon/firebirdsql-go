//go:build !plan9

package firebirdsql

import (
	"strings"
	"testing"
)

// auxResponseFrame builds the op_response packet connAuxRequest reads, carrying
// addrPayload as the length-prefixed data buffer.
func auxResponseFrame(addrPayload []byte) []byte {
	var f acceptFrame
	f.opResponseFrame(5, addrPayload) // aux handle 5
	return f.bytes()
}

func newTestSubscription(frame []byte) *Subscription {
	return &Subscription{fc: &firebirdsqlConn{wp: testProtocol(frame)}}
}

func sockAddrIPv4(port int, a, b, c, d byte) []byte {
	return []byte{
		byte(afInet & 0xFF), byte(afInet >> 8),
		byte(port >> 8), byte(port & 0xFF), // port, big-endian
		a, b, c, d, // address
		0, 0, 0, 0, 0, 0, 0, 0, // sockaddr_in padding
	}
}

func TestConnAuxRequest_ValidIPv4(t *testing.T) {
	s := newTestSubscription(auxResponseFrame(sockAddrIPv4(3051, 192, 168, 1, 5)))
	handle, addr, err := s.connAuxRequest()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if handle != 5 {
		t.Errorf("handle: got %d, want 5", handle)
	}
	if addr != "192.168.1.5:3051" {
		t.Errorf("addr: got %q, want 192.168.1.5:3051", addr)
	}
}

func TestConnAuxRequest_TruncatedBuffers(t *testing.T) {
	cases := []struct {
		name    string
		payload []byte
		wantSub string
	}{
		{"empty", nil, "too short"},
		{"header only", []byte{2, 0, 0x0B, 0xEB}, "IPv4 address truncated"},
		{"ipv6 truncated", append([]byte{
			byte(afInet6Linux & 0xFF), byte(afInet6Linux >> 8), 0x0B, 0xEB,
		}, make([]byte, 8)...), "IPv6 address truncated"},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			s := newTestSubscription(auxResponseFrame(tt.payload))
			_, _, err := s.connAuxRequest()
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !strings.Contains(err.Error(), tt.wantSub) {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}
