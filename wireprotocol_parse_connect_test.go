package firebirdsql

import (
	"bufio"
	"errors"
	"io"
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// Edge-case tests for _parse_connect_response read-error handling: partial
// reads, non-EOF transport failures, and the dummy-loop branches.
// No live Firebird server required.
// ---------------------------------------------------------------------------

// A read that makes progress before failing (some bytes arrive, then the
// connection drops) must surface the same way as a clean close: recvPackets
// returns the raw read error, and the handshake must not mistake the partly
// filled buffer for server data.
func TestParseConnectResponse_PartialReadThenClose(t *testing.T) {
	cases := []struct {
		name  string
		frame []byte
	}{
		{"two of four opcode bytes", []byte{0x00, 0x00}},
		{"two of four dummy-loop bytes", func() []byte {
			var f acceptFrame
			f.int32(op_dummy)
			f.buf.Write([]byte{0x00, 0x00})
			return f.bytes()
		}()},
		{"five of twelve header bytes", func() []byte {
			var f acceptFrame
			f.int32(op_accept)
			f.int32(PROTOCOL_VERSION13) // 4 of the 12 header bytes
			f.buf.Write([]byte{0x00})   // one more, then the stream ends
			return f.bytes()
		}()},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			err := parseConnectResponse(t, tt.frame)
			if !errors.Is(err, io.EOF) {
				t.Fatalf("want io.EOF, got %v", err)
			}
		})
	}
}

// errLinkReset stands in for a non-EOF transport failure, as a net.Error
// (timeout, reset) would be: the caller must be able to tell it apart both
// from a protocol violation and from a clean close.
var errLinkReset = errors.New("test: connection reset by peer")

// errAfterBytes serves its data, then fails every further read with err.
type errAfterBytes struct {
	data []byte
	err  error
}

func (r *errAfterBytes) Read(p []byte) (int, error) {
	if len(r.data) > 0 {
		n := copy(p, r.data)
		r.data = r.data[n:]
		return n, nil
	}
	return 0, r.err
}

// connectResponseErrAt feeds data to the handshake and fails the read that
// runs past it with errLinkReset.
func connectResponseErrAt(t *testing.T, data []byte) (*wireProtocol, error) {
	t.Helper()
	p := &wireProtocol{}
	p.conn.reader = bufio.NewReader(&errAfterBytes{data: data, err: errLinkReset})
	p.conn.writer = bufio.NewWriter(io.Discard)
	clientPublic, clientSecret, err := getClientSeed()
	if err != nil {
		t.Fatalf("getClientSeed: %v", err)
	}
	return p, p._parse_connect_response("SYSDBA", "masterkey", testConnectOptions(), clientPublic, clientSecret)
}

func TestParseConnectResponse_NonEOFErrorPropagated(t *testing.T) {
	cases := []struct {
		name string
		data []byte
	}{
		{"on the first read", nil},
		{"in the op_dummy loop", func() []byte {
			var f acceptFrame
			f.int32(op_dummy)
			return f.bytes()
		}()},
		{"on the header read", func() []byte {
			var f acceptFrame
			f.int32(op_accept) // straight to the 12-byte header
			return f.bytes()
		}()},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			_, err := connectResponseErrAt(t, tt.data)
			if !errors.Is(err, errLinkReset) {
				t.Fatalf("want the transport error as it is, got %q", err)
			}
			if strings.Contains(err.Error(), "protocol error") {
				t.Fatalf("transport error masked as a protocol violation: %q", err)
			}
		})
	}
}

// A failed header read must leave the handshake state untouched: a caller that
// ignores the error must not see half a header as a negotiated protocol
// version.
func TestParseConnectResponse_HeaderReadErrorLeavesHandshakeUntouched(t *testing.T) {
	var f acceptFrame
	f.int32(op_accept)
	f.int32(PROTOCOL_VERSION13) // 4 of the 12 header bytes; the rest never arrives

	p, err := connectResponseErrAt(t, f.bytes())
	if !errors.Is(err, errLinkReset) {
		t.Fatalf("want the transport error, got %v", err)
	}
	if p.protocolVersion != 0 {
		t.Errorf("protocolVersion = %d after a failed header read, want 0", p.protocolVersion)
	}
	if p.acceptArchitecture != 0 {
		t.Errorf("acceptArchitecture = %d after a failed header read, want 0", p.acceptArchitecture)
	}
	if p.acceptType != 0 {
		t.Errorf("acceptType = %d after a failed header read, want 0", p.acceptType)
	}
	if p.user != "" {
		t.Errorf("user = %q after a failed header read, want empty", p.user)
	}
}

// The op_dummy loop must keep working as before: a rejection after one or more
// dummies is still reported as a rejection.
func TestParseConnectResponse_DummyThenReject(t *testing.T) {
	var f acceptFrame
	f.int32(op_dummy)
	f.int32(op_reject)

	err := parseConnectResponse(t, f.bytes())
	if err == nil || !strings.Contains(err.Error(), "op_reject") {
		t.Fatalf("want the op_reject error, got %v", err)
	}
}

// A server error response after op_dummy (an auth failure, for instance)
// reaches the caller as the server's error, not as a transport failure.
func TestParseConnectResponse_DummyThenErrorResponse(t *testing.T) {
	var f acceptFrame
	f.int32(op_dummy)
	f.opResponseFrame(0, nil, isc_arg_gds, 335544336 /* deadlock */)

	err := parseConnectResponse(t, f.bytes())
	var fbErr *FbError
	if !errors.As(err, &fbErr) {
		t.Fatalf("want the server's error, got %v", err)
	}
	found := false
	for _, code := range fbErr.GDSCodes {
		if code == 335544336 {
			found = true
		}
	}
	if !found {
		t.Errorf("GDSCodes = %v, want it to contain 335544336", fbErr.GDSCodes)
	}
}

// Two op_dummy round trips before a legitimate accept: the handshake completes.
func TestParseConnectResponse_DummiesThenValidAccept(t *testing.T) {
	var f acceptFrame
	f.int32(op_dummy)
	f.int32(op_dummy)
	f.acceptHeader(op_accept_data)
	f.blob(srpServerData(32, 256)) // 292-byte blob: the shape real servers send
	f.blob([]byte("Srp256"))
	f.int32(0) // not yet authenticated
	f.blob(nil)

	if err := parseConnectResponse(t, f.bytes()); err != nil {
		t.Fatalf("handshake after two op_dummy failed: %v", err)
	}
}

// A failure on the very first read leaves nothing in flight: nothing has been
// written, and the error reaches the caller as it is.
func TestParseConnectResponse_ErrorOnFirstReadKeepsWriterEmpty(t *testing.T) {
	p, err := connectResponseErrAt(t, nil)
	if !errors.Is(err, errLinkReset) {
		t.Fatalf("want the transport error, got %v", err)
	}
	if n := p.conn.writer.Buffered(); n != 0 {
		t.Errorf("%d bytes left in the writer after a failed first read", n)
	}
}
