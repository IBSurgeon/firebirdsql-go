package firebirdsql

import (
	"context"
	"errors"
	"io"
	"slices"
	"testing"
)

// ---------------------------------------------------------------------------
// Edge-case tests around receiveTwoResponses and the Exec/Cancel wire guards.
// No live Firebird server required.
// ---------------------------------------------------------------------------

// When both replies are refusals the caller gets the first one: it names the
// operation that actually failed. The second refusal is still read in full, so
// the connection stays in step.
func TestBatchTwoRepliesFirstErrorWins(t *testing.T) {
	requests := []struct {
		name string
		send func(p *wireProtocol) error
	}{
		{"create", func(p *wireProtocol) error {
			return p.opBatchCreate(2, []byte{1}, 8, []byte{batchVersion1})
		}},
		{"msg", func(p *wireProtocol) error {
			return p.opBatchMsg(2, [][]byte{{0, 0, 0, 1}})
		}},
		{"release", func(p *wireProtocol) error {
			return p.opBatchRelease(2, op_batch_rls)
		}},
	}
	for _, rq := range requests {
		t.Run(rq.name, func(t *testing.T) {
			var f acceptFrame
			f.opResponseFrame(0, nil, isc_arg_gds, 335544336)    // deadlock: the batch refusal
			f.opResponseFrame(0, nil, isc_arg_gds, ISCCancelled) // the follow-up reply's refusal
			f.opResponseFrame(7, nil)                            // marker: the next reply on the wire
			p := testProtocol(f.bytes())
			p.protocolVersion = PROTOCOL_VERSION16

			err := rq.send(p)
			var fbErr *FbError
			if !errors.As(err, &fbErr) {
				t.Fatalf("err = %v, want the server's refusal", err)
			}
			if !slices.Contains(fbErr.GDSCodes, 335544336) {
				t.Fatalf("GDSCodes = %v, want the first refusal (deadlock)", fbErr.GDSCodes)
			}
			if slices.Contains(fbErr.GDSCodes, ISCCancelled) {
				t.Fatalf("GDSCodes = %v, got the second refusal instead of the first", fbErr.GDSCodes)
			}
			if p.desynced {
				t.Fatal("desynced after both refusals read in full")
			}
			handle, _, _, err := p.opResponse()
			if err != nil || handle != 7 {
				t.Fatalf("next reply = handle %d, err %v; want the marker (handle 7)", handle, err)
			}
			if n := p.conn.reader.Buffered(); n != 0 {
				t.Fatalf("%d bytes left on the wire", n)
			}
		})
	}
}

// A server may interleave an op_dummy between the two replies; reading them
// both must skip it.
func TestBatchTwoRepliesTolerateDummyBetween(t *testing.T) {
	var f acceptFrame
	f.opResponseFrame(0, nil) // batch request reply: ok
	f.int32(op_dummy)         // the follow-up reply is one dummy deeper
	f.opResponseFrame(0, nil) // its op_ping reply
	f.opResponseFrame(7, nil) // marker
	p := testProtocol(f.bytes())
	p.protocolVersion = PROTOCOL_VERSION16

	if err := p.opBatchRelease(2, op_batch_rls); err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if p.desynced {
		t.Fatal("desynced after replies read around an op_dummy")
	}
	handle, _, _, err := p.opResponse()
	if err != nil || handle != 7 {
		t.Fatalf("next reply = handle %d, err %v; want the marker (handle 7)", handle, err)
	}
	if n := p.conn.reader.Buffered(); n != 0 {
		t.Fatalf("%d bytes left on the wire", n)
	}
}

// Cancel on an in-step wire still releases the server batch, and surfaces a
// refusal instead of hiding it: the ping reply must be consumed either way, or
// the next request on the connection reads it as its own.
func TestBatchCancelOnHealthyWire(t *testing.T) {
	t.Run("release accepted", func(t *testing.T) {
		var f acceptFrame
		f.opResponseFrame(0, nil) // op_batch_rls
		f.opResponseFrame(0, nil) // its op_ping
		fc, log := batchTestConn(f.bytes())
		b := &PreparedBatch{fc: fc, stmt: &firebirdsqlStmt{fc: fc, stmtHandle: 2}, created: true}

		if err := b.Cancel(context.Background()); err != nil {
			t.Fatalf("Cancel err = %v, want nil", err)
		}
		if b.created {
			t.Fatal("created still set")
		}
		fc.wp.conn.writer.Flush()
		if !log.sent(op_batch_rls) {
			t.Fatal("no op_batch_rls sent")
		}
		if n := fc.wp.conn.reader.Buffered(); n != 0 {
			t.Fatalf("%d reply bytes left unread", n)
		}
	})

	t.Run("release refused", func(t *testing.T) {
		var f acceptFrame
		f.opResponseFrame(0, nil, isc_arg_gds, 335544336) // op_batch_rls refused
		f.opResponseFrame(0, nil)                         // its op_ping
		fc, _ := batchTestConn(f.bytes())
		b := &PreparedBatch{fc: fc, stmt: &firebirdsqlStmt{fc: fc, stmtHandle: 2}, created: true}

		err := b.Cancel(context.Background())
		var fbErr *FbError
		if !errors.As(err, &fbErr) {
			t.Fatalf("Cancel err = %v, want the server's refusal", err)
		}
		if b.created {
			t.Fatal("created still set")
		}
		if !fc.IsValid() {
			t.Fatal("IsValid() = false after a refusal read in full")
		}
		if n := fc.wp.conn.reader.Buffered(); n != 0 {
			t.Fatalf("%d reply bytes left unread; the ping reply was not consumed", n)
		}
	})
}

// A completion reply cut short: the release that follows runs into the same
// dead wire, so the connection must end up marked desynced — not handed back
// to the pool to read the next request's reply as its own.
func TestBatchExecTruncatedCompletionDesyncs(t *testing.T) {
	var f acceptFrame
	f.int32(op_batch_cs) // the completion header, then the stream ends
	fc, log := batchTestConn(f.bytes())
	b := &PreparedBatch{fc: fc, stmt: &firebirdsqlStmt{fc: fc, stmtHandle: 2}, created: true}

	_, err := b.Exec(context.Background())
	if !errors.Is(err, io.EOF) {
		t.Fatalf("Exec err = %v, want the raw read error", err)
	}
	if b.created {
		t.Fatal("created still set")
	}
	if fc.IsValid() {
		t.Fatal("IsValid() = true after a reply cut short")
	}
	fc.wp.conn.writer.Flush()
	if log.sent(op_commit_retaining) {
		t.Fatal("op_commit_retaining sent into a wire whose position is unknown")
	}
}

// Row errors in the completion roll the autocommit transaction back — the
// batch never commits — and leave the connection in step with the server.
func TestBatchExecRowErrorsRollBack(t *testing.T) {
	var f acceptFrame
	// Completion: stmt 2, total 2 rows, no update counts, one detailed error
	// (row 1, deadlock), no simplified errors.
	for _, v := range []int32{op_batch_cs, 2, 2, 0, 1, 0, 1} {
		f.int32(v)
	}
	f.int32(isc_arg_gds)
	f.int32(335544336) // deadlock
	f.int32(isc_arg_end)
	f.opResponseFrame(0, nil) // op_batch_rls
	f.opResponseFrame(0, nil) // its op_ping
	f.opResponseFrame(0, nil) // the rollback
	fc, log := batchTestConn(f.bytes())
	b := &PreparedBatch{fc: fc, stmt: &firebirdsqlStmt{fc: fc, stmtHandle: 2}, created: true}

	res, err := b.Exec(context.Background())
	var be *BatchExecutionError
	if !errors.As(err, &be) {
		t.Fatalf("Exec err = %v, want a BatchExecutionError", err)
	}
	if len(be.Errors) != 1 || be.Errors[0].Row != 1 {
		t.Fatalf("errors = %+v, want one error at row 1", be.Errors)
	}
	var fbErr *FbError
	if !errors.As(be.Errors[0].Err, &fbErr) || !slices.Contains(fbErr.GDSCodes, 335544336) {
		t.Fatalf("row error = %v, want the deadlock", be.Errors[0].Err)
	}
	fc.wp.conn.writer.Flush()
	if log.sent(op_commit_retaining) {
		t.Fatal("a batch with row errors was committed")
	}
	if !log.sent(op_rollback) {
		t.Fatal("no rollback of the failed batch")
	}
	if res == nil {
		t.Fatal("Exec returned no result alongside the batch error")
	}
	if !fc.IsValid() {
		t.Fatal("IsValid() = false after the rollback")
	}
	if n := fc.wp.conn.reader.Buffered(); n != 0 {
		t.Fatalf("%d reply bytes left unread", n)
	}
}

// Exec that starts on a connection already out of step: unlike Cancel, Exec
// has no wire check at entry, so the exchange runs and fails on the reads.
// The outcome must still be safe — nothing committed, batch released, the
// connection stays flagged for eviction.
func TestBatchExecOnDesyncedWireStaysUncommitted(t *testing.T) {
	fc, log := batchTestConn(nil)
	fc.wp.desynced = true
	b := &PreparedBatch{fc: fc, stmt: &firebirdsqlStmt{fc: fc, stmtHandle: 2}, created: true}

	_, err := b.Exec(context.Background())
	if err == nil {
		t.Fatal("Exec succeeded on a desynced wire")
	}
	if b.created {
		t.Fatal("created still set")
	}
	if fc.IsValid() {
		t.Fatal("IsValid() = true on a desynced connection")
	}
	fc.wp.conn.writer.Flush()
	if log.sent(op_commit_retaining) {
		t.Fatal("op_commit_retaining sent into a desynced wire")
	}
}
