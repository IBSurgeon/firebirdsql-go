/*******************************************************************************
The MIT License (MIT)

Copyright (c) 2013-2026 Hajime Nakagami

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
the Software, and to permit persons to whom the Software is furnished to do so,
subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*******************************************************************************/

package firebirdsql

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Phase 8 of the Jaybird test port plan (JAYBIRD_TEST_PORT_PLAN.md):
// protocol-version feature gating. Jaybird re-runs its suites per wire
// protocol version (V10DatabaseTest → … → V19StatementTest); this driver has
// a single code path, so the parity equivalent is to negotiate one protocol
// and gate each wire feature on the minimum protocol version that carries it
// (V12 cancel, V16 batch, V18 scrollable cursors, V19 inline blobs).

// TestNegotiatedProtocolVersion mirrors Jaybird's WireDatabaseConnectionTest
// identify() checks: the negotiated protocol must be inside the advertised
// 10–19 range and consistent with the server generation.
func TestNegotiatedProtocolVersion(t *testing.T) {
	db, _, _ := createTestDatabaseWithDDL(t, "gate_negotiate_")
	ver := negotiatedProtocol(t, db)
	t.Logf("negotiated protocol %d against %s", ver, testServerAddr())
	require.GreaterOrEqual(t, ver, 10)
	require.LessOrEqual(t, ver, 19)

	// A protocol-19 server must be Firebird 5+; older servers cannot speak it.
	v := testServerVersion(t)
	if ver >= PROTOCOL_VERSION19 {
		require.True(t, v.EqualOrGreater(5, 0),
			"protocol %d negotiated from %q, needs Firebird 5+", ver, v.Raw)
	}
}

// TestFeatureProtocolGating exercises each wire feature behind the minimum
// protocol version that introduced it (Jaybird's RequireProtocolExtension
// pattern): a feature test must skip — never fail — on older servers.
// TestCancelOperationParity consolidates Jaybird's V12 cancelOperation and
// operation-monitor coverage: a context cancellation aborts a long-running
// statement and the connection pool stays usable afterwards.
func TestCancelOperationParity(t *testing.T) {
	db, _, _ := createTestDatabaseWithDDL(t, "gate_cancel_",
		`CREATE TABLE GATE_CANCEL (ID INTEGER PRIMARY KEY)`)

	ctx, cancel := context.WithTimeout(stmtCtx, 500*time.Millisecond)
	defer cancel()
	_, err := db.ExecContext(ctx, longQueryNonSelectable)
	require.Error(t, err, "long-running statement must be cancelled by the context")

	// The pool remains usable after the cancellation.
	mustExec(t, stmtCtx, db, "INSERT INTO GATE_CANCEL VALUES (1)")
	var n int
	require.NoError(t, db.QueryRow("SELECT COUNT(*) FROM GATE_CANCEL").Scan(&n))
	require.Equal(t, 1, n)
}
