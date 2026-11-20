package firebirdsql

import "testing"

// The gbak_attach connection option is off unless asked for: an ordinary
// attach must never carry isc_dpb_gbak_attach, which lets it into a database
// that is being restored.
func TestParseDSNGbakAttach(t *testing.T) {
	for _, c := range []struct {
		dsn  string
		want bool
	}{
		{"user:password@localhost:3050/db.fdb", false},
		{"user:password@localhost:3050/db.fdb?gbak_attach=false", false},
		{"user:password@localhost:3050/db.fdb?gbak_attach=true", true},
		{"user:password@localhost:3050/db.fdb?gbak_attach=nonsense", false},
	} {
		dsn, err := parseDSN(c.dsn)
		if err != nil {
			t.Errorf("parseDSN(%q): %v", c.dsn, err)
			continue
		}
		if got := convertToBool(dsn.options["gbak_attach"], false); got != c.want {
			t.Errorf("parseDSN(%q): gbak_attach = %v, want %v", c.dsn, got, c.want)
		}
	}
}
