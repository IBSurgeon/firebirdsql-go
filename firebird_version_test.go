package firebirdsql

import (
	"strings"
	"testing"
)

// serverVersionFrame builds the op_response GetServerVersion reads: a service
// buffer of [isc_info_svc_server_version][2-byte len][banner].
func serverVersionFrame(banner string) []byte {
	var f acceptFrame
	buf := []byte{isc_info_svc_server_version, byte(len(banner)), byte(len(banner) >> 8)}
	buf = append(buf, banner...)
	f.opResponseFrame(0, buf)
	return f.bytes()
}

func TestGetServerVersion_Garbage(t *testing.T) {
	svc := &ServiceManager{wp: testProtocol(serverVersionFrame("totally bogus banner"))}
	_, err := svc.GetServerVersion()
	if err == nil {
		t.Fatal("expected error for unrecognized banner, got nil")
	}
	if !strings.Contains(err.Error(), "unrecognized server version") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestGetServerVersion_Valid(t *testing.T) {
	svc := &ServiceManager{wp: testProtocol(serverVersionFrame("LI-V3.0.11.33703 Firebird 3.0"))}
	v, err := svc.GetServerVersion()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v.Major != 3 || v.Minor != 0 {
		t.Errorf("got %d.%d, want 3.0", v.Major, v.Minor)
	}
}

func TestParseFirebirdVersion_RealBanners(t *testing.T) {
	cases := []struct {
		raw                       string
		major, minor, patch, bnum int
	}{
		{"LI-V2.5.9.27139 Firebird 2.5", 2, 5, 9, 27139},
		{"LI-V3.0.11.33703 Firebird 3.0", 3, 0, 11, 33703},
		{"WI-V5.0.1.1469 Firebird 5.0", 5, 0, 1, 1469},
		{"LI-T6.0.0.345 Firebird 6.0 Initial", 6, 0, 0, 345},
	}
	for _, tt := range cases {
		t.Run(tt.raw, func(t *testing.T) {
			v := ParseFirebirdVersion(tt.raw)
			if v.Major != tt.major || v.Minor != tt.minor || v.Patch != tt.patch || v.BuildNumber != tt.bnum {
				t.Errorf("got %d.%d.%d.%d, want %d.%d.%d.%d",
					v.Major, v.Minor, v.Patch, v.BuildNumber, tt.major, tt.minor, tt.patch, tt.bnum)
			}
			if v.Full == "" {
				t.Error("Full should be populated for a real banner")
			}
		})
	}
}

func TestParseFirebirdVersion_Garbage(t *testing.T) {
	// A non-matching banner must not panic; it yields a zero version with Raw
	// preserved and a conservatively-false EqualOrGreater.
	for _, raw := range []string{"", "not a version", "garbage from a hostile server"} {
		v := ParseFirebirdVersion(raw)
		if v.Full != "" || v.Major != 0 {
			t.Errorf("%q: expected zero version, got %+v", raw, v)
		}
		if v.Raw != raw {
			t.Errorf("%q: Raw not preserved, got %q", raw, v.Raw)
		}
		if v.EqualOrGreater(3, 0) {
			t.Errorf("%q: zero version must not satisfy EqualOrGreater(3,0)", raw)
		}
	}
}
