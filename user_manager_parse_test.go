package firebirdsql

import (
	"strings"
	"testing"
)

// serviceStreamFrames builds the op_response packets GetUsers reads: a
// ServiceStart ack, one isc_info_svc_to_eof chunk carrying payload, then a
// terminating chunk.
func serviceStreamFrames(payload []byte) []byte {
	var f acceptFrame
	f.opResponseFrame(0, nil) // ServiceStart ack
	chunk := append([]byte{isc_info_svc_to_eof, byte(len(payload)), byte(len(payload) >> 8)}, payload...)
	f.opResponseFrame(0, chunk)
	f.opResponseFrame(0, []byte{isc_info_svc_to_eof, 0, 0, isc_info_end})
	return f.bytes()
}

func newTestUserManager(frame []byte) *UserManager {
	return &UserManager{sm: &ServiceManager{wp: testProtocol(frame)}}
}

func TestGetUsers_Valid(t *testing.T) {
	payload := []byte{isc_spb_sec_username, 6, 0, 's', 'y', 's', 'd', 'b', 'a'}
	um := newTestUserManager(serviceStreamFrames(payload))
	users, err := um.GetUsers()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(users) != 1 || users[0].Username == nil || *users[0].Username != "sysdba" {
		t.Fatalf("users: got %+v, want one user 'sysdba'", users)
	}
}

func TestGetUsers_FieldBeforeUsername(t *testing.T) {
	// A firstname tag with no preceding username must error, not nil-panic.
	payload := []byte{isc_spb_sec_firstname, 3, 0, 'a', 'b', 'c'}
	um := newTestUserManager(serviceStreamFrames(payload))
	_, err := um.GetUsers()
	if err == nil {
		t.Fatal("expected error for field before username, got nil")
	}
	if !strings.Contains(err.Error(), "before any username") {
		t.Errorf("unexpected error: %v", err)
	}
}
