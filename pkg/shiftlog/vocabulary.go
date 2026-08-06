package shiftlog

// The log vocabulary (ADR-0046 §3), in one place so the rules a human reads
// and the rules the build enforces are the same list.
//
// The enforcement itself lives in vocabulary_test.go, which parses every
// non-test file in hub/, runner/ and gateway/ — including the gateway, which
// does not import this package. Nothing here is consulted at runtime; it
// exists to be read and to be tested against.

// CanonicalKeys are the context keys with ONE correct spelling. A near-miss is
// worse than a new key: two spellings of the same idea split every query that
// uses it, and the split is invisible until somebody's filter quietly returns
// half the records.
var CanonicalKeys = map[string]string{
	// wrong spelling → the one to use
	"task_id":     KeyTask,
	"taskid":      KeyTask,
	"taskID":      KeyTask,
	"flow_name":   KeyFlow,
	"flowname":    KeyFlow,
	"runner_id":   KeyRunner,
	"runnerid":    KeyRunner,
	"request_id":  KeyRequest,
	"requestid":   KeyRequest,
	"req_id":      KeyRequest,
	"reqid":       KeyRequest,
	"id":          KeyRequest,
	"correlation": KeyRequest,
	"gateway_id":  KeyGateway,
	"account_id":  KeyAccount,
	"dur_ms":      KeyDurationMS,
	"duration":    KeyDurationMS,
	"elapsed_ms":  KeyDurationMS,
	"took_ms":     KeyDurationMS,
	"err":         KeyError,
	"error_msg":   KeyError,
}

// ForbiddenKeys name things whose VALUE would be a credential or a payload.
// A record may identify a credential — `cert_serial`, a fingerprint — but a
// key called `token` is a key whose value is a token (ADR-0046 §7).
//
// Matched as substrings of the key, so `api_token` and `client_secret` are
// caught along with `token` and `secret`.
var ForbiddenKeys = []string{
	"secret",
	"token",
	"password",
	"passwd",
	"credential",
	"authorization",
	"cookie",
	"private_key",
	"apikey",
	"api_key",
	"payload",
	"record_data",
}

// KeyExempt marks a key that looks forbidden but is not, because it names a
// credential rather than carrying one.
var KeyExempt = map[string]bool{
	// The SHA-256 of a capability token, which is what the hub compares
	// against — knowing it grants nothing (ADR-0042 §3b).
	"token_sha256": true,
	// Which certificate, not the key material.
	"cert_serial": true,
}
