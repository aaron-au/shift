package gwclient

import (
	"encoding/json"
	"time"
)

// pollRequest mirrors the gateway's poll body. Kept in its own file so the
// wire shape is one small, reviewable thing rather than something buried in
// the loop — the gateway and the runner are separate modules by design
// (depguard), so this contract has no compiler enforcing it.
//
// It carries NO labels, and that omission is the security property of
// ADR-0041 §3. A runner used to state its own placement here, which meant a
// compromised or misconfigured one could claim `environment: production` and
// be handed production traffic. It now proves WHO it is with a client
// certificate and the hub's roster says WHAT it is; the runner has no way to
// assert placement at all.
type pollRequest struct {
	WaitSeconds float64 `json:"wait_seconds,omitempty"`
}

func encodePoll(wait time.Duration) ([]byte, error) {
	return json.Marshal(pollRequest{WaitSeconds: wait.Seconds()})
}
