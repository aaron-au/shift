package gwclient

import (
	"encoding/json"
	"time"
)

// pollRequest mirrors the gateway's poll body. Kept in its own file so the
// wire shape is one small, reviewable thing rather than something buried in
// the loop — the gateway and the runner are separate modules by design
// (depguard), so this contract has no compiler enforcing it.
type pollRequest struct {
	Labels      map[string]string `json:"labels,omitempty"`
	WaitSeconds float64           `json:"wait_seconds,omitempty"`
}

func encodePoll(labels map[string]string, wait time.Duration) ([]byte, error) {
	return json.Marshal(pollRequest{Labels: labels, WaitSeconds: wait.Seconds()})
}
