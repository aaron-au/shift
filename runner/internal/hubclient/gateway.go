package hubclient

// Gateway is one gateway the hub has told this runner to poll: where it is,
// and WHO it will prove itself to be when the runner dials it.
//
// The identity travels with the address because a hub-issued gateway
// certificate carries NO subject alternative name — deliberately, because a
// DMZ box has no stable hostname the hub can commit to at issue time. So the
// usual TLS hostname check has nothing to match, and the runner pins the
// COMMON NAME instead, which is the gateway's hub-assigned id.
//
// Without it a runner could verify only that a peer's certificate chained to
// the control-plane CA — and every runner in the fleet holds a certificate
// from that same CA. One compromised runner could then stand up a listener,
// be dialled as a gateway, and be handed inbound payload to answer. Pinning
// the id is what makes "this is gateway X" mean something.
//
// It lives here rather than in gwclient because gwclient imports this package
// and the reverse would be a cycle; gwclient re-exports it as an alias.
type Gateway struct {
	URL string `json:"url"`
	ID  string `json:"id"`
}
