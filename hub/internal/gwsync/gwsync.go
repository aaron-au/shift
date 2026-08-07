// Package gwsync keeps adopted gateways converged on what the hub intends
// (ADR-0038 §6a, ADR-0049 §6).
//
// It exists because the gateway cannot ask for anything. It never dials
// inward, so it cannot fetch configuration when it starts, cannot request a
// renewal when its identity is about to lapse, and cannot report that it is
// behind. Every one of those is therefore something the HUB must notice, on a
// timer, and act on.
//
// That makes this loop the gateway's whole lifeline, and shapes two decisions
// below: renewal is treated as more urgent than configuration (a stale route
// table serves yesterday's traffic; a lapsed identity serves none), and a
// failing gateway is retried forever rather than being marked dead — there is
// no other way back.
package gwsync

import (
	"context"
	"log/slog"
	"time"

	"github.com/aaron-au/shift/hub/internal/gwpush"
	"github.com/aaron-au/shift/hub/internal/store"
)

// Options configure the loop.
type Options struct {
	Store  *store.Store
	Client *gwpush.Client

	// Interval is how often the hub looks for work (default 30s).
	Interval time.Duration

	// RenewBefore is how far ahead of expiry an identity is replaced
	// (default: a third of the CA's issuing TTL, floored at 4h).
	//
	// Generous on purpose. A runner that misses a renewal window retries in
	// seconds because it dials; a gateway that misses one is recovered only by
	// the pinned fallback, which is a heavier path. Renewing early costs a
	// certificate nobody needed.
	RenewBefore time.Duration

	// RunnerCA travels with every identity so the gateway can verify runners
	// polling it for work.
	RunnerCA func() []byte

	// ConfigFor builds the configuration for one gateway. Nil means this hub
	// pushes no configuration yet and the loop only maintains identities —
	// which is a coherent state, not a broken one: an adopted gateway with a
	// live identity and no routes serves 503, which is exactly what ADR-0038
	// says a gateway with nothing to route should do.
	ConfigFor func(ctx context.Context, gatewayID string) (any, error)
}

// Loop is the reconcile loop.
type Loop struct{ opts Options }

// New builds a loop.
func New(opts Options) *Loop {
	if opts.Interval <= 0 {
		opts.Interval = 30 * time.Second
	}
	if opts.RenewBefore <= 0 {
		opts.RenewBefore = 4 * time.Hour
	}
	return &Loop{opts: opts}
}

// Run reconciles until ctx ends.
func (l *Loop) Run(ctx context.Context) {
	t := time.NewTicker(l.opts.Interval)
	defer t.Stop()
	for {
		l.Tick(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// Tick performs one reconcile pass. Exported so a test can drive it without a
// clock, and so an administrator action can force one.
func (l *Loop) Tick(ctx context.Context) {
	// Pair first. A gateway waiting to be adopted is serving nothing at all,
	// so it is the most urgent thing in the pass — and pairing is what makes
	// everything below possible for it.
	pending, err := l.opts.Store.GatewaysPending(ctx)
	if err != nil {
		slog.Error("listing gateways to pair", "event", "gateway.reconcile_failed", "error", err.Error())
	}
	for _, gw := range pending {
		l.pair(ctx, gw)
	}

	due, err := l.opts.Store.GatewaysDue(ctx, l.opts.RenewBefore)
	if err != nil {
		slog.Error("listing gateways to reconcile", "event", "gateway.reconcile_failed", "error", err.Error())
		return
	}
	for _, gw := range due {
		l.reconcile(ctx, gw)
	}
}

// pair adopts a gateway that has never been adopted, using its one-time
// install token (ADR-0049 §1a).
//
// The fingerprint is LEARNED here and pinned from now on. Recording it and
// burning the token happen in one statement, so a token cannot be spent twice
// and two concurrent passes cannot both adopt.
func (l *Loop) pair(ctx context.Context, gw store.Gateway) {
	var runnerCA []byte
	if l.opts.RunnerCA != nil {
		runnerCA = l.opts.RunnerCA()
	}
	issued, fingerprint, err := l.opts.Client.Pair(ctx, gw.URL, gw.InstallToken, gw.ID, runnerCA)
	if err != nil {
		// Retried on the next tick until the token expires. A gateway that is
		// not deployed yet is the common case, not a fault.
		slog.Warn("pairing with a gateway", "event", "gateway.pair_failed",
			"gateway", gw.ID, "error", err.Error())
		_ = l.opts.Store.RecordGatewayPush(ctx, gw.ID, gw.PushedVersion, err)
		return
	}
	if err := l.opts.Store.LearnGatewayFingerprint(ctx, gw.ID, fingerprint); err != nil {
		slog.Error("recording a paired gateway's key", "event", "gateway.pair_failed",
			"gateway", gw.ID, "error", err.Error())
		return
	}
	if err := l.opts.Store.MarkGatewayAdopted(ctx, gw.ID, issued.Serial, issued.NotAfter); err != nil {
		slog.Error("recording an adoption", "event", "gateway.pair_failed",
			"gateway", gw.ID, "error", err.Error())
		return
	}
	slog.Info("gateway adopted", "event", "gateway.adopted",
		"gateway", gw.ID, "fingerprint", fingerprint, "cert_serial", issued.Serial)
}

func (l *Loop) reconcile(ctx context.Context, gw store.Gateway) {
	// Identity first. A gateway whose certificate lapses is unreachable over
	// mutual TLS, so pushing configuration before renewing would fail on
	// exactly the gateways that most need the pass to succeed.
	if l.needsIdentity(gw) {
		var runnerCA []byte
		if l.opts.RunnerCA != nil {
			runnerCA = l.opts.RunnerCA()
		}
		issued, err := l.opts.Client.Renew(ctx, gw.URL, gw.Fingerprint, gw.ID, runnerCA)
		if err != nil {
			slog.Error("renewing a gateway identity", "event", "gateway.renew_failed",
				"gateway", gw.ID, "error", err.Error())
			_ = l.opts.Store.RecordGatewayPush(ctx, gw.ID, gw.PushedVersion, err)
			return
		}
		if err := l.opts.Store.RecordGatewayCertificate(ctx, gw.ID, issued.Serial, issued.NotAfter); err != nil {
			slog.Error("recording a renewed gateway identity", "event", "gateway.renew_failed",
				"gateway", gw.ID, "error", err.Error())
			return
		}
		slog.Info("gateway identity renewed", "event", "gateway.renewed",
			"gateway", gw.ID, "cert_serial", issued.Serial)
	}

	if l.opts.ConfigFor == nil || gw.PushedVersion >= gw.ConfigVersion {
		return
	}
	cfg, err := l.opts.ConfigFor(ctx, gw.ID)
	if err != nil {
		slog.Error("building a gateway configuration", "event", "gateway.push_failed",
			"gateway", gw.ID, "error", err.Error())
		_ = l.opts.Store.RecordGatewayPush(ctx, gw.ID, gw.PushedVersion, err)
		return
	}
	if err := l.opts.Client.Push(ctx, gw.URL, gw.ID, cfg); err != nil {
		// Recorded, not retried here: the next tick retries. A gateway that
		// cannot be reached is never marked dead, because the hub dialling it
		// is the only way it can ever come back.
		slog.Error("pushing configuration to a gateway", "event", "gateway.push_failed",
			"gateway", gw.ID, "error", err.Error())
		_ = l.opts.Store.RecordGatewayPush(ctx, gw.ID, gw.PushedVersion, err)
		return
	}
	if err := l.opts.Store.RecordGatewayPush(ctx, gw.ID, gw.ConfigVersion, nil); err != nil {
		slog.Error("recording a gateway push", "event", "gateway.push_failed",
			"gateway", gw.ID, "error", err.Error())
		return
	}
	slog.Info("gateway configuration pushed", "event", "gateway.pushed",
		"gateway", gw.ID, "version", gw.ConfigVersion)
}

// needsIdentity reports whether the gateway's certificate is missing or close
// enough to expiry to replace.
func (l *Loop) needsIdentity(gw store.Gateway) bool {
	return gw.CertNotAfter == nil || time.Until(*gw.CertNotAfter) < l.opts.RenewBefore
}
