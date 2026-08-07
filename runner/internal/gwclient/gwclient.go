// Package gwclient is the runner's gateway intake (ADR-0038 §4): a loop per
// gateway that long-polls for inbound HTTP work, executes the named flow, and
// posts the flow's output back.
//
// The direction is the whole point. The gateway sits in a DMZ and never dials
// inward; the runner reaches OUT to it. Two consequences fall out for free:
//
//   - A runner behind NAT, a firewall, or a Kubernetes NetworkPolicy that
//     denies all ingress is still reachable for inbound traffic — it is not
//     "reachable" at all, it is a client.
//   - The set of runners currently polling IS the set available. There is no
//     liveness table to keep fresh and no way to route to a dead backend.
//
// A runner polls EVERY gateway eligible to send it work, because each
// gateway's poll registry is its own (shared state between DMZ hosts is
// exactly what this design avoids). The hub computes that address list — it
// already holds the gateway records and the runner labels — so adding a
// gateway reconfigures nothing here.
package gwclient

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aaron-au/shift/pkg/flowdoc"
	"github.com/aaron-au/shift/runner/internal/hubclient"
	"github.com/aaron-au/shift/runner/internal/service"
	"github.com/aaron-au/shift/runner/internal/task"
)

// Wire constants, mirroring gateway/internal/ingress. They are duplicated
// rather than shared because the gateway is deliberately a separate module
// with no dependency on the runner (depguard enforces it): this is the one
// component that may live in a DMZ, and what it can import decides what code
// can end up there. The cost is these seven strings.
const (
	pollPath    = "/api/v1/gw/poll"
	deliverPath = "/api/v1/gw/deliver/"

	hdrRequestID = "X-Shift-Request-Id"
	hdrFlow      = "X-Shift-Flow"
	// The gateway's stamped facts about the caller (ADR-0038 §4b) and, for a
	// status read, what to answer (ADR-0042 §3). Separate modules by design
	// (depguard), so nothing but these names enforces the contract.
	hdrOp         = "X-Shift-Op"
	hdrTask       = "X-Shift-Task"
	hdrRoute      = "X-Shift-Route"
	hdrPrincipal  = "X-Shift-Principal"
	hdrPublicBase = "X-Shift-Public-Base"
	hdrQuery      = "X-Shift-Query"
	// opStatus is the only op a runner answers itself rather than by running
	// a flow.
	opStatus  = "status"
	hdrStatus = "X-Shift-Status"
	fwdPrefix = "X-Shift-Fwd-"
)

// maxResponseBody bounds what one execution may return to a caller. Matches
// the synchronous HTTP intake: request-reply is for answers, not for bulk
// transfer, and an unbounded response is a memory-exhaustion primitive on a
// path anyone on the internet can trigger.
const maxResponseBody = 8 << 20

// FlowLookup resolves a flow name to its document. The gateway sends a NAME,
// never a document — a DMZ component that could hand a runner arbitrary code
// to execute would be a remote-code-execution primitive with extra steps.
type FlowLookup func(name string) (*flowdoc.Document, bool)

// Options configure the intake.
type Options struct {
	// Addrs are the gateway control-listener base URLs to poll. One loop runs
	// per address, concurrently. These are STATIC: configured locally and
	// never withdrawn. Discover below is the normal source in a deployment.
	Addrs []string
	// Discover asks the hub which gateways this runner should be polling
	// (ADR-0038 §4). Optional; without it the address list is whatever Addrs
	// was at startup.
	//
	// The hub answers for the identity the runner's credential proves, so
	// adding a gateway or relabelling a runner takes effect here without
	// touching the runner — which is the point: an address list baked into
	// every deployment means a new DMZ host is a fleet-wide redeploy.
	//
	// A failure is NOT an empty list. The hub being unreachable says nothing
	// about which gateways exist, so a failed pass keeps the current set and
	// retries; only a successful answer may withdraw a gateway.
	Discover func(ctx context.Context) ([]string, error)
	// DiscoverEvery is how often to re-ask (default 60s).
	DiscoverEvery time.Duration
	// NB: there is deliberately NO Labels field. A runner used to state its own
	// placement on every poll, which meant a compromised or misconfigured one
	// could claim `environment: production` and be handed production traffic.
	// Labels now come from the hub's roster, keyed by the identity this runner
	// PROVES with its client certificate (ADR-0041 §3). A runner tells a
	// gateway nothing about itself.

	// Service executes the flows.
	Service *service.Service
	// Lookup resolves flow names to documents (hub-synced webhook registry).
	Lookup FlowLookup
	// Bind resolves {"$secret":...} references before execution. Optional;
	// without it a flow that uses a credential fails with a clear error
	// rather than handing a connector a reference object.
	Bind func(ctx context.Context, doc *flowdoc.Document) (*flowdoc.Document, []string, error)
	// Token is the shared secret the gateway's control listener requires. It
	// is sent as a bearer credential on both calls. Empty is valid only
	// against an unauthenticated (loopback-bound) gateway.
	//
	// SUPERSEDED by TLS below (ADR-0041). A shared secret proves only that the
	// caller knows a string: it cannot say WHICH runner is calling, so nothing
	// can be attributed or revoked per runner, and it gives the runner no way
	// to tell a real gateway from anything that answered on that address.
	Token string
	// TLS carries this runner's client certificate and the control-plane CA.
	// When set, the runner presents its identity on every call (which is what
	// the gateway resolves labels by) and verifies the gateway in return.
	TLS *tls.Config
	// PollWait is the long-poll window (default 30s).
	PollWait time.Duration
	// PollConcurrency is how many polls this runner parks PER GATEWAY
	// (default 16). It bounds how many inbound requests it can have in flight
	// from one gateway; the real ceiling remains the resource governor, which
	// every poll re-checks before parking.
	//
	// It is a parked-connection count, not a task cap: an idle poll costs a
	// goroutine stack and a socket, so this can be generous. One is not — see
	// Run.
	PollConcurrency int
	// HeadroomPoll is how often a poll re-checks capacity while the runner is
	// full (default 250ms), matching the hub lease loop.
	HeadroomPoll time.Duration
	// Client is the HTTP client used for both calls. Optional.
	Client *http.Client
	// Log receives operational events. Optional.
	Log *slog.Logger
	// Status records and reads caller-facing execution status (ADR-0042 §3).
	// Optional: a runner with no hub still serves async requests, it simply
	// hands out no status URL — there would be nowhere to read it from.
	Status StatusReader
	// OnDone, when set, receives each completed task so execution metadata
	// can be reported to the hub — OFF the response path, since the caller is
	// already waiting and nobody is waiting on the metadata.
	OnDone func(t task.Task)
}

// Loop is the runner's gateway intake.
type Loop struct {
	opts Options
	log  *slog.Logger
	cl   *http.Client
	// schemas compiles each distinct input schema once (ADR-0042 §4c).
	schemas schemaCache
}

// New builds an intake. It does not dial anything until Run.
func New(opts Options) *Loop {
	if opts.PollWait <= 0 {
		opts.PollWait = 30 * time.Second
	}
	if opts.PollConcurrency <= 0 {
		opts.PollConcurrency = 16
	}
	if opts.HeadroomPoll <= 0 {
		opts.HeadroomPoll = 250 * time.Millisecond
	}
	if opts.DiscoverEvery <= 0 {
		opts.DiscoverEvery = time.Minute
	}
	log := opts.Log
	if log == nil {
		log = slog.Default()
	}
	cl := opts.Client
	if cl == nil {
		// The connection pool MUST be sized to the poll concurrency.
		//
		// http.DefaultTransport keeps 2 idle connections per host. With N
		// parked polls plus their deliveries against one gateway, everything
		// above that is torn down after each request and lands in TIME_WAIT —
		// and on a busy runner that exhausts the ephemeral port range
		// outright: "dial tcp: connect: can't assign requested address",
		// every poll failing, and callers waiting out the full delivery
		// timeout for responses the runner cannot send.
		//
		// Observed at 16 polls against a single gateway. Two connections per
		// poll slot (one parked, one delivering) plus headroom.
		perHost := opts.PollConcurrency*2 + 8
		tr := &http.Transport{
			Proxy:               http.ProxyFromEnvironment,
			MaxIdleConnsPerHost: perHost,
			MaxConnsPerHost:     0, // unbounded: the poll count is the real bound
			// Sized for several gateways when the set is discovered, because
			// it is not known here: an idle-connection cap below what the
			// polls actually hold is the port-exhaustion shape above.
			MaxIdleConns:    perHost * max(len(opts.Addrs), 4),
			IdleConnTimeout: 90 * time.Second,
			TLSClientConfig: opts.TLS,
		}
		// ForceAttemptHTTP2 matters more than it looks: over h2, every parked
		// poll to one gateway shares a SINGLE connection instead of holding its
		// own socket, which is the pressure that caused the port exhaustion
		// this pool is sized around (docs/bench-gateway.md).
		tr.ForceAttemptHTTP2 = true
		cl = &http.Client{
			// No client-side timeout: a poll legitimately blocks for the whole
			// window and an execution may legitimately take longer still. The
			// request context bounds both, which is the honest place for it —
			// a blanket Timeout here would abort long-running work as if it
			// had failed.
			Transport: tr,
		}
	}
	return &Loop{opts: opts, log: log, cl: cl}
}

// Run polls every configured gateway until ctx ends.
//
// Concurrency matters more here than it looks. A runner holding ONE poll per
// gateway can serve exactly ONE inbound request at a time — while it executes,
// it is not parked, so every other caller gets a 503 from a runner that had
// ample capacity. Measured at 1 poll: ~800 req/s and an 82% error rate at 8
// concurrent callers, against a governor sized for far more.
//
// So each gateway gets several parked polls, and the real limit stays the
// resource governor (ADR-0005) rather than this number: a poll only re-parks
// while there is admission headroom, so the fleet self-regulates exactly as
// the hub lease loop does.
// The address set is LIVE when Discover is set: gateways come and go while the
// runner runs, and each one gets its own cancellable group of polls. Withdrawing
// one cancels its polls rather than letting them keep parking against a gateway
// the hub has removed — a decommissioned DMZ host that a runner kept talking to
// would still be able to hand it work.
func (l *Loop) Run(ctx context.Context) {
	var wg sync.WaitGroup
	// Static addresses are started once and never withdrawn: they were
	// configured locally, so no remote answer should be able to turn them off.
	running := map[string]context.CancelFunc{}
	static := map[string]bool{}
	start := func(addr string) {
		gctx, cancel := context.WithCancel(ctx)
		running[addr] = cancel
		for range l.opts.PollConcurrency {
			// The deferred cancel releases the group's context once its polls
			// have stopped; withdrawing the gateway is what stops them, and
			// cancelling twice is harmless.
			wg.Go(func() { defer cancel(); l.pollLoop(gctx, addr) })
		}
	}
	for _, addr := range l.opts.Addrs {
		addr = strings.TrimSuffix(addr, "/")
		if running[addr] == nil {
			static[addr] = true
			start(addr)
		}
	}
	if l.opts.Discover == nil {
		wg.Wait()
		return
	}
	for ctx.Err() == nil {
		addrs, err := l.opts.Discover(ctx)
		switch {
		case err != nil:
			// Keep serving what we already poll. The hub is the control
			// plane, and this is the data plane — an unreachable hub must not
			// take inbound traffic down with it.
			if ctx.Err() == nil {
				l.log.Warn("gateway discovery failed; keeping the current gateways",
					"event", "runner.gateway.discovery_failed",
					"gateways", len(running), "error", err)
			}
		default:
			l.reconcile(addrs, running, static, start)
		}
		sleep(ctx, l.discoverEvery())
	}
	for _, cancel := range running {
		cancel()
	}
	wg.Wait()
}

// reconcile brings the set of polled gateways in line with the hub's answer.
func (l *Loop) reconcile(addrs []string, running map[string]context.CancelFunc, static map[string]bool, start func(string)) {
	want := make(map[string]bool, len(addrs))
	for _, addr := range addrs {
		want[strings.TrimSuffix(addr, "/")] = true
	}
	for addr, cancel := range running {
		if want[addr] || static[addr] {
			continue
		}
		l.log.Info("gateway withdrawn", "event", "runner.gateway.withdrawn", "gateway", addr)
		cancel()
		delete(running, addr)
	}
	for addr := range want {
		if running[addr] != nil {
			continue
		}
		l.log.Info("gateway added", "event", "runner.gateway.added", "gateway", addr)
		start(addr)
	}
}

func (l *Loop) discoverEvery() time.Duration { return l.opts.DiscoverEvery }

// pollLoop holds one of a gateway's long-polls, re-parking after every outcome.
func (l *Loop) pollLoop(ctx context.Context, addr string) {
	backoff := time.Second
	for ctx.Err() == nil {
		// Capacity-gated, exactly like the hub lease loop: parking without
		// headroom would accept work this runner cannot start, converting a
		// clean 503 (which another runner can answer) into a queued request
		// stuck behind admission on this one.
		if !l.headroom() {
			sleep(ctx, l.headroomPoll())
			continue
		}
		req, err := l.poll(ctx, addr)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			// A gateway being down is normal (rolling restart, DMZ host
			// replaced) and must not take the runner with it — the other
			// gateways and the hub lease loop keep working.
			l.log.Warn("gateway poll failed", "event", "runner.gateway.poll_failed", "gateway", addr, "error", err, "retry_in", backoff)
			sleep(ctx, backoff)
			backoff = min(backoff*2, 30*time.Second)
			continue
		}
		backoff = time.Second
		if req == nil {
			continue // empty window: re-poll immediately
		}
		l.serve(ctx, addr, req)
	}
}

// headroom reports whether the governor can admit another task. Polling past
// it would accept work this runner cannot start — turning a 503 that another
// runner could have answered into a request queued behind admission here.
//
// A nil Service means no gating (the wire-level tests construct one that way);
// a runner built by runnerd always has one.
func (l *Loop) headroom() bool {
	if l.opts.Service == nil {
		return true
	}
	st := l.opts.Service.Status()
	return st.Governor.Used+st.TaskCost <= st.Governor.Budget
}

func (l *Loop) headroomPoll() time.Duration { return l.opts.HeadroomPoll }

// inbound is one request handed over by a gateway.
type inbound struct {
	id      string
	flow    string
	headers http.Header // caller headers plus the gateway's stamped identity
	body    []byte
	// query is the caller's raw query string, parsed. A status read on an
	// anonymous route carries its capability token here.
	query url.Values
}

// op returns what the gateway asked for: "" for ordinary work, opStatus for a
// status read.
func (in *inbound) op() string { return in.headers.Get(hdrOp) }

// poll parks against one gateway. It returns (nil, nil) on an empty window.
func (l *Loop) poll(ctx context.Context, addr string) (*inbound, error) {
	body, err := encodePoll(l.opts.PollWait)
	if err != nil {
		return nil, err
	}
	// The request deadline is the poll window plus slack: without it a
	// gateway that accepts the connection and then wedges would park this
	// goroutine forever, and the runner would quietly stop serving that
	// gateway with nothing to show for it.
	pctx, cancel := context.WithTimeout(ctx, l.opts.PollWait+30*time.Second)
	defer cancel()

	hreq, err := http.NewRequestWithContext(pctx, http.MethodPost, addr+pollPath, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	hreq.Header.Set("Content-Type", "application/json")
	l.auth(hreq)
	resp, err := l.cl.Do(hreq)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	switch resp.StatusCode {
	case http.StatusNoContent:
		return nil, nil // nothing arrived; normal
	case http.StatusOK:
	case http.StatusUnauthorized:
		// Worth its own arm: a wrong or missing shared secret otherwise reads
		// as a generic poll failure and gets retried forever at the backoff
		// ceiling, with nothing in the log saying why.
		return nil, errors.New("poll: unauthorized — gateway rejected the control credential (SHIFT_GATEWAY_TOKEN)")
	default:
		return nil, fmt.Errorf("poll: status %d", resp.StatusCode)
	}

	// The body is the caller's payload. It is read fully here because the
	// engine's @webhook source binds a byte slice; a streaming hand-off
	// would need the deliver POST already open, which is a larger change to
	// the synchronous execution path (tracked with the rest of ADR-0038).
	payload, err := io.ReadAll(http.MaxBytesReader(nil, resp.Body, maxResponseBody))
	if err != nil {
		return nil, fmt.Errorf("poll: read body: %w", err)
	}
	in := &inbound{
		id:      resp.Header.Get(hdrRequestID),
		flow:    resp.Header.Get(hdrFlow),
		headers: unprefix(resp.Header),
		body:    payload,
	}
	if q := in.headers.Get(hdrQuery); q != "" {
		// A malformed query is not worth failing a request over; the values
		// that did parse are what the checks below use.
		in.query, _ = url.ParseQuery(q)
	}
	if in.id == "" || in.flow == "" {
		return nil, errors.New("poll: gateway sent work with no request id or flow")
	}
	return in, nil
}

// serve executes one inbound request and delivers the result. Every exit path
// delivers SOMETHING: a caller blocked on the gateway would otherwise wait out
// the full delivery timeout for an answer that was never coming.
func (l *Loop) serve(ctx context.Context, addr string, in *inbound) {
	status, body, ctype := l.execute(ctx, in)
	if err := l.deliver(ctx, addr, in.id, status, ctype, body); err != nil {
		l.log.Warn("gateway deliver failed", "event", "runner.gateway.deliver_failed", "gateway", addr, "request", in.id, "error", err)
	}
}

func (l *Loop) execute(ctx context.Context, in *inbound) (status int, body []byte, ctype string) {
	// A status read is answered HERE rather than by running the flow: a status
	// read that executed a flow would be a side effect on a GET.
	if in.op() == opStatus {
		return l.serveStatus(ctx, in)
	}

	doc, ok := l.opts.Lookup(in.flow)
	if !ok {
		// The gateway's configuration names a flow this runner does not have.
		// 404 rather than 500: the request was well-formed, we are the wrong
		// place for it, and saying so lets the gateway's operator see the
		// drift instead of chasing an opaque failure.
		return http.StatusNotFound, []byte(`{"error":"unknown flow"}`), "application/json"
	}

	// Verify BEFORE anything else runs (ADR-0042 §4). A malformed payload is
	// the one failure the caller can fix, and this is the only moment they
	// still hold the data.
	if res := l.verify(doc, in.body); !res.ok() {
		switch {
		case res.TooLarge:
			l.log.Warn("request too large to verify", "event", "request.too_large", "flow", in.flow, "request", in.id,
				"bytes", len(in.body), "limit", res.Limit)
			return http.StatusRequestEntityTooLarge,
				problem(http.StatusRequestEntityTooLarge, "input_too_large",
					"the request is larger than this flow verifies", nil), "application/json"
		case res.Err != nil:
			// The parse error names a column, which is useful and reveals no
			// payload — it is a position, not content.
			return http.StatusBadRequest,
				problem(http.StatusBadRequest, "input_invalid",
					"the request body is not valid JSON: "+res.Err.Error(), nil), "application/json"
		default:
			return http.StatusBadRequest,
				problem(http.StatusBadRequest, "input_invalid",
					"the request does not satisfy this flow's input schema", res.Violations), "application/json"
		}
	}

	var secretValues []string
	if l.opts.Bind != nil {
		bound, vals, err := l.opts.Bind(ctx, doc)
		if err != nil {
			l.log.Error("secret resolution failed", "event", "request.secrets_failed", "flow", in.flow, "request", in.id, "error", err)
			return http.StatusInternalServerError, []byte(`{"error":"secret resolution failed"}`), "application/json"
		}
		doc, secretValues = bound, vals
	}

	// ASYNC IS THE DEFAULT (ADR-0042 §1). A flow terminating at @response has
	// something to say to the caller and keeps the exchange open; anything else
	// is acknowledged immediately and executes with the caller already gone.
	//
	// The gateway does not know which mode it is serving and must not: it hands
	// over work and streams back whatever comes. Only the TIMING of the delivery
	// changes, which is why the exchange lifetime collapses from "flow duration"
	// to "validate and accept" without the gateway gaining a single code path.
	if !doc.TerminatesAtResponse() {
		return l.accept(ctx, doc, in, secretValues)
	}

	out := &boundedBuffer{limit: maxResponseBody}
	t, err := l.opts.Service.RunSync(doc, service.SubmitOpts{
		WebhookBody:  in.body,
		Response:     out,
		SecretValues: secretValues,
		// The caller's headers, including the gateway's stamped principal, are
		// deliberately NOT injected into the flow document yet: doing so needs
		// a flow-variable model the engine does not have (ADR-0031 open). They
		// are logged so a request is traceable end to end in the meantime.
	})
	if l.opts.OnDone != nil && err == nil {
		go l.opts.OnDone(t)
	}
	if err != nil {
		l.log.Error("gateway flow failed", "event", "request.failed", "flow", in.flow, "request", in.id, "error", err)
		return http.StatusUnprocessableEntity, []byte(`{"error":"execution failed"}`), "application/json"
	}
	if t.State == task.StateFailed {
		// The error text is redacted by the service, but it is still derived
		// from payload and internals — it goes to the runner's log, never to
		// an internet caller.
		l.log.Error("gateway flow failed", "event", "request.failed", "flow", in.flow, "request", in.id, "task", t.ID, "error", t.Error)
		return http.StatusUnprocessableEntity, []byte(`{"error":"execution failed"}`), "application/json"
	}
	return http.StatusOK, out.buf.Bytes(), "application/x-ndjson"
}

// accept answers 202 and lets the flow run without the caller (ADR-0042 §1).
//
// The body must already be fully read by this point, and it is: the @webhook
// source binds a byte slice, so the request is in memory before we get here.
// That ordering is not incidental — delivering a response ENDS the exchange, so
// anything unread at that moment is gone (ADR-0042 §5).
func (l *Loop) accept(ctx context.Context, doc *flowdoc.Document, in *inbound, secretValues []string) (int, []byte, string) {
	// The id is minted HERE (ADR-0042 §3a). Uniqueness comes from randomness
	// rather than from a sequence the hub owns, which is what lets it be quoted
	// in the 202 without coordinating with anyone. Minting is not owning: the
	// row lives at the hub, so any runner can serve the status read and this
	// one may be long gone by then.
	// Fire-and-forget (ADR-0042 §3d) skips all of it: no id, no hub round trip
	// on the accept path, no row to retain. A million status rows nobody reads
	// is bookkeeping for its own sake, and quoting a task id that cannot be
	// looked up would be worse than quoting none.
	var taskID, statusURL string
	if doc.EffectiveAck() != flowdoc.AckNone {
		var err error
		if taskID, err = newTaskID(); err != nil {
			l.log.Error("minting a task id failed", "event", "request.accept_failed", "flow", in.flow, "error", err)
			return http.StatusInternalServerError,
				problem(http.StatusInternalServerError, "runner_error", "could not accept", nil), "application/json"
		}
		if statusURL, err = l.recordAccept(ctx, doc, in, taskID); err != nil {
			// Durable accept (ADR-0042 §6): the record comes BEFORE the promise,
			// so a status URL never 404s and an accepted task never vanishes
			// without trace. Failing here is honest — the caller has been
			// promised nothing.
			l.log.Error("recording the accept failed", "event", "request.accept_failed", "flow", in.flow, "request", in.id, "error", err)
			return http.StatusServiceUnavailable,
				problem(http.StatusServiceUnavailable, "runner_unavailable",
					"this runner cannot accept work right now", nil), "application/json"
		}
	}

	_, err := l.opts.Service.SubmitWith(doc, service.SubmitOpts{
		WebhookBody:  in.body,
		SecretValues: secretValues,
		// The completion hooks belong HERE rather than on the response path:
		// the caller is already gone, and nobody is waiting on the metadata.
		OnDone: func(t task.Task) {
			l.finishStatus(taskID, t)
			if l.opts.OnDone != nil {
				l.opts.OnDone(t)
			}
		},
	})
	if err != nil {
		// Admission refused it (draining, or the document is unrunnable). The
		// status row exists and would otherwise sit at "accepted" forever, so
		// it is closed out here.
		l.log.Error("gateway flow rejected at admission", "event", "request.rejected", "flow", in.flow, "request", in.id, "error", err)
		l.failStatus(taskID, "admission", "runner_unavailable", err.Error())
		return http.StatusServiceUnavailable,
			problem(http.StatusServiceUnavailable, "runner_unavailable",
				"this runner cannot accept work right now", nil), "application/json"
	}

	body, err := json.Marshal(accepted{
		Task:       taskID,
		Flow:       doc.Name,
		Status:     "accepted",
		StatusURL:  statusURL,
		AcceptedAt: time.Now().UTC().Format(time.RFC3339),
	})
	if err != nil {
		// Unreachable with these field types, but a marshalling failure must
		// not lose a task that is already running.
		l.log.Error("encoding the accept response failed", "event", "request.encode_failed", "flow", in.flow, "task", taskID, "error", err)
		return http.StatusAccepted, []byte(`{"status":"accepted"}`), "application/json"
	}
	return http.StatusAccepted, body, "application/json"
}

// recordAccept writes the status row and returns the URL the caller polls.
//
// A runner with no hub records nothing and returns no URL: there would be
// nowhere to read it from, and advertising a URL that 404s is worse than
// omitting the field.
func (l *Loop) recordAccept(ctx context.Context, doc *flowdoc.Document, in *inbound, taskID string) (string, error) {
	if l.opts.Status == nil {
		return "", nil
	}
	route := in.headers.Get(hdrRoute)
	principal := in.headers.Get(hdrPrincipal)

	// An anonymous route stamps one principal for every caller, so comparing
	// principals authorises nothing. The status URL becomes a capability URL
	// instead (ADR-0042 §3b) — whoever holds it holds the read.
	var token, digest string
	if principal == "" || principal == anonymousPrincipal {
		var err error
		if token, err = newCapabilityToken(); err != nil {
			return "", err
		}
		sum := sha256.Sum256([]byte(token))
		digest = hex.EncodeToString(sum[:])
	}

	// A background context with its own bound, not ctx: this write is what
	// makes the accept durable, and abandoning it because the caller's request
	// context is closing would defeat the point.
	rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	if err := l.opts.Status.AcceptExecution(rctx, hubclient.AcceptedExecution{
		ID: taskID, FlowName: doc.Name, Route: route, Principal: principal, TokenSHA256: digest,
	}); err != nil {
		return "", err
	}
	return statusURL(in.headers.Get(hdrPublicBase), route, taskID, token), nil
}

// finishStatus closes out a status row when the task reaches a terminal state.
func (l *Loop) finishStatus(taskID string, t task.Task) {
	// An empty id means fire-and-forget (ADR-0042 §3d): no row was written, so
	// there is nothing to close out. The execution report and metering happen
	// elsewhere and are unaffected.
	if l.opts.Status == nil || taskID == "" {
		return
	}
	out := hubclient.ExecutionOutcome{
		State:      string(t.State),
		RecordsIn:  t.RecordsIn,
		RecordsOut: t.RecordsOut,
	}
	if t.State == task.StateFailed {
		// The step and the class of failure — never record content, since an
		// internet caller reads this (ADR-0031, ADR-0042 §3). t.Error is
		// already redacted by the service, and HandlerStep names the failing
		// step when the flow declared an error handler.
		out.ErrorStep, out.ErrorCode, out.Error = t.HandlerStep, "execution_failed", t.Error
	}
	if t.Started != nil {
		s := t.Started.UTC()
		out.StartedAt = &s
	}
	if t.Finished != nil {
		f := t.Finished.UTC()
		out.FinishedAt = &f
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := l.opts.Status.FinishExecution(ctx, taskID, out); err != nil {
		// The work is done either way; the row's TTL bounds the consequence.
		l.log.Warn("finalising execution status failed", "event", "task.status_finalise_failed", "task", taskID, "error", err)
	}
}

// failStatus closes out a row for work that never started.
func (l *Loop) failStatus(taskID, step, code, msg string) {
	if l.opts.Status == nil || taskID == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := l.opts.Status.FinishExecution(ctx, taskID, hubclient.ExecutionOutcome{
		State: "failed", ErrorStep: step, ErrorCode: code, Error: msg,
	}); err != nil {
		l.log.Warn("closing an unstarted execution status failed", "event", "task.status_finalise_failed", "task", taskID, "error", err)
	}
}

// accepted is the 202 body (ADR-0042 §2).
type accepted struct {
	// Task is omitted for a fire-and-forget route (ADR-0042 §3d), where no id
	// was minted. An id the caller cannot look up would be a promise of
	// something that does not exist.
	Task string `json:"task,omitempty"`
	Flow string `json:"flow"`
	// StatusURL is where the caller polls. Omitted when this runner has no hub
	// to record against: advertising a URL that 404s is worse than saying
	// nothing, and the field's absence is the honest signal.
	StatusURL  string `json:"status_url,omitempty"`
	Status     string `json:"status"`
	AcceptedAt string `json:"accepted_at"`
}

func (l *Loop) deliver(ctx context.Context, addr, id string, status int, ctype string, body []byte) error {
	// A background context with its own deadline, not ctx: on shutdown the
	// caller is still blocked on the gateway, and abandoning the delivery
	// would turn a completed execution into a 504 for them.
	dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(dctx, http.MethodPost, addr+deliverPath+id, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", ctype)
	req.Header.Set(hdrStatus, strconv.Itoa(status))
	l.auth(req)
	resp, err := l.cl.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body)
	switch resp.StatusCode {
	case http.StatusNoContent:
		return nil
	case http.StatusGone:
		// The caller gave up. Expected under load or on a slow flow, and
		// nothing to retry — the work is simply wasted.
		l.log.Info("caller gave up before the flow finished", "event", "request.caller_gone", "request", id)
		return nil
	default:
		return fmt.Errorf("deliver: status %d", resp.StatusCode)
	}
}

// auth attaches the control-listener credential, when one is configured.
func (l *Loop) auth(r *http.Request) {
	if l.opts.Token != "" {
		r.Header.Set("Authorization", "Bearer "+l.opts.Token)
	}
}

// unprefix strips the gateway's forward prefix, restoring the caller's
// original header names (plus the gateway's stamped X-Shift-* identity).
func unprefix(h http.Header) http.Header {
	out := make(http.Header, len(h))
	for k, vs := range h {
		ck := http.CanonicalHeaderKey(k)
		if !strings.HasPrefix(ck, fwdPrefix) {
			continue
		}
		out[http.CanonicalHeaderKey(strings.TrimPrefix(ck, fwdPrefix))] = vs
	}
	return out
}

// boundedBuffer accumulates up to limit bytes and errors past it, so an
// oversized response surfaces as a task failure rather than an unbounded
// allocation.
type boundedBuffer struct {
	buf   bytes.Buffer
	limit int
}

func (b *boundedBuffer) Write(p []byte) (int, error) {
	if b.buf.Len()+len(p) > b.limit {
		return 0, fmt.Errorf("response body exceeds %d bytes", b.limit)
	}
	return b.buf.Write(p)
}

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
