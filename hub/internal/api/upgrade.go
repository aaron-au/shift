package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/aaron-au/shift/hub/internal/store"
	"github.com/aaron-au/shift/pkg/flowdoc"
)

// Bulk connector upgrade — locate, test, publish-all (ADR-0047 §9).
//
// The friction this removes is real and the reason it matters is not
// convenience: a platform where upgrading means republishing forty flows by
// hand is one where people stop upgrading, and then a connector security fix
// ships to a registry nobody moves to. Bulk is the mechanism that makes the
// support window (§5) something an operator can actually satisfy.
//
// But a mass republish is a mass change against live data, so it is three
// steps and never one:
//
//	1. locate   GET  …/upgrade?to=      — who is behind, and what crossing costs
//	2. test     POST …/upgrade/test     — stage drafts, run them on the test tier
//	3. publish  POST …/upgrade/publish  — publish the drafts that passed
//
// Two properties make the staging load-bearing rather than ceremonial. The
// drafts step 3 publishes are the EXACT documents step 2 tested — not
// equivalent ones rebuilt from the same inputs — so a passing test is a
// statement about what ships. And the target version is fixed once, at stage
// time, so a release landing between steps cannot retarget a batch somebody
// already read a report about.

// upgradeCandidate is one flow in the locate report.
type upgradeCandidate struct {
	Flow    string   `json:"flow"`
	Version int      `json:"version"`
	Steps   []string `json:"steps"`
	Pinned  string   `json:"pinned"`
	Behind  int      `json:"behind"`
	// Summary folds the WHOLE span, not the last hop: somebody moving three
	// releases crosses three sets of changes they never read, and "what will
	// this do to me?" is the question they actually have (§6).
	Summary  string   `json:"summary"`
	Breaking []string `json:"breaking,omitempty"`
}

// locateUpgrades: GET /api/v1/connectors/{name}/upgrade?to=<version>
//
// Reports every PUBLISHED flow version pinning this connector below the
// target, with the folded compatibility diff per flow. Read-only by
// construction — §9 step 1 is a report, and a report that changed something
// would be step 3 wearing a disguise.
func (a *api) locateUpgrades(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	order := a.orderedVersions(r.Context(), name)
	if len(order) == 0 {
		writeErr(w, http.StatusNotFound, fmt.Errorf("connector %q has no registered versions", name))
		return
	}
	target := r.URL.Query().Get("to")
	if target == "" {
		target = order[0].Version // newest, the overwhelmingly common intent
	}
	ti := indexOfVersion(order, target)
	if ti < 0 {
		writeErr(w, http.StatusUnprocessableEntity,
			fmt.Errorf("connector %q has no version %q to upgrade to", name, target))
		return
	}

	pinned, err := a.st.FlowsPinningConnector(r.Context(), name)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	out := []upgradeCandidate{}
	for _, p := range pinned {
		// Only the flow's CURRENT published version is a candidate. Its
		// predecessor is retained so a rollback has somewhere to land
		// (ADR-0047 §2) — republishing it forward would destroy exactly that,
		// and would do it in a batch nobody was reading closely.
		if !p.Current {
			continue
		}
		pi := indexOfVersion(order, p.Pinned)
		// pi < 0: a build the registry cannot place — provisioned outside the
		// registry, or already collected. "Unknown" must not read as "behind",
		// because an upgrade report is a list somebody acts on in bulk.
		// pi <= ti: already at or ahead of the target. Not a candidate, and
		// never silently moved BACKWARD by a bulk operation.
		if pi < 0 || pi <= ti {
			continue
		}
		c := upgradeCandidate{
			Flow: p.Flow, Version: p.Version, Steps: p.Steps,
			Pinned: p.Pinned, Behind: pi - ti,
		}
		span := order[ti:pi]
		var behaviour, undeclared []string
		for _, v := range span {
			switch v.Compat {
			case store.CompatBreaking:
				c.Breaking = append(c.Breaking, v.Version)
			case store.CompatBehaviour:
				behaviour = append(behaviour, v.Version)
			case store.CompatCompatible:
			default:
				undeclared = append(undeclared, v.Version)
			}
		}
		c.Summary = describeSpan(len(span), c.Breaking, behaviour, undeclared)
		out = append(out, c)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"connector": name, "target": target, "flows": out,
	})
}

// stageUpgradeTest: POST /api/v1/connectors/{name}/upgrade/test
//
// Body {"to":"v0.5.0","flows":["orders",…]}. For each flow it copies the
// published document, moves this connector's pins to the target, stores that
// as a DRAFT, and queues a test-tier execution of the draft (ADR-0048).
//
// Staging the draft here rather than at publish is the point. Testing the
// current published version would test the thing nobody is changing; building
// the draft twice — once to test, once to publish — would test a document that
// merely resembles what ships. One draft, tested and then published, is the
// only arrangement where the gate means anything.
func (a *api) stageUpgradeTest(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	var req struct {
		To    string   `json:"to"`
		Flows []string `json:"flows"`
	}
	if err := readBody(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if len(req.Flows) == 0 {
		writeErr(w, http.StatusUnprocessableEntity,
			errors.New("flows is required: a bulk upgrade names the flows it moves, it does not select them for you"))
		return
	}
	order := a.orderedVersions(r.Context(), name)
	if req.To == "" && len(order) > 0 {
		req.To = order[0].Version
	}
	if indexOfVersion(order, req.To) < 0 {
		writeErr(w, http.StatusUnprocessableEntity,
			fmt.Errorf("connector %q has no version %q to upgrade to", name, req.To))
		return
	}

	staged := make([]store.StagedFlow, 0, len(req.Flows))
	for _, flowName := range req.Flows {
		s, err := a.stageOne(r, name, req.To, flowName)
		if err != nil {
			// One bad flow aborts the batch rather than producing a partial
			// one. A batch missing a flow would pass its gate and then report
			// success having left that flow behind — the silent-partial-change
			// shape this ADR exists to remove.
			writeLookupErr(w, fmt.Errorf("flow %q: %w", flowName, err))
			return
		}
		staged = append(staged, s)
	}
	batch, err := a.st.CreateUpgradeBatch(r.Context(), name, req.To, actor(r), staged)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	_ = a.st.Audit(r.Context(), actor(r), "connector.upgrade-staged", name, map[string]any{
		"batch": batch, "target": req.To, "flows": len(staged),
	})
	slog.Info("bulk connector upgrade staged",
		"event", "hub.connector.upgrade_staged", "connector", name,
		"target", req.To, "batch", batch, "flows", len(staged))

	out, err := a.st.GetUpgradeBatch(r.Context(), batch)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusAccepted, out)
}

// stageOne builds and queues one flow's upgrade draft.
func (a *api) stageOne(r *http.Request, connector, target, flowName string) (store.StagedFlow, error) {
	var s store.StagedFlow
	_, raw, err := a.st.GetFlow(r.Context(), flowName, 0) // 0 = the published version
	if err != nil {
		return s, err
	}
	doc, err := flowdoc.Parse(raw)
	if err != nil {
		return s, fmt.Errorf("published document does not parse: %w", err)
	}
	from := ""
	for _, p := range doc.ConnectorPins() {
		if p.Connector == connector {
			from = p.Version
			break
		}
	}
	moved := doc.RepinConnector(connector, target)
	if len(moved) == 0 {
		return s, fmt.Errorf("no step uses %s at a different version", connector)
	}
	body, err := json.Marshal(doc)
	if err != nil {
		return s, err
	}
	draft, err := a.st.DeployFlow(r.Context(), flowName, body)
	if err != nil {
		return s, err
	}
	// A test run of a specific DRAFT version — not of the flow's published
	// version, which is the one this exists to replace.
	task, err := a.st.EnqueueTest(r.Context(), flowName, draft, "", 1)
	if err != nil {
		// The draft stands; the batch records no task, and publish-all will
		// refuse it as untested. Losing the queue is not a reason to lose the
		// staged work, and it is emphatically not a reason to let it through.
		slog.Warn("upgrade draft staged but its test run could not be queued",
			"event", "hub.connector.upgrade_test_unqueued", "flow", flowName, "draft", draft, "error", err)
	}
	return store.StagedFlow{Flow: flowName, From: from, Draft: draft, TaskID: task}, nil
}

// getUpgradeBatch: GET /api/v1/connectors/upgrades/{id} — the batch with each
// flow's live test-task state, which is how somebody watches step 2 finish.
func (a *api) getUpgradeBatch(w http.ResponseWriter, r *http.Request) {
	b, err := a.st.GetUpgradeBatch(r.Context(), r.PathValue("id"))
	if err != nil {
		writeLookupErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, b)
}

// listUpgradeBatches: GET /api/v1/connectors/upgrades — recent batches. This
// is the audit view §9 asks for: what moved, when, and by whom.
func (a *api) listUpgradeBatches(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	out, err := a.st.UpgradeBatches(r.Context(), limit)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"batches": out})
}

// publishUpgradeBatch: POST /api/v1/connectors/upgrades/{id}/publish
//
// Publishes every draft in the batch, as one audited action, with each flow's
// review notices recorded against it.
//
// It REFUSES unless every flow has passed its test run — 409, naming them.
// That refusal is the whole reason step 2 exists: a publish-all that ran
// regardless would make testing advice, and advice is what the one-button
// version this ADR rejects amounts to. "Not passed" includes still-running,
// because an absent result has proven nothing.
func (a *api) publishUpgradeBatch(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	b, err := a.st.GetUpgradeBatch(r.Context(), id)
	if err != nil {
		writeLookupErr(w, err)
		return
	}
	if b.Published != nil {
		writeErr(w, http.StatusConflict, store.ErrAlreadyPublished)
		return
	}
	untested, err := a.st.UntestedFlows(r.Context(), id)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	if len(untested) > 0 {
		writeJSON(w, http.StatusConflict, map[string]any{
			"error":    store.ErrUntested.Error(),
			"untested": untested,
			"detail": "Every flow in a batch must have a completed test run before it is published. " +
				"A flow still running has proven nothing yet; a flow that failed has proven the opposite.",
		})
		return
	}

	// Claim the batch BEFORE publishing anything. Two operators pressing the
	// button together would otherwise both pass the gate and both publish;
	// the loser gets 409 and no flow is published twice.
	if err := a.st.CloseUpgradeBatch(r.Context(), id); err != nil {
		if errors.Is(err, store.ErrAlreadyPublished) {
			writeErr(w, http.StatusConflict, err)
			return
		}
		writeErr(w, http.StatusInternalServerError, err)
		return
	}

	published := []string{}
	var failed []map[string]string
	for _, f := range b.Flows {
		// The SAME gates a single publish enforces (§4, §7). A bulk route that
		// skipped them would be the way to publish a flow the ordinary route
		// refuses — and the reason to reach for it would be that the ordinary
		// route refused. The batch only moves ONE connector forward, so a
		// refusal here is about some other step: another connector on the same
		// flow that is stale or past its end of life.
		if err := a.publishGate(r, f.Flow, f.Draft); err != nil {
			failed = append(failed, map[string]string{"flow": f.Flow, "error": err.Error()})
			continue
		}
		if err := a.st.PublishFlow(r.Context(), f.Flow, f.Draft); err != nil {
			// Report rather than abort: the flows already published are
			// published, and unwinding them would be a second unreviewed mass
			// change. The response names both halves so the remainder can be
			// finished by hand.
			failed = append(failed, map[string]string{"flow": f.Flow, "error": err.Error()})
			continue
		}
		notices, _ := json.Marshal(a.noticesWithCurrency(r, f.Flow, f.Draft))
		_ = a.st.RecordBatchPublish(r.Context(), id, f.Flow, notices)
		published = append(published, f.Flow)
	}
	_ = a.st.Audit(r.Context(), actor(r), "connector.upgrade-published", b.Connector, map[string]any{
		"batch": id, "target": b.Target, "published": published, "failed": len(failed),
	})
	slog.Info("bulk connector upgrade published",
		"event", "hub.connector.upgrade_published", "connector", b.Connector,
		"target", b.Target, "batch", id, "flows", len(published), "failed", len(failed))

	status := http.StatusOK
	if len(failed) > 0 {
		status = http.StatusMultiStatus
	}
	writeJSON(w, status, map[string]any{
		"batch": id, "connector": b.Connector, "target": b.Target,
		"published": published, "failed": failed,
	})
}
