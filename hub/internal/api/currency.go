package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/aaron-au/shift/hub/internal/store"
	"github.com/aaron-au/shift/pkg/flowdoc"
)

// Connector currency notices (ADR-0047 §5/§6).
//
// These live at the HUB rather than in flowdoc's check registry, and the
// division is not incidental. A flowdoc check answers a question the document
// answers on its own — "does this flow end at @response?" — and gives the same
// answer everywhere, which is what lets the runner, the CLI and the hub agree.
// "Is this pin three releases old?" is not that question: it is a fact about
// the REGISTRY at this moment, it changes without the flow changing, and it is
// different on two hubs holding different artifacts.
//
// Putting it in flowdoc would mean a check that needs a database, or a check
// that quietly does nothing wherever there is no registry.
//
// §5 is emphatic that age produces a NOTICE and never a refusal. A flow that
// pins an old build keeps executing; it simply stops receiving fixes. Refusing
// to run on grounds of age would be the arbitrary limit the platform doctrine
// rejects — and it would fire on a flow nobody had touched, which is the exact
// time-bomb ADR-0047 exists to avoid.

// supportWindow is how many releases back the patch policy covers: the current
// version and the one before it (n-1). Outside it, a pin is advised, not
// blocked.
const supportWindow = 2

// currencyNotices reports how far behind each pinned connector is.
//
// It is best-effort in the strict sense: a registry read that fails produces no
// notice rather than an error, because these ride on responses that are
// primarily about something else. A deploy that worked must not report as a
// failure because an advisory lookup did not.
func (a *api) currencyNotices(ctx context.Context, doc *flowdoc.Document) []flowdoc.Notice {
	if doc == nil {
		return nil
	}
	var out []flowdoc.Notice
	seen := map[string]bool{}
	for _, p := range doc.ConnectorPins() {
		if p.Version == "" || seen[p.Connector+"@"+p.Version] {
			continue // unpinned is the connector-pin check's story, not this one
		}
		seen[p.Connector+"@"+p.Version] = true
		if n, ok := a.currencyNotice(ctx, p); ok {
			out = append(out, n)
		}
	}
	return out
}

// currencyNotice builds the notice for one pin, or reports that there is
// nothing worth saying.
func (a *api) currencyNotice(ctx context.Context, p flowdoc.Pin) (flowdoc.Notice, bool) {
	order := a.orderedVersions(ctx, p.Connector)
	if len(order) == 0 {
		return flowdoc.Notice{}, false
	}
	behind := indexOfVersion(order, p.Version)
	// A pin the registry has never heard of is not "old" — it is a connector
	// provisioned some other way, or a version that has been collected. Saying
	// "0 versions behind" about it would be worse than saying nothing.
	if behind < 0 || behind < supportWindow {
		return flowdoc.Notice{}, false
	}

	// Fold the whole span, not the last hop. Somebody moving from v0.2.0 to
	// v0.5.0 crosses three releases they never read, and "what will change?"
	// is the question they actually have (ADR-0047 §6).
	span := order[:behind]
	var breaking, behaviour, undeclared []string
	for _, v := range span {
		switch v.Compat {
		case store.CompatBreaking:
			breaking = append(breaking, v.Version)
		case store.CompatBehaviour:
			behaviour = append(behaviour, v.Version)
		case store.CompatCompatible:
		default:
			undeclared = append(undeclared, v.Version)
		}
	}

	severity := flowdoc.SeverityInfo
	if len(breaking) > 0 || len(behaviour) > 0 {
		severity = flowdoc.SeverityWarn
	}
	detail := fmt.Sprintf("The %q step pins %s %s. The current version is %s, and the supported window is %s and newer — this pin still RUNS, it just stops receiving fixes. ",
		p.StepID, p.Connector, p.Version, order[0].Version, order[min(supportWindow, len(order))-1].Version)
	detail += describeSpan(len(span), breaking, behaviour, undeclared)
	detail += " Republish the flow to move it forward."

	return flowdoc.Notice{
		Code:     "connector-currency.behind",
		Severity: severity,
		Title:    fmt.Sprintf("%s is %d versions behind", p.Connector, behind),
		Detail:   detail,
		Step:     p.StepID,
		Docs:     "docs/adr/0047-connector-versioning-and-retention.md",
	}, true
}

// describeSpan says what crossing those versions would mean, in the terms
// somebody decides on: what breaks, what changes quietly, and what nobody
// declared. The last is worth naming — an undeclared version is not a safe
// one, it is one nobody said anything about.
func describeSpan(n int, breaking, behaviour, undeclared []string) string {
	parts := []string{}
	if len(breaking) > 0 {
		parts = append(parts, fmt.Sprintf("%d BREAKING (%s: config or output changed, the flow needs editing)",
			len(breaking), strings.Join(breaking, ", ")))
	}
	if len(behaviour) > 0 {
		parts = append(parts, fmt.Sprintf("%d behaviour change%s (%s: same config, different results)",
			len(behaviour), plural(len(behaviour)), strings.Join(behaviour, ", ")))
	}
	if len(undeclared) > 0 {
		parts = append(parts, fmt.Sprintf("%d undeclared (%s: the publisher did not say)",
			len(undeclared), strings.Join(undeclared, ", ")))
	}
	if len(parts) == 0 {
		return fmt.Sprintf("All %d intervening version%s are declared compatible.", n, plural(n))
	}
	return "Crossing them means " + strings.Join(parts, "; ") + "."
}

func plural(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}

// noticesWithCurrency is what the deploy and publish responses carry: the
// document's own notices plus the registry-derived ones.
func (a *api) noticesWithCurrency(r *http.Request, name string, version int) []flowdoc.Notice {
	_, raw, err := a.st.GetFlow(r.Context(), name, version)
	if err != nil {
		return nil
	}
	notices := flowdoc.ReviewRaw(raw)
	doc, err := flowdoc.Parse(raw)
	if err != nil {
		return notices
	}
	// End-of-life first: "this stops working on the 3rd" outranks "this is
	// three versions behind", and the order notices are read in is decided
	// here rather than in each client.
	notices = append(notices, a.eolNotices(r, doc)...)
	return append(notices, a.currencyNotices(r.Context(), doc)...)
}

// staleUpgradePins enforces ADR-0047 §4: a flow being moved FORWARD may not
// pin a connector build outside the support window.
//
// The rule has one exemption, and it is the important half. Publishing a
// version at or below the one already published is a ROLLBACK — an emergency
// action, taken when the current version is misbehaving — and it must never be
// blocked by a currency policy. A rollback that refused because the good
// version pins an old build would deny somebody the one thing they need at the
// worst possible moment. Rolling back deliberately keeps its original pins
// (ADR-0047 §1); that is what makes it a rollback.
//
// So the gate applies to going forward, where the person is already editing
// and the upgrade is cheap, and never to going back.
func (a *api) staleUpgradePins(r *http.Request, name string, version int) ([]string, error) {
	f, err := a.st.FlowByName(r.Context(), name)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, nil // publish will answer 404 for it
		}
		return nil, err
	}
	if version <= f.PublishedVersion {
		return nil, nil // rollback: never gated
	}
	_, raw, err := a.st.GetFlow(r.Context(), name, version)
	if err != nil {
		//nolint:nilerr // deliberate: publish reports a missing version itself; this gate stays silent
		return nil, nil
	}
	doc, err := flowdoc.Parse(raw)
	if err != nil {
		//nolint:nilerr // deliberate: an unparseable stored document is publish's problem, not this gate's
		return nil, nil
	}
	var stale []string
	for _, p := range doc.ConnectorPins() {
		if p.Version == "" {
			continue // unpinned resolves to newest, which is by definition current
		}
		behind, current, ok := a.versionsBehind(r.Context(), p)
		if !ok || behind < supportWindow {
			continue
		}
		stale = append(stale, fmt.Sprintf(
			"step %q pins %s %s, which is %d versions behind (current %s, supported window %s and newer)",
			p.StepID, p.Connector, p.Version, behind, current, current))
	}
	return stale, nil
}

// versionsBehind reports how many releases separate a pin from the newest
// build, and the newest version's name. ok is false when the registry cannot
// place the pin at all — a connector provisioned outside the registry, or a
// version that has been collected — because "unknown" must not read as "stale"
// and block a publish.
func (a *api) versionsBehind(ctx context.Context, p flowdoc.Pin) (behind int, current string, ok bool) {
	order := a.orderedVersions(ctx, p.Connector)
	if len(order) == 0 {
		return 0, "", false
	}
	i := indexOfVersion(order, p.Version)
	if i < 0 {
		return 0, order[0].Version, false
	}
	return i, order[0].Version, true
}

// orderedVersions lists a connector's versions newest first, one entry per
// version. The registry stores a row per platform, and "how many releases
// behind" is a question about releases, not about artifacts.
func (a *api) orderedVersions(ctx context.Context, connector string) []store.ConnectorVersion {
	versions, err := a.st.ConnectorVersions(ctx, connector)
	if err != nil {
		return nil
	}
	seen := map[string]bool{}
	var order []store.ConnectorVersion
	for _, v := range versions {
		if seen[v.Version] {
			continue
		}
		seen[v.Version] = true
		order = append(order, v)
	}
	return order
}

// indexOfVersion returns how many releases newer than version exist, or -1
// when the registry has never heard of it.
func indexOfVersion(order []store.ConnectorVersion, version string) int {
	for i, v := range order {
		if v.Version == version {
			return i
		}
	}
	return -1
}
