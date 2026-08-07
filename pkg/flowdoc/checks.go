package flowdoc

import "fmt"

// The built-in review checks (ADR-0042 §7). Each is self-contained and
// registered here; adding one is a function and an init entry, and removing
// one deletes both without touching anything else.
//
// Every check answers a question a developer would otherwise only get an
// answer to in production.

func init() {
	RegisterCheck(Check{
		Code:    "async-response",
		Summary: "a request-triggered flow with no @response answers 202, not the result",
		Fn:      checkAsyncResponse,
	})
	RegisterCheck(Check{
		Code:    "unverified-input",
		Summary: "a request-triggered flow with no input schema accepts anything",
		Fn:      checkUnverifiedInput,
	})
	RegisterCheck(Check{
		Code:    "input-scope",
		Summary: "records scope verifies the first record only",
		Fn:      checkInputScope,
	})
	RegisterCheck(Check{
		Code:    "sync-blocking",
		Summary: "a synchronous flow that must consume its whole stream before answering",
		Fn:      checkSyncBlocking,
	})
	RegisterCheck(Check{
		Code:    "connector-pin",
		Summary: "a connector step with no pinned version runs whatever is newest",
		Fn:      checkConnectorPin,
	})
}

// checkConnectorPin reports steps that will resolve to "newest" at dispatch
// (ADR-0047 §1).
//
// On a DRAFT that is expected and says so. On a PUBLISHED flow it means the
// registry had nothing to pin — usually a connector nobody published, or a
// name with a typo in it — and the consequence is the one pinning exists to
// remove: the next release of that connector changes what this flow does,
// without an edit, against live data.
func checkConnectorPin(d *Document) []Notice {
	var out []Notice
	for _, p := range d.ConnectorPins() {
		if p.Version != "" {
			continue
		}
		out = append(out, Notice{
			Code:     "connector-pin.unpinned",
			Severity: SeverityWarn,
			Title:    "This step is not pinned to a connector version",
			Detail: fmt.Sprintf("The %q step runs whichever build of %q is newest when the task starts, "+
				"so publishing a new version of it changes what this flow does with no edit here. "+
				"Publishing pins every connector the registry knows about — this one it did not, "+
				"which usually means %q has no published artifact, or the name is misspelt.",
				p.StepID, p.Connector, p.Connector),
			Step: p.StepID,
			Docs: "docs/adr/0047-connector-versioning-and-retention.md",
		})
	}
	return out
}

// checkAsyncResponse is the notice the developer asked for by name: a webhook
// flow with no @response is asynchronous, and nothing in the document says so
// out loud — the absence of a node is easy to miss on a canvas.
func checkAsyncResponse(d *Document) []Notice {
	id, ok := d.webhookSource()
	if !ok {
		// A scheduled or hub-queued flow has no caller waiting, so "no
		// @response" is not a fact about anything.
		return nil
	}
	if d.TerminatesAtResponse() {
		return []Notice{{
			Code:     "async-response.synchronous",
			Severity: SeverityInfo,
			Title:    "This flow answers its caller directly",
			Detail: "It ends at @response, so it is deployed as a SYNCHRONOUS web call: " +
				"the caller's connection stays open until the flow finishes, and the output " +
				"is the response body. Remove the @response node to answer 202 immediately instead.",
			Step: id,
			Docs: "docs/adr/0042-async-by-default-and-input-verification.md",
		}}
	}
	if d.EffectiveAck() == AckNone {
		return []Notice{{
			Code:     "async-response.fire-and-forget",
			Severity: SeverityInfo,
			Title:    "This flow will be deployed as fire-and-forget",
			Detail: "It has no @response node and declares ack \"none\", so callers receive 202 " +
				"Accepted with nothing to poll — no task id and no status URL. Failures are " +
				"visible to you in the execution history, but never to the caller. " +
				"Remove ack \"none\" to hand back a status URL.",
			Step: id,
			Docs: "docs/adr/0042-async-by-default-and-input-verification.md",
		}}
	}
	return []Notice{{
		Code:     "async-response.asynchronous",
		Severity: SeverityInfo,
		Title:    "This flow will be deployed as an asynchronous web call",
		Detail: "It has no @response node, so callers receive 202 Accepted with a task id and " +
			"a status URL, and the flow runs after the connection closes. " +
			"Add an @response sink if the caller needs the result in the same request.",
		Step: id,
		Docs: "docs/adr/0042-async-by-default-and-input-verification.md",
	}}
}

// checkUnverifiedInput is the other half of an honest 202. Answering "accepted"
// to a request nobody looked at means the only feedback a broken caller gets is
// a dead letter somebody reads tomorrow.
func checkUnverifiedInput(d *Document) []Notice {
	id, ok := d.webhookSource()
	if !ok {
		return nil
	}
	if _, has := d.InputSpec(); has {
		return nil
	}
	sev := SeverityWarn
	detail := "Any body that parses is accepted and answered 202, so a caller sending the " +
		"wrong shape learns nothing until the flow dead-letters. Add an input schema to the " +
		"webhook node to reject it synchronously, with the offending field named."
	if d.EffectiveAck() == AckNone {
		// Unverified AND untrackable is the one combination with no feedback
		// path at all: nothing is checked before the 202 and there is nothing
		// to poll afterwards. A 400 is the only signal such a caller can ever
		// get, and this flow has given up the ability to send one.
		detail = "This endpoint accepts anything AND is fire-and-forget, so a caller has no way " +
			"to discover a mistake: nothing is checked before the 202, and there is no status to " +
			"poll after it. An input schema is the only feedback such a caller can receive."
	} else if d.TerminatesAtResponse() {
		// A synchronous flow at least fails in front of its caller. Still worth
		// saying, but it is not the silent failure the warning is about.
		sev = SeverityInfo
		detail = "Any body that parses is accepted. This flow is synchronous, so a failure does " +
			"reach the caller — but as whatever error the failing step produced, rather than as " +
			"a field-level 400. Add an input schema to the webhook node for a precise refusal."
	}
	return []Notice{{
		Code:     "unverified-input.no-schema",
		Severity: sev,
		Title:    "This endpoint accepts unverified input",
		Detail:   detail,
		Step:     id,
		Docs:     "docs/adr/0042-async-by-default-and-input-verification.md",
	}}
}

// checkInputScope names the guarantee that scope: records actually gives,
// which is weaker than the word "verified" suggests.
func checkInputScope(d *Document) []Notice {
	in, ok := d.InputSpec()
	if !ok || in.EffectiveScope() != ScopeRecords {
		return nil
	}
	id, _ := d.webhookSource()
	return []Notice{{
		Code:     "input-scope.records",
		Severity: SeverityInfo,
		Title:    "Only the first record is verified",
		Detail: "This input uses scope \"records\", so the rest of the stream is validated as it " +
			"runs and a bad record part-way through becomes a dead letter, not a 400. " +
			"Use scope \"body\" to verify the whole request before accepting it.",
		Step: id,
		Docs: "docs/adr/0042-async-by-default-and-input-verification.md",
	}}
}

// checkSyncBlocking catches the sync flow that cannot answer early. A streaming
// @response flow starts writing as soon as records arrive; one containing a
// blocking operator has to consume its entire input first, which is where a
// synchronous route meets the delivery timeout.
func checkSyncBlocking(d *Document) []Notice {
	if !d.TerminatesAtResponse() {
		return nil
	}
	var out []Notice
	for kind, id := range d.blockingSteps() {
		out = append(out, Notice{
			Code:     "sync-blocking." + kind,
			Severity: SeverityWarn,
			Title:    fmt.Sprintf("The caller waits for the whole stream (%s)", kind),
			Detail: fmt.Sprintf("A %s step cannot emit until it has read all of its input, so this "+
				"synchronous flow holds the caller's connection open for the entire run and may "+
				"exceed the gateway's delivery timeout on a large request. "+
				"Removing the @response node deploys it asynchronously instead.", kind),
			Step: id,
			Docs: "docs/adr/0042-async-by-default-and-input-verification.md",
		})
	}
	return out
}

// webhookSource returns the id of the flow's @webhook source and whether it has
// one — that is, whether a CALLER exists to have opinions about. In linear form
// there is no step id, so the source is named by its reserved connector name,
// which is what the studio's canvas labels it anyway.
func (d *Document) webhookSource() (string, bool) {
	if d.Source.Connector == WebhookSource {
		return WebhookSource, true
	}
	for i := range d.Steps {
		s := &d.Steps[i]
		if s.Type == "source" && s.Connector == WebhookSource {
			return s.ID, true
		}
	}
	return "", false
}

// blockingSteps returns the flow's blocking operators as kind → step id (one
// per kind: naming every aggregate in a ten-aggregate flow says the same thing
// ten times).
func (d *Document) blockingSteps() map[string]string {
	out := map[string]string{}
	for i := range d.Ops {
		if d.Ops[i].Type == "aggregate" {
			out["aggregate"] = "aggregate"
			break
		}
	}
	for i := range d.Steps {
		s := &d.Steps[i]
		switch {
		case s.Type == "aggregate":
			if _, seen := out["aggregate"]; !seen {
				out["aggregate"] = s.ID
			}
		case s.Type == "merge" && s.Mode == MergeJoin:
			// A join buffers its build side entirely — the same stall, from a
			// different node.
			if _, seen := out["join"]; !seen {
				out["join"] = s.ID
			}
		}
	}
	return out
}
