package service

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/aaron-au/shift/runner/internal/flow"
	"github.com/aaron-au/shift/runner/internal/task"
)

// ADR-0029 claims that ANY validated topology executes — nested fan-outs and
// mixed fan-out/fan-in included. The hand-built shapes in multipath_test.go
// test six topologies; they do not test the quantifier. This file generates
// random VALID documents and executes them, so the claim is checked against
// shapes nobody wrote down.
//
// The invariant is record conservation, computed exactly rather than
// approximated. Every node kind has a defined effect on the multiset of source
// records flowing through it:
//
//	tee     duplicates   — every branch sees every record
//	router  partitions   — first matching route wins, the rest take the
//	                       default, and with no default they are DROPPED
//	filter  removes      — by a predicate over a known field
//	concat  sums         — the union of its two inputs
//	join    multiplies   — one output per (probe, matching build) pair;
//	                       a left join keeps an unmatched probe once
//
// So the generator carries a multiset alongside the document it is building
// and knows, before the flow runs, exactly how many records the sinks must
// confirm. That number is the assertion: a tee that drops a branch, a router
// that broadcasts instead of partitioning, or a merge that loses a side all
// move it. "No error" would catch none of them.
//
// Failures print the seed and the generated document, because an
// unreproducible failure in a generative test is worth very little.

// topologySeed roots the corpus. It is constant so the suite is deterministic
// and a failure is reproducible from the printed seed alone; bump it to sweep
// a different corpus.
const topologySeed = 20260809

// topologyCases keeps the corpus at unit-test scale — this runs in every pass,
// not in a nightly fuzzing job.
const topologyCases = 32

// topologyRecords is the size of the injected body. It is deliberately small
// enough to be ONE ndjson batch (the reader's default is 1024 records/batch).
//
// That is not only for speed. A fan-out branch feeding a blocking merge (a
// join builds its whole right side before the probe streams) parks its records
// in the tee's per-branch queue and the branch pipe, both bounded at 4 batches
// — so the enrichment shape deadlocks permanently somewhere between 5 and 12
// batches of input, measured. Raising this number would make this test HANG
// rather than fail, which is a much worse way to learn it.
const topologyRecords = 9

// mset is a multiset over source record ids: how many copies of record i a
// stream carries at some point in the graph.
type mset map[int]int

func fullSet() mset {
	m := make(mset, topologyRecords)
	for i := range topologyRecords {
		m[i] = 1
	}
	return m
}

func (m mset) clone() mset {
	out := make(mset, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func (m mset) addAll(o mset) {
	for k, v := range o {
		m[k] += v
	}
}

func (m mset) size() int64 {
	var n int64
	for _, v := range m {
		n += int64(v)
	}
	return n
}

// recordBody builds the injected NDJSON. Every field a generated predicate can
// name lives here, and all of them are strings: a record's id is its identity
// (unique, so a self-join's multiplicity is exactly the input multiplicity),
// k is what routers partition on, and v is what selective filters test.
func recordBody() []byte {
	var out []byte
	for i := range topologyRecords {
		out = append(out, fmt.Sprintf("{\"id\":%d,\"k\":%q,\"v\":%q}\n", i, routeKey(i), keepFlag(i))...)
	}
	return out
}

func routeKey(i int) string { return []string{"a", "b", "c"}[i%3] }
func keepFlag(i int) string {
	if i%2 == 0 {
		return "keep"
	}
	return "drop"
}

// topoGen builds one random document. It is not a fuzzer: every choice it
// makes is one pkg/flowdoc accepts, so a validation error from a generated
// document is a finding (in the generator or in validation), never noise.
type topoGen struct {
	rnd   *rand.Rand
	steps []flow.Step
	n     int
	// expect accumulates the records every sink must confirm.
	expect int64
	// feat tallies which shapes the corpus actually produced, so a generator
	// that quietly degenerates to source→sink is caught by the corpus check
	// rather than passing 32 vacuous cases.
	feat map[string]int
	// belowFedMerge is set while generating the subgraph downstream of a merge
	// whose inputs come from a fan-out. A fan-out there is the shape whose
	// compilation used to depend on Go map iteration order (TC-028): the same
	// document ran or failed at random. It is tallied, not suppressed — the
	// corpus check below insists the corpus keeps containing it.
	belowFedMerge bool
}

func newTopoGen(seed uint64) *topoGen {
	//nolint:gosec // G404: a topology shape drawn from a fixed seed, not a credential — reproducibility is the point
	return &topoGen{rnd: rand.New(rand.NewPCG(seed, seed^0x9e3779b9)), feat: map[string]int{}}
}

func (g *topoGen) id(prefix string) string {
	g.n++
	return fmt.Sprintf("%s%d", prefix, g.n)
}

func (g *topoGen) add(s flow.Step) { g.steps = append(g.steps, s) }

// budgeted reports whether the graph has grown enough to stop branching. A
// hard cap keeps a depth-3 fan-out of width 3 from producing a 100-node
// document; the point is breadth of SHAPE across cases, not size within one.
func (g *topoGen) budgeted() bool { return g.n >= 24 }

// document generates one complete flow.
func (g *topoGen) document(name string) *flow.Document {
	depth := 1 + g.rnd.IntN(3)
	// One source, or two independent @webhook sources fanned in at a merge.
	// The second shape matters because its fan-in has no tee above it — the
	// merge's inputs are roots, not branches.
	if g.rnd.IntN(4) == 0 {
		g.feat["two-sources"]++
		s1 := g.source()
		s2 := g.source()
		// Both sources are @webhook, so each replays the whole injected body:
		// two roots carrying one full copy of the input each.
		mergeID := g.merge(s1.ID, fullSet(), s2.ID, fullSet(), depth)
		s1.OnSuccess, s2.OnSuccess = mergeID, mergeID
		g.add(s1)
		g.add(s2)
	} else {
		src := g.source()
		src.OnSuccess = g.chain(fullSet(), depth, false)
		g.add(src)
	}
	return &flow.Document{Name: name, Steps: g.steps}
}

func (g *topoGen) source() flow.Step {
	s := step(g.id("src"), "source")
	s.Connector, s.Action = "@webhook", "ndjson"
	return s
}

// chain creates the subgraph that consumes `in` and returns the id of its
// ENTRY node; the caller wires its own edge to that id. Every path chain
// creates terminates at a sink, so the document is always complete.
func (g *topoGen) chain(in mset, depth int, insideFanOut bool) string {
	if depth <= 0 || g.budgeted() {
		return g.sink(in)
	}
	switch g.rnd.IntN(11) {
	case 0, 1:
		return g.sink(in)
	case 2, 3, 4:
		return g.transform(in, depth, insideFanOut)
	case 5, 6, 7:
		return g.tee(in, depth, insideFanOut)
	case 8, 9:
		return g.router(in, depth, insideFanOut)
	default:
		return g.teeIntoMerge(in, depth, insideFanOut)
	}
}

func (g *topoGen) sink(in mset) string {
	s := step(g.id("out"), "sink")
	s.Connector = "@discard"
	g.add(s)
	g.expect += in.size()
	g.feat["sink"]++
	return s.ID
}

// transform emits one branch-local operator and recurses. All three are
// count-exact: two are pass-throughs (so an operator between structural nodes
// is exercised without perturbing the arithmetic) and one drops half the
// records by a predicate the model evaluates the same way the engine does.
func (g *topoGen) transform(in mset, depth int, insideFanOut bool) string {
	var s flow.Step
	out := in.clone()
	switch g.rnd.IntN(3) {
	case 0:
		s = step(g.id("keep"), "filter")
		s.Path, s.Cmp = "$.id", "exists" // always true: every record has an id
	case 1:
		s = step(g.id("sel"), "filter")
		s.Path, s.Cmp, s.Value = "$.v", "eq", json.RawMessage(`"keep"`)
		for id := range in {
			if keepFlag(id) != "keep" {
				delete(out, id)
			}
		}
	default:
		s = step(g.id("proj"), "project")
		// Every field a downstream predicate or join key can name is kept, so
		// a project anywhere in the graph is count-exact and never strands a
		// later step's path.
		s.Fields = []flow.ProjectField{{Path: "$.id"}, {Path: "$.k"}, {Path: "$.v"}}
	}
	next := g.chain(out, depth-1, insideFanOut)
	// onSuccess and onComplete are structurally identical happy edges; using
	// both keeps the generated documents from testing only one spelling.
	if g.rnd.IntN(2) == 0 {
		s.OnSuccess = next
	} else {
		s.OnComplete = next
	}
	g.add(s)
	return s.ID
}

// tee duplicates: every branch sees every record.
func (g *topoGen) tee(in mset, depth int, insideFanOut bool) string {
	t := step(g.id("tee"), "tee")
	if insideFanOut {
		g.feat["nested-fanout"]++
	}
	if g.belowFedMerge {
		g.feat["fanout-below-merge"]++
	}
	g.feat["tee"]++
	width := 2 + g.rnd.IntN(2)
	for range width {
		t.Branches = append(t.Branches, g.chain(in.clone(), depth-1, true))
	}
	g.add(t)
	return t.ID
}

// router partitions: a record takes the FIRST route it matches, else the
// default, else it is dropped. All three outcomes are modelled — the dropped
// case included, because a router with no default legitimately loses records
// and a test that only ever generated a default would not know the difference.
func (g *topoGen) router(in mset, depth int, insideFanOut bool) string {
	r := step(g.id("rt"), "router")
	if insideFanOut {
		g.feat["nested-fanout"]++
	}
	if g.belowFedMerge {
		g.feat["fanout-below-merge"]++
	}
	g.feat["router"]++

	keys := []string{"a", "b", "c"}
	g.rnd.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })
	nRoutes := 1 + g.rnd.IntN(2)
	// A router needs ≥2 targets: with a single route the default is mandatory.
	withDefault := nRoutes == 1 || g.rnd.IntN(3) == 0
	routed := keys[:nRoutes]

	arms := make([]mset, nRoutes)
	for i := range arms {
		arms[i] = mset{}
	}
	def := mset{}
	for id, count := range in {
		matched := false
		for i, k := range routed {
			if routeKey(id) == k {
				arms[i][id] += count
				matched = true
				break // first match wins
			}
		}
		if !matched && withDefault {
			def[id] += count
		}
		// Neither matched nor defaulted ⇒ dropped, and counted nowhere.
	}

	for i, k := range routed {
		to := g.chain(arms[i], depth-1, true)
		r.Routes = append(r.Routes, flow.Route{Path: "$.k", Cmp: "eq", Value: json.RawMessage(`"` + k + `"`), To: to})
	}
	if withDefault {
		g.feat["router-default"]++
		r.Default = g.chain(def, depth-1, true)
	} else {
		g.feat["router-drops"]++
	}
	g.add(r)
	return r.ID
}

// teeIntoMerge is the mixed shape: one stream teed in two, each leg carrying
// its own operators — or none at all — then joined back at a merge. ADR-0029
// names the enrichment version of this as the most common real integration.
func (g *topoGen) teeIntoMerge(in mset, depth int, insideFanOut bool) string {
	t := step(g.id("tee"), "tee")
	if insideFanOut {
		g.feat["nested-fanout"]++
	}
	g.feat["tee"]++
	g.feat["fanout-into-fanin"]++

	// The merge id is allocated first so both legs can point at it.
	mergeID := g.id("mg")
	entryA, lastA, outA := g.leg(in.clone(), t.ID, mergeID, 0)
	// The two legs may not BOTH be empty: that would give the tee the same
	// branch twice and the merge one producer named twice, which is a different
	// (and invalid) document, not the shape under test.
	minB := 0
	if entryA == mergeID {
		minB = 1
	}
	entryB, lastB, outB := g.leg(in.clone(), t.ID, mergeID, minB)
	t.Branches = []string{entryA, entryB}
	g.add(t)

	g.buildMerge(mergeID, lastA, outA, lastB, outB, depth, true)
	return t.ID
}

// leg builds a tee branch as a short run of transforms ending at `to`, or —
// when it draws zero operators and minOps allows it — as no steps at all, so
// the tee feeds the merge directly.
//
// The empty leg is deliberate: it is a topology pkg/flowdoc accepts, and the
// DAG compiler used to refuse it at run time because the branch pipe was keyed
// under the merge's own id rather than the fan-out's (TC-027). Both the entry
// the tee branches to and the producer the merge names are then the FAN-OUT.
func (g *topoGen) leg(in mset, fanOutID, to string, minOps int) (entry, last string, out mset) {
	// Kinds are drawn first so the multiset can be computed forwards while the
	// steps are wired backwards (each needs its successor's id).
	kinds := make([]int, max(minOps, g.rnd.IntN(3)))
	out = in.clone()
	if len(kinds) == 0 {
		g.feat["empty-merge-leg"]++
		return to, fanOutID, out
	}
	for i := range kinds {
		kinds[i] = g.rnd.IntN(3)
		if kinds[i] == 2 {
			// A selective leg is what makes the two sides of a join DIFFER:
			// with identical sides an inner join always matches and a left
			// join never takes its unmatched arm, so neither is really tested.
			for id := range in {
				if keepFlag(id) != "keep" {
					delete(out, id)
				}
			}
		}
	}

	next := to
	for i := len(kinds) - 1; i >= 0; i-- {
		var s flow.Step
		switch kinds[i] {
		case 0:
			s = step(g.id("keep"), "filter")
			s.Path, s.Cmp = "$.id", "exists"
		case 1:
			s = step(g.id("proj"), "project")
			s.Fields = []flow.ProjectField{{Path: "$.id"}, {Path: "$.k"}, {Path: "$.v"}}
		default:
			s = step(g.id("sel"), "filter")
			s.Path, s.Cmp, s.Value = "$.v", "eq", json.RawMessage(`"keep"`)
		}
		s.OnSuccess = next
		g.add(s)
		if i == len(kinds)-1 {
			last = s.ID // the step that actually flows into the merge
		}
		next = s.ID
		entry = s.ID
	}
	return entry, last, out
}

// merge wires a fan-in over two named producers and returns the merge id, for
// the two-source shape where the producers are the sources themselves.
func (g *topoGen) merge(fromA string, outA mset, fromB string, outB mset, depth int) string {
	id := g.id("mg")
	g.buildMerge(id, fromA, outA, fromB, outB, depth, false)
	return id
}

// buildMerge creates the merge node with the given id and continues downstream.
// fedByFanOut says its inputs come from a tee/router rather than from roots;
// see topoGen.belowFedMerge for why that matters to what follows it.
func (g *topoGen) buildMerge(id, fromA string, outA mset, fromB string, outB mset, depth int, fedByFanOut bool) {
	m := step(id, "merge")
	m.Inputs = []string{fromA, fromB}

	var merged mset
	if g.rnd.IntN(2) == 0 {
		g.feat["merge-concat"]++
		m.Mode = "concat"
		merged = outA.clone()
		merged.addAll(outB)
	} else {
		g.feat["merge-join"]++
		m.Mode = "join"
		m.On = &flow.JoinOn{Left: "$.id", Right: "$.id"}
		m.As = "match" + id
		// Either side may be the build side; the other is the probe.
		build, probe := outA, outB
		m.Build = fromA
		if g.rnd.IntN(2) == 0 {
			build, probe = outB, outA
			m.Build = fromB
		}
		left := g.rnd.IntN(2) == 0
		m.JoinType = "inner"
		if left {
			m.JoinType = "left"
			g.feat["join-left"]++
		} else {
			g.feat["join-inner"]++
		}
		// One output row per (probe, matching build) pair; a left join keeps
		// an unmatched probe record once, with a null match.
		merged = mset{}
		for id, p := range probe {
			switch b := build[id]; {
			case b > 0:
				merged[id] = p * b
			case left:
				merged[id] = p
			}
		}
	}

	defer func(prev bool) { g.belowFedMerge = prev }(g.belowFedMerge)
	g.belowFedMerge = g.belowFedMerge || fedByFanOut
	m.OnSuccess = g.chain(merged, depth-1, false)
	g.add(m)
}

// TestAnyGeneratedValidTopologyExecutesAndConservesRecords is the quantifier
// ADR-0029 asserts, checked over a generated corpus rather than a handful of
// drawn shapes.
func TestAnyGeneratedValidTopologyExecutesAndConservesRecords(t *testing.T) {
	svc := newBuiltinService(t, Options{})
	body := recordBody()

	corpus := map[string]int{}
	for c := range topologyCases {
		seed := uint64(topologySeed + c)
		g := newTopoGen(seed)
		doc := g.document(fmt.Sprintf("topo-%d", c))
		for k, v := range g.feat {
			corpus[k] += v
		}

		t.Run(fmt.Sprintf("seed-%d", seed), func(t *testing.T) {
			dump := func() string {
				b, err := json.MarshalIndent(doc, "", "  ")
				if err != nil {
					return fmt.Sprintf("<undumpable document: %v>", err)
				}
				return string(b)
			}

			// A generated document that validation rejects is a finding either
			// way: the generator emitted something the model does not allow, or
			// validation rejects something ADR-0029 permits. Neither is a pass.
			if err := doc.Validate(); err != nil {
				t.Fatalf("generated document failed validation (seed %d): %v\n%s", seed, err, dump())
			}
			if _, err := doc.Plan(); err != nil {
				t.Fatalf("generated document failed to plan (seed %d): %v\n%s", seed, err, dump())
			}

			id, err := svc.SubmitWith(doc, SubmitOpts{WebhookBody: body})
			if err != nil {
				t.Fatalf("submit (seed %d): %v\n%s", seed, err, dump())
			}
			tk := awaitTerminal(t, svc, id)
			if tk.State != task.StateCompleted {
				t.Fatalf("seed %d: state = %s: %s\n%s", seed, tk.State, tk.Error, dump())
			}
			if tk.SinkConfirmed != g.expect {
				t.Fatalf("seed %d: sinks confirmed %d records, want %d (%d source records through %d steps)\n%s",
					seed, tk.SinkConfirmed, g.expect, topologyRecords, len(doc.Steps), dump())
			}
		})
	}

	// The corpus itself has to be worth running. Without this a generator that
	// degenerated to source→sink would report 32 green cases while testing
	// nothing ADR-0029 claims.
	t.Logf("corpus over %d cases: %v", topologyCases, corpus)
	for _, want := range []string{
		"tee", "router", "router-default", "router-drops",
		"merge-concat", "merge-join", "join-inner", "join-left",
		"nested-fanout", "fanout-into-fanin", "two-sources",
		// The two shapes this test was once blind to (TC-027, TC-028). They are
		// listed here so the blindness cannot come back by accident: a generator
		// change that stopped producing them fails the corpus check rather than
		// quietly reporting 32 green cases again.
		"empty-merge-leg", "fanout-below-merge",
	} {
		if corpus[want] == 0 {
			t.Errorf("the generated corpus contains no %q shape; the generator has degenerated", want)
		}
	}
	if corpus["sink"] < 2*topologyCases {
		t.Errorf("corpus has %d sinks over %d cases; fan-out is barely being generated", corpus["sink"], topologyCases)
	}
}
