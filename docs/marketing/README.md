# Marketing mocks (internal)

Positioning and pricing mockups. **Internal only** — not a published site, not
built by CI, not linked from anywhere.

Open `index.html` directly in a browser (`open docs/marketing/index.html`). It is
a single self-contained file: no build step, no dependencies, no network calls,
and the in-page navigation is plain anchors that work from `file://`.

## Status of what's in here

- **Pricing is invented.** The *shape* follows ADR-0033 (a hub-held core budget,
  community at 4 cores, paid from a 4-core minimum, support contracts carrying
  the revenue). The dollar figures are placeholders for discussion.
- **Performance figures are real** — from `docs/bench-M1.md` and
  `docs/bench-M7/results.md`, which `make bench-report` regenerates. If the
  engine changes, these go stale; re-check them before this text is ever used.
- **The comparison is against our own buffered baseline**, not a named
  competitor. That is deliberate: it is a measurement we can defend and
  reproduce, and it avoids a benchmarking claim about someone else's product.
  Any move to a named comparison needs a real, reproducible methodology first.
- **Three connector chips are marked "soon"** (Salesforce, ServiceNow, Dynamics)
  because they do not exist yet. Keep unbuilt things visibly unbuilt.
