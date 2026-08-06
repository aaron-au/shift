# Gateway request-reply benchmark (ADR-0038)

What it measures: **caller → gateway → parked runner → connector work → back**,
over real HTTP on both hops, with the response streaming to the original
caller.

The runner is a stub rather than the real engine, deliberately. The engine's
own cost is already measured (`docs/bench-M1.md`) and the runner's trigger path
separately (`docs/dev/04-runner.md`). What was unmeasured — and what this
isolates — is the **gateway's contribution**: the dispatch hand-over, the
two-request poll/deliver exchange, and how both behave under concurrency.

```sh
go test ./gateway/internal/ingress -run xxx -bench GatewayRequestReply -benchtime 2s
SHIFT_BENCH_PROFILE=1 go test ./gateway/internal/ingress -run TestGatewayLatencyProfile -v
SHIFT_BENCH_PROFILE=1 go test ./gateway/internal/ingress -run TestMutualTLSCost -v
go test ./gateway/internal/ingress -run xxx -bench ControlHandshake -benchtime 2000x
```

**All figures below are over mutual TLS** (ADR-0041), which is what ships: the
runner presents a client certificate, the gateway resolves its labels from the
hub's roster on every poll, and an unrostered identity is refused. The cost of
that is measured in "What mTLS cost" below rather than assumed.

## Why the backends have jitter and spikes

A zero-latency stub measures Go's scheduler, not this system. Every backend
here is a service-time *distribution* — a floor, uniform jitter on top, and a
1–3% spike arm — because that is what a connector actually does: talk to
something else and wait, mostly quickly, occasionally not.

| Backend | Service time | Models |
|---|---|---|
| `instant` | 0 | the platform with nothing underneath — pure framework cost |
| `fast-1ms` | 0.8–1.2 ms | a local cache or in-memory lookup |
| `rest-20ms` | 15–25 ms, 1% +100 ms | a same-region REST API — the shape most request-reply flows are |
| `db-50ms` | 40–65 ms, 2% +250 ms | a database round trip with a contended pool |
| `legacy-200ms` | 150–270 ms, 3% +800 ms | a slow SOAP endpoint — the case where the gateway must simply not be the problem |

The spikes are the point of the exercise: they are what produces the `rest-20ms`
p99 of 117 ms against a p50 of 20 ms, and an integration platform is judged on
that gap rather than on its mean.

## Results

Apple M4 Max, `GOMAXPROCS=14`, loopback, mutual TLS + HTTP/2 on the control
listener, runner pool sized `2×conc`.

| Backend | conc | p50 | p95 | p99 | max | req/s | errors |
|---|---:|---:|---:|---:|---:|---:|---:|
| instant | 1 | **0.22 ms** | 0.48 ms | 0.71 ms | 0.94 ms | 3,709 | 0 |
| instant | 8 | 0.92 ms | 1.32 ms | 2.29 ms | 2.88 ms | 8,611 | 0 |
| instant | 64 | 2.47 ms | 5.77 ms | 16.85 ms | 21.37 ms | **20,066** | 0 |
| fast-1ms | 1 | 1.47 ms | 1.73 ms | 1.93 ms | 2.13 ms | 667 | 0 |
| fast-1ms | 8 | 1.36 ms | 1.86 ms | 2.21 ms | 2.33 ms | 5,706 | 0 |
| fast-1ms | 64 | 2.57 ms | 5.22 ms | 7.80 ms | 14.79 ms | 19,903 | 0 |
| rest-20ms | 1 | 22.16 ms | 26.19 ms | 120.61 ms | 123.78 ms | 40 | 0 |
| rest-20ms | 8 | 21.42 ms | 25.94 ms | 120.11 ms | 125.53 ms | 284 | 0 |
| rest-20ms | 64 | 20.56 ms | 25.13 ms | 117.22 ms | 127.62 ms | 2,489 | 42 × 503 |
| db-50ms | 64 | 53.59 ms | 65.07 ms | 303.33 ms | 314.74 ms | 834 | 0 |
| legacy-200ms | 64 | 213.65 ms | 268.95 ms | 1029.72 ms | 1066.94 ms | 232 | 0 |

The 42 failures are **503s, not defects**: at that instant no runner was parked,
and the gateway sheds rather than queues (ADR-0038 §2). The benchmark now
reports failures by kind for exactly this reason — a bare count invites the
reader to assume noise, and "the gateway shed load" and "a connection broke"
mean opposite things.

**Platform cost** — p50 minus the backend's *expected median* (floor + half the
jitter; spikes excluded, they move the tail not the median):

| Backend | conc=1 | conc=8 | conc=64 |
|---|---:|---:|---:|
| instant | 0.22 ms | 0.92 ms | 2.47 ms |
| fast-1ms | 0.47 ms | 0.36 ms | 1.57 ms |
| rest-20ms | 2.16 ms | 1.42 ms | **0.56 ms** |
| db-50ms | 3.50 ms | 2.24 ms | 1.09 ms |
| legacy-200ms | 1.71 ms | 3.63 ms | 3.65 ms |

Read that table's *trend*, not its absolute values at low concurrency. Cost
**falls** as concurrency rises, which is backwards for real overhead and is the
tell that the residual at `conc=1` is measurement error, not the gateway:
`time.Sleep` resolves to roughly a millisecond on darwin, and at 60 samples the
sample median of a wide uniform distribution wanders (hence `legacy-200ms`
showing 11 ms at `conc=1` and 3.6 ms at `conc=64` for identical work).

**The honest overhead figure is the `instant` row**: no sleep, no subtraction,
nothing to mis-estimate. **0.22 ms p50, 0.71 ms p99, single caller.**

## What mTLS cost

Three transports, identical in every other respect — same roster lookup, same
identity resolution, same backends, same runner count — so the only variable is
the wire. `mtls-http1` pins ALPN to HTTP/1.1 to isolate encryption from
protocol; `mtls-http2` is what ships.

| Backend | conc | plaintext p50 | mtls-http1 | mtls-http2 | http1 Δ | http2 Δ |
|---|---:|---:|---:|---:|---:|---:|
| instant | 1 | 0.22 ms | 0.22 ms | 0.24 ms | −0.00 ms | +0.02 ms |
| instant | 8 | 0.93 ms | 0.84 ms | 0.88 ms | −0.09 ms | −0.05 ms |
| instant | 64 | 1.91 ms | 1.78 ms | 2.65 ms | −0.13 ms | **+0.74 ms** |
| rest-20ms | 1 | 22.25 ms | 22.36 ms | 22.34 ms | +0.11 ms | +0.09 ms |
| rest-20ms | 8 | 21.23 ms | 21.42 ms | 21.33 ms | +0.18 ms | +0.10 ms |
| rest-20ms | 64 | 20.50 ms | 20.43 ms | 20.50 ms | −0.07 ms | +0.00 ms |

**Encryption itself costs nothing measurable.** Every `mtls-http1` delta is
within run-to-run noise, and several are negative. That is the expected result
and it is worth stating plainly: TLS 1.3 with P-256 on a warm connection is
symmetric-crypto-only, and at 256-byte payloads that is not where the time goes.

**The handshake is real but paid once per connection:**

| | ns/op |
|---|---:|
| `ControlHandshake/cold` — new connection every poll | 1,330,000 |
| `ControlHandshake/warm` — connection reused | 213,000 |
| **difference: the mutual handshake** | **≈ 1.11 ms** |

A runner opens its connections at startup and parks them for its lifetime, so
the per-request share of that 1.11 ms tends to zero. It matters exactly once,
at process start, and it is the reason a runner that reconnects per poll would
be paying five times its own service cost in handshakes.

**HTTP/2 is where a real cost showed up, and it is not encryption.** At
`conc=64` against the *instant* backend, h2 costs ~0.7 ms p50 and drops
throughput from ~27,100 to ~20,100 req/s — reproduced across three runs. It
disappears entirely once a backend has any service time at all (the `rest-20ms`
rows are identical across all three wires), which is why it did not show up
until the framework-cost row was measured on its own. The mechanism is not
established; measuring it, rather than explaining it, is what this table claims.

The trade is deliberate for now: h2 means a runner's parked polls share one
connection instead of holding a socket each, which is connection economy on
both ends and defence in depth against the port exhaustion documented below.
It is also a one-line runner-side change (`ForceAttemptHTTP2`) if the throughput
matters more — the gateway offers both and the client chooses.

**Worth revisiting when ADR-0042 lands.** Async-by-default collapses the
exchange to "validate and accept", which makes the *instant* row — the one
place h2 costs anything — the realistic shape rather than a synthetic one.

## Targets

| | Realistic | Stretch | Measured | |
|---|---|---|---|---|
| Platform overhead, p50 | ≤ 1 ms | ≤ 0.5 ms | **0.22 ms** | stretch met |
| Platform overhead, p99 | ≤ 5 ms | ≤ 1 ms | **0.71 ms** | stretch met |
| Throughput, one gateway | 10,000 req/s | 25,000 req/s | **20,066 req/s** (h2) / 27,130 (h1) | realistic met; stretch met only over HTTP/1.1 |
| Tail at 64 concurrent | p99 ≤ 25 ms | p99 ≤ 10 ms | **16.85 ms** (h2) / 5.50 ms (h1) | realistic met; stretch met only over HTTP/1.1 |
| Added cost vs a real 20 ms backend | ≤ 5 ms | ≤ 1 ms | **0.56 ms** | stretch met |
| Errors under sustained load | 0 | 0 | **0 transport**; 503s when capacity is exceeded | met |

Those targets are for the **gateway machinery**, measured against stub runners.
For what a real runner serves end to end, see the next section — the two are
different questions and the numbers differ by ~3×.

For scale: ~9,300 real executions/sec on a single runner is **93× the 100 tps**
that already counts as a very large integration deployment, and the gateway
itself absorbs roughly twice that again. Throughput is not the constraint this
design will hit first.

The end-to-end runner figures in the next section predate mTLS and have not been
re-measured on the cluster; the control hop there was plaintext. Given that
encryption itself cost nothing measurable above, they should move very little —
but they are labelled rather than quietly reused.

## The real end-to-end figure: a whole runner, not a stub

Everything above isolates the gateway. This section is the other question —
**what does one real runner actually serve**, with `gatewayd`, `runnerd`, the
engine, and a `@webhook → @response` flow, over real HTTP.

Same host (so the load generator, gateway and runner share 14 cores — a real
deployment separates them), one runner, 16 parked polls:

| Concurrent callers | ok/s | 503s | p50 | p95 | p99 |
|---:|---:|---:|---:|---:|---:|
| 1 | 4,069 | 0 | 0.25 ms | 0.38 ms | 0.45 ms |
| 4 | 6,950 | 0 | 0.57 ms | 0.92 ms | 1.09 ms |
| 8 | **9,285** | 8 | 0.80 ms | 1.62 ms | 2.09 ms |
| 16 | 8,234 | 70,087 | 1.36 ms | 3.12 ms | 4.14 ms |
| 64 | 6,896 | 267,734 | 1.97 ms | 4.62 ms | 6.11 ms |

**~9,300 executions/sec on ONE runner**, at the point where offered load
matches capacity. Past that the excess is shed as immediate 503s — working as
designed, and the signal to add a runner rather than to queue.

Raising the parked-poll count (`-gateway-polls`) does not lift the ceiling
much (16 → 128 moved 4,080 → 5,216 ok/s at 64 callers): poll slots were not the
constraint once the connection pool was fixed. The gateway itself absorbs
~50,000 req/s total in that state, most of them 503s.

**Do not read the 20,066 figure as executions.** That is the gateway's dispatch
machinery against *stub* runners doing no work. The end-to-end number is this
one.

## Three bugs this benchmark found

Neither was visible to the unit tests, and both needed load to surface.

1. **Ephemeral port exhaustion on the runner.** `gwclient` used a default
   `http.Client`, and `http.DefaultTransport` keeps **2** idle connections per
   host. With 16 parked polls plus deliveries against one gateway, everything
   above that was torn down into `TIME_WAIT` until the port range ran out:
   `dial tcp: connect: can't assign requested address`, every poll failing, and
   callers waiting out the full 60-second delivery timeout for responses the
   runner could not send. A 6-second run took 63 seconds. The pool is now sized
   to the poll concurrency.

2. **Silent response truncation.** The registry closed the exchange in
   `Dispatch`'s defer — which runs when Dispatch *returns*, before the ingress
   handler has copied the body. That let `Deliver` return, which let the
   runner's HTTP handler return, which closed the body mid-copy. Callers got a
   correct status and an empty or partial body, intermittently. Responsibility
   for closing now transfers to `Response.Release`, called after the copy.
   Regression test: `TestResponseBodyIsNotTruncated`.

3. **The rig itself rotted, silently.** ADR-0041 moved labels out of the poll
   body; the rig kept sending them, matched no route selector, and every
   dispatch answered 503. Nothing failed, because benchmarks do not run under
   `make check` — the measurement was simply unavailable until someone ran it
   by hand. `TestBenchRigServesOverEveryWire` now serves one request over each
   wire in the normal suite, so the fixture cannot rot that way again.

The second one is the reason to distrust a benchmark that only reports
aggregates: it produced no errors, no log lines, and a 200. Only a test that
compared the bytes caught it. The third is the reason a benchmark needs a test
of its own — an unrun measurement decays at exactly the rate the code changes.

## Known limits, stated rather than buried

- **~29 KB and ~354 allocations per request**, from `BenchmarkGatewayRequestReply`.
  Three HTTP messages cross per caller request (caller in, poll out, deliver
  in), each with its own header map. At 26 k req/s that is roughly 780 MB/s of
  allocation churn, and it is what will cap throughput before CPU does. It is
  irrelevant at realistic rates and it is the first thing to attack if that
  ever stops being true.
- **Loopback, single host.** Mutual TLS on the control hop, but **no TLS on the
  public edge**, no NIC, no cluster network. A real deployment adds a TLS
  handshake for each caller and a network hop to the runner. The minikube figure in `deploy/k8s/README.md` (6.1 ms p50) is the
  same code path through `kubectl port-forward` and a Calico overlay — treat
  that as the pessimistic bound and this as the optimistic one.
- **The runner is a stub.** These numbers say nothing about flow execution,
  which is measured elsewhere.
- **`conc=1` rows are 60 samples.** Enough to see a shape, not enough for a
  percentile claim; the `conc=64` rows carry ~3,800.
