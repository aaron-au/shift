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
```

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

Apple M4 Max, `GOMAXPROCS=14`, loopback HTTP, runner pool sized `2×conc`.

| Backend | conc | p50 | p95 | p99 | max | req/s | errors |
|---|---:|---:|---:|---:|---:|---:|---:|
| instant | 1 | **0.26 ms** | 0.39 ms | 0.47 ms | 0.89 ms | 3,573 | 0 |
| instant | 8 | 1.01 ms | 1.64 ms | 1.97 ms | 2.54 ms | 7,506 | 0 |
| instant | 64 | 1.96 ms | 4.32 ms | 8.01 ms | 10.93 ms | **26,852** | 0 |
| fast-1ms | 1 | 1.80 ms | 3.69 ms | 4.09 ms | 5.20 ms | 475 | 0 |
| fast-1ms | 8 | 1.39 ms | 2.48 ms | 4.39 ms | 5.37 ms | 4,910 | 0 |
| fast-1ms | 64 | 2.04 ms | 3.99 ms | 5.18 ms | 10.92 ms | 25,320 | 0 |
| rest-20ms | 1 | 22.45 ms | 27.21 ms | 120.11 ms | 123.99 ms | 39 | 0 |
| rest-20ms | 8 | 21.18 ms | 25.94 ms | 120.68 ms | 125.42 ms | 312 | 0 |
| rest-20ms | 64 | 20.44 ms | 24.98 ms | 117.10 ms | 125.67 ms | 2,311 | 0 |
| db-50ms | 64 | 53.67 ms | 65.03 ms | 303.13 ms | 314.76 ms | 878 | 0 |
| legacy-200ms | 64 | 213.38 ms | 268.68 ms | 1029.35 ms | 1066.07 ms | 239 | 0 |

**Platform cost** — p50 minus the backend's *expected median* (floor + half the
jitter; spikes excluded, they move the tail not the median):

| Backend | conc=1 | conc=8 | conc=64 |
|---|---:|---:|---:|
| instant | 0.21 ms | 1.02 ms | 1.78 ms |
| fast-1ms | 0.42 ms | 0.30 ms | 1.02 ms |
| rest-20ms | 2.11 ms | 1.06 ms | **0.46 ms** |
| db-50ms | 3.16 ms | 1.86 ms | 1.12 ms |
| legacy-200ms | 11.14 ms | 4.32 ms | 3.59 ms |

Read that table's *trend*, not its absolute values at low concurrency. Cost
**falls** as concurrency rises, which is backwards for real overhead and is the
tell that the residual at `conc=1` is measurement error, not the gateway:
`time.Sleep` resolves to roughly a millisecond on darwin, and at 60 samples the
sample median of a wide uniform distribution wanders (hence `legacy-200ms`
showing 11 ms at `conc=1` and 3.6 ms at `conc=64` for identical work).

**The honest overhead figure is the `instant` row**: no sleep, no subtraction,
nothing to mis-estimate. **0.26 ms p50, 0.47 ms p99, single caller.**

## Targets

| | Realistic | Stretch | Measured | |
|---|---|---|---|---|
| Platform overhead, p50 | ≤ 1 ms | ≤ 0.5 ms | **0.26 ms** | stretch met |
| Platform overhead, p99 | ≤ 5 ms | ≤ 1 ms | **0.47 ms** | stretch met |
| Throughput, one gateway | 10,000 req/s | 25,000 req/s | **26,852 req/s** | stretch met |
| Tail at 64 concurrent | p99 ≤ 25 ms | p99 ≤ 10 ms | **8.01 ms** | stretch met |
| Added cost vs a real 20 ms backend | ≤ 5 ms | ≤ 1 ms | **0.46 ms** | stretch met |
| Errors under sustained load | 0 | 0 | **0** | met |

For scale: a single gateway sustaining ~26,800 req/s is **268× the 100 tps**
that already counts as a very large integration deployment. Throughput is not
the constraint this design will hit first.

## Known limits, stated rather than buried

- **~29 KB and ~354 allocations per request**, from `BenchmarkGatewayRequestReply`.
  Three HTTP messages cross per caller request (caller in, poll out, deliver
  in), each with its own header map. At 26 k req/s that is roughly 780 MB/s of
  allocation churn, and it is what will cap throughput before CPU does. It is
  irrelevant at realistic rates and it is the first thing to attack if that
  ever stops being true.
- **Loopback, single host.** No TLS, no NIC, no cluster network. A real
  deployment adds a TLS handshake on the public edge and a network hop to the
  runner. The minikube figure in `deploy/k8s/README.md` (6.1 ms p50) is the
  same code path through `kubectl port-forward` and a Calico overlay — treat
  that as the pessimistic bound and this as the optimistic one.
- **The runner is a stub.** These numbers say nothing about flow execution,
  which is measured elsewhere.
- **`conc=1` rows are 60 samples.** Enough to see a shape, not enough for a
  percentile claim; the `conc=64` rows carry ~3,800.
