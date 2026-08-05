# SHIFT on Kubernetes — the DMZ walkthrough

This bundle exists to make one claim **falsifiable** rather than merely
asserted:

> Nothing in the DMZ ever initiates a connection inward (ADR-0038 §2).

The gateway runs in `shift-dmz` under a NetworkPolicy that denies it **all
egress** — it cannot open a connection to anything, anywhere. The runner runs
in `shift-internal`, which denies **all ingress from the DMZ**, and has no
Service and no inbound port at all.

Then a request from outside the cluster runs through the gateway, through the
runner, and back.

If the direction claim were wrong, that would not work.

## What is here

| File | What |
|---|---|
| `00-namespaces.yaml` | two zones, labelled so the policies can select them |
| `10-netpol.yaml` | **the demonstration**: gateway egress denied outright, internal ingress from DMZ denied |
| `20-gateway.yaml` | the gateway: public NodePort + a **headless** control service |
| `30-runner.yaml` | the runner: no Service, no inbound port, polls both gateways |

The gateway is a **StatefulSet**, and that is not incidental. A runner must
address each gateway *individually* — a poll parked on replica 1 is only
usable by replica 1 — so it needs stable per-pod DNS rather than one rotating
VIP. Routing runner polls through a load balancer would strand most of the
fleet behind whichever backend it picked.

## Prerequisites

A CNI that actually enforces NetworkPolicy. Minikube's default does **not**,
which would make this whole walkthrough a no-op that appears to pass:

```sh
minikube start --cni=calico --memory=4096 --cpus=4
```

## Run it

```sh
# 1. Build the images INTO minikube's docker daemon (no registry needed).
eval $(minikube docker-env)
make images

# 2. Apply, in order.
kubectl apply -f deploy/k8s/

# 3. Wait for both zones.
kubectl -n shift-dmz      rollout status statefulset/shift-gateway
kubectl -n shift-internal rollout status deployment/shift-runner
```

## Prove it

### 1. The runner parked itself on both gateways

```sh
kubectl -n shift-dmz exec shift-gateway-0 -- wget -qO- localhost:8444/healthz
# {"config_version":1,"configured":true,"runners_parked":1}
kubectl -n shift-dmz exec shift-gateway-1 -- wget -qO- localhost:8444/healthz
# {"config_version":1,"configured":true,"runners_parked":1}
```

Both, at the same time, from one runner. Each gateway's poll registry is its
own — that is why the runner polls every gateway rather than one.

### 2. A request works end to end

```sh
GW=$(minikube service -n shift-dmz shift-gateway --url | head -1)
curl -s -X POST "$GW/orders" \
  -H 'Authorization: Bearer demo-token' \
  -d '{"order":1,"customer":"acme"}'
# {"order":1,"customer":"acme"}
```

### 3. The gateway genuinely cannot reach inward

```sh
kubectl -n shift-dmz exec shift-gateway-0 -- wget -T3 -qO- http://shift-runner.shift-internal:8340/
# hangs, then fails — there is no route, no Service, and no egress
```

Repeat step 2 afterwards. It still works. The runner is serving public traffic
from a pod the public-facing zone cannot open a connection to.

### 4. Placement is enforced, not advisory

```sh
kubectl -n shift-internal set args deployment/shift-runner \
  runnerd -- -labels=environment=staging,workload=api
kubectl -n shift-internal rollout status deployment/shift-runner

curl -s -o /dev/null -w '%{http_code}\n' -X POST "$GW/orders" \
  -H 'Authorization: Bearer demo-token' -d '{}'
# 503
```

A runner is parked, healthy and willing — and the route asks for
`environment=production`, so it is not eligible. 503 rather than a wrong
runner.

### 5. No runner ⇒ 503, never a queue

```sh
kubectl -n shift-internal scale deployment/shift-runner --replicas=0
curl -s -o /dev/null -w '%{http_code} in %{time_total}s\n' -X POST "$GW/orders" \
  -H 'Authorization: Bearer demo-token' -d '{}'
# 503 in 0.001s
```

Immediate, not a timeout. A gateway that held the request until a runner
appeared would be a queue, and a queue in the DMZ is durable state — the exact
thing this component exists not to have.

## Latency

Measured on a loopback dev box (`gatewayd` and `runnerd` as local processes,
the same code path as above without the cluster network):

```
min=0.4ms  p50=0.5ms  p95=0.6ms  max=0.8ms
```

That is caller → gateway → parked runner → engine → back. The hand-over to a
parked runner is a channel send; the only structural addition over the gateway
dialling out is that deliver is a second request, which is one extra RTT.

## What this bundle is NOT

- **Not production manifests.** No TLS, no mTLS on the control listener, no
  hub, no Postgres, no secrets. The control listener binds `:8444` here and is
  kept internal by NetworkPolicy alone; ADR-0038 §6a gives it mutual TLS and an
  identity bundle, and that is not built yet.
- **Not the configuration story.** The gateway's routes come from a ConfigMap
  and the runner's flow from a mounted file. In a real deployment **all** of it
  comes from the hub (ADR-0038 §6) — these files stand in for a push that is
  not built yet.
- The demo bearer token (`demo-token`) and its digest are in
  `20-gateway.yaml` in plain sight, deliberately: it is a non-secret dev
  default, exactly like everything in `compose.yml`.
