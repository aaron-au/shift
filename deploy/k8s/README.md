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

## Check your context first

**Every command below pins `--context minikube` deliberately.** These manifests
create namespaces, deny-all NetworkPolicies and NodePorts; applying them to a
cluster you did not mean to is an outage, and `kubectl` uses whatever context
happens to be current. Confirm before you start:

```sh
kubectl config current-context
```

If that is not `minikube`, do not drop the `--context` flags.

## Run it

```sh
# 1. Build the images INTO minikube's docker daemon (no registry needed).
eval $(minikube docker-env)
make images

# 2. Apply, in order.
kubectl --context minikube apply -f deploy/k8s/

# 3. Wait for both zones.
kubectl --context minikube -n shift-dmz      rollout status statefulset/shift-gateway
kubectl --context minikube -n shift-internal rollout status deployment/shift-runner
```

## Prove it

Open two port-forwards first. `minikube service --url` blocks on the docker
driver on macOS, and the gateway image is distroless — no shell, no `wget`, by
design — so `kubectl exec` into it is not available either. Port-forward needs
neither.

```sh
kubectl --context minikube -n shift-dmz port-forward service/shift-gateway 18443:8443 &
kubectl --context minikube -n shift-dmz port-forward pod/shift-gateway-0 18444:8444 &
kubectl --context minikube -n shift-dmz port-forward pod/shift-gateway-1 18445:8444 &
TOKEN=demo-token
```

### 0. An unauthenticated caller cannot impersonate a runner

```sh
curl -s -o /dev/null -w '%{http_code}\n' -X POST \
  http://127.0.0.1:18444/api/v1/gw/poll -d '{"wait_seconds":1}'
# 401
```

Without that, anyone who reached the control port would be handed real inbound
payloads and could deliver forged responses to real callers.

### 1. The runner parked itself on both gateways

```sh
curl -s http://127.0.0.1:18444/healthz   # gateway-0
curl -s http://127.0.0.1:18445/healthz   # gateway-1
# {"config_version":1,"configured":true,"runners_parked":1}
# {"config_version":1,"configured":true,"runners_parked":1}
```

Both, at the same time, from **one** runner. Each gateway's poll registry is
its own — which is why the runner polls every gateway rather than one, and why
`30-runner.yaml` lists both pod addresses instead of the service name.

### 2. A request works end to end

```sh
curl -s -X POST http://127.0.0.1:18443/orders \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"order":1,"customer":"acme"}'
# {"order":1,"customer":"acme"}

curl -s -o /dev/null -w '%{http_code}\n' -X POST http://127.0.0.1:18443/orders \
  -H 'Authorization: Bearer wrong' -d '{}'
# 401
```

### 3. The gateway genuinely cannot reach inward

The gateway pod has no shell to test from, so put a **probe pod carrying the
gateway's own label** into the DMZ — the NetworkPolicy selects on
`app: shift-gateway`, so the probe inherits exactly the gateway's egress rules:

```sh
kubectl --context minikube -n shift-dmz run dmz-probe \
  --image=busybox:1.36 --labels=app=shift-gateway --command -- sleep 600
kubectl --context minikube -n shift-dmz wait --for=condition=Ready pod/dmz-probe

RUNNER_IP=$(kubectl --context minikube -n shift-internal \
  get pod -l app=shift-runner -o jsonpath='{.items[0].status.podIP}')

kubectl --context minikube -n shift-dmz exec dmz-probe -- wget -T5 -qO- "http://$RUNNER_IP:8340/api/status"
# wget: download timed out

kubectl --context minikube -n shift-dmz exec dmz-probe -- wget -T5 -qO- http://1.1.1.1/
# wget: download timed out   (not even the internet — egress is denied outright)
```

Now repeat step 2. **It still works.** The runner is serving public HTTP
traffic from a pod that the public-facing zone cannot open a connection to, in
either direction, at all.

```sh
kubectl --context minikube -n shift-dmz delete pod dmz-probe
```

### 4. Placement is enforced, not advisory

`args[2]` is the `-labels` flag (see `30-runner.yaml`); flip it to staging
while the route still asks for production:

```sh
kubectl --context minikube -n shift-internal patch deployment shift-runner --type=json \
  -p='[{"op":"replace","path":"/spec/template/spec/containers/0/args/2","value":"-labels=environment=staging,workload=api"}]'
kubectl --context minikube -n shift-internal rollout status deployment/shift-runner

curl -s http://127.0.0.1:18444/healthz
# {"config_version":1,"configured":true,"runners_parked":1}   <- it IS parked

curl -s -o /dev/null -w '%{http_code}\n' -X POST http://127.0.0.1:18443/orders \
  -H "Authorization: Bearer $TOKEN" -d '{}'
# 503
```

Parked, healthy and willing — and not eligible, because the route asks for
`environment=production`. **503 rather than a wrong runner.** That pair of
outputs together is the whole point: availability and eligibility are separate
questions, and the second one is not advisory.

Put it back with the same patch and `production`.

### 5. No runner ⇒ 503, never a queue

```sh
kubectl --context minikube -n shift-internal scale deployment/shift-runner --replicas=0
curl -s -o /dev/null -w '%{http_code} in %{time_total}s retry-after=%header{retry-after}\n' \
  -X POST http://127.0.0.1:18443/orders -H "Authorization: Bearer $TOKEN" -d '{}'
# 503 in 0.009804s retry-after=1
```

Immediate, not a timeout. A gateway that held the request until a runner
appeared would be a queue, and a queue in the DMZ is durable state — the exact
thing this component exists not to have.

## Latency

Caller → gateway → parked runner → engine → back, measured two ways:

| Setup | min | p50 | p95 | max |
|---|---|---|---|---|
| loopback (both as local processes) | 0.4 ms | **0.5 ms** | 0.6 ms | 0.8 ms |
| this bundle (minikube + Calico, via port-forward) | 5.4 ms | **6.1 ms** | 8.6 ms | 18.9 ms |

The loopback figure is the code path's own cost. The cluster figure includes
`kubectl port-forward` (a userspace TCP relay over the API server) and the
Calico overlay, neither of which a real deployment pays — read it as an upper
bound, not as the gateway's overhead.

The hand-over to a parked runner is a channel send; the only structural
addition over the gateway dialling out is that deliver is a second request,
which is one extra RTT.

## What this bundle is NOT

- **Not production manifests.** No TLS on the public listener, no mTLS on the
  control listener, no hub, no Postgres. The control listener binds `:8444`
  and is guarded by a **shared secret plus NetworkPolicy** — gatewayd refuses
  to start on a non-loopback control bind with no secret, because an
  unauthenticated `/poll` lets anyone park a fake runner and be handed real
  payloads. ADR-0038 §6a replaces that with mutual TLS and a per-gateway
  identity bundle; not built yet.
- **The shared secret is duplicated across namespaces** (`shift-gateway-control`
  exists in both), because a Secret cannot be mounted across one. That is an
  honest cost of shared secrets and part of why mTLS supersedes it.
- **Not the configuration story.** The gateway's routes come from a ConfigMap
  and the runner's flow from a mounted file. In a real deployment **all** of it
  comes from the hub (ADR-0038 §6) — these files stand in for a push that is
  not built yet.
- The demo bearer token (`demo-token`), its digest, and the control secret are
  in the manifests in plain sight, deliberately: they are non-secret dev
  defaults, exactly like everything in `compose.yml`. Generate real ones per
  deployment with `openssl rand -hex 32`.
