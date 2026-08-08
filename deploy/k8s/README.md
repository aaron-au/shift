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

This is a **full** deployment: hub, Postgres, control-plane CA, two gateways
and a runner, with mutual TLS on every control connection. There is no
reduced-security variant, because the binaries no longer offer one — see
"Why the order is enforced" below.

## What is here

| File | What |
|---|---|
| `00-namespaces.yaml` | two zones, labelled so the policies can select them |
| `05-hub.yaml` | the control plane: Postgres, the CA/KEK bootstrap job, `hubd`, the seed job |
| `10-netpol.yaml` | **the demonstration**: gateway egress denied outright, internal ingress from DMZ denied |
| `20-gateway.yaml` | the gateways: public NodePort + a **headless** control service |
| `30-runner.yaml` | the runner: no Service, no inbound port, discovers its gateways from the hub |

The gateway is a **StatefulSet**, and that is not incidental. A runner must
address each gateway *individually* — a poll parked on replica 1 is only
usable by replica 1 — so it needs stable per-pod DNS rather than one rotating
VIP. Routing runner polls through a load balancer would strand most of the
fleet behind whichever backend it picked.

## Why the order is enforced

Install order is **hub → adopt → gateway/runner**, and it is not a
convenience. Off-loopback gateway↔runner traffic requires mutual TLS on both
halves (ADR-0041), mTLS requires somebody to issue certificates, and the only
thing that issues them is the hub. So there is no ordering in which the hub
comes second, and no configuration in which it is absent:

- `gatewayd` **refuses to start** on a non-loopback control bind with no mTLS
  identity. A shared secret authenticates the *caller* to the gateway and tells
  a polling runner nothing about the gateway, so it is the half of the problem
  that does not matter: payload would go to whatever answered on the address.
- `runnerd` **refuses to start** pointed at a non-loopback gateway without an
  identity of its own, for the same reason from the other side.

The gateway holds **no configuration of its own** — no routes file, no roster,
no hand-placed certificate. It starts empty, generates its own key, and waits
to be adopted (ADR-0049). Everything it serves arrives over the control
channel from the hub.

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

## Install

```sh
# Build the images INTO minikube's docker daemon (no registry needed).
eval $(minikube -p minikube docker-env)
make images
```

### Stage 1 — the control plane

```sh
kubectl --context minikube apply -f deploy/k8s/00-namespaces.yaml
kubectl --context minikube apply -f deploy/k8s/10-netpol.yaml
kubectl --context minikube apply -f deploy/k8s/05-hub.yaml

kubectl --context minikube -n shift-internal rollout status deployment/hubd
kubectl --context minikube -n shift-internal wait --for=condition=complete job/shift-seed --timeout=120s
```

`shift-certs` mints the control-plane CA, the hub's certificate and the KEK
onto a shared volume, **once** — re-running keeps existing material rather than
regenerating, because regenerating would invalidate the trust every runner and
gateway already holds. `shift-seed` then provisions the *running* hub: a
trusted publisher key, signed connector artifacts, and a single-use runner
registration token.

Open an admin port-forward for the rest of the install:

```sh
kubectl --context minikube -n shift-internal port-forward svc/hubd 18400:8400 &
HUB=https://localhost:18400
ADMIN='Authorization: Bearer bundle-admin-token-dev-only'
```

### Stage 2 — register the gateways and hand out install tokens

Each gateway replica is a **separate** gateway with its own hub record, its own
one-time install token and its own identity. Register both, then deliver the
tokens to the DMZ as a Secret keyed by pod name:

```sh
for i in 0 1; do
  curl -sk -X POST -H "$ADMIN" -H 'Content-Type: application/json' \
    -d "{\"name\":\"shift-gateway-$i\",\"url\":\"https://shift-gateway-$i.shift-gateway-control.shift-dmz.svc.cluster.local:8444\"}" \
    "$HUB/api/v1/gateways" > /tmp/gw$i.json
done

kubectl --context minikube -n shift-dmz create secret generic shift-gateway-install \
  --from-literal=shift-gateway-0="$(python3 -c "import json;print(json.load(open('/tmp/gw0.json'))['install_token'])")" \
  --from-literal=shift-gateway-1="$(python3 -c "import json;print(json.load(open('/tmp/gw1.json'))['install_token'])")"
```

The token is a **bootstrap** credential, not a standing one. It proves the
pairing exchange once and is burned at adoption, after which the gateway's own
key is pinned by the hub and the token is worthless. Note the direction:
nothing in the DMZ reaches inward to fetch it — the material is pushed to the
DMZ, the same rule as everything else here.

> This is the one genuinely manual step in the install, and it is a gap rather
> than a design: the hub holds every fact needed to generate it.

### Stage 3 — the DMZ and the runner

```sh
kubectl --context minikube apply -f deploy/k8s/20-gateway.yaml
kubectl --context minikube apply -f deploy/k8s/30-runner.yaml

kubectl --context minikube -n shift-dmz      rollout status statefulset/shift-gateway
kubectl --context minikube -n shift-internal rollout status deployment/shift-runner
```

The hub dials each gateway, proves it holds that gateway's install token,
issues an identity, and starts pushing configuration:

```sh
kubectl --context minikube -n shift-dmz logs shift-gateway-0 | grep adopted
# {"msg":"adopted","event":"gateway.adopted","gateway":"a9aabe04-…"}
```

### Stage 4 — routes, flows and placement

Everything below is hub state. None of it is a file on the gateway or the
runner.

```sh
# Routes: the public paths, and which flow each one runs.
curl -sk -X POST -H "$ADMIN" -H 'Content-Type: application/json' \
  -d '{"path":"/orders","method":"POST","flow":"echo","auth_principal":"acme-erp","max_body_bytes":1048576}' \
  "$HUB/api/v1/routes"     # -> {"id":…,"token":"sgr_…"}   <- the CALLER's token

# Flows: deploy, publish, then expose as a webhook so the runner syncs it.
curl -sk -X PUT  -H "$ADMIN" -H 'Content-Type: application/json' \
  --data-binary @- "$HUB/api/v1/flows/echo" <<'JSON'
{"name":"echo","source":{"connector":"@webhook"},"sink":{"connector":"@response"}}
JSON
curl -sk -X POST -H "$ADMIN" "$HUB/api/v1/flows/echo/versions/1/publish"
curl -sk -X PUT  -H "$ADMIN" -H 'Content-Type: application/json' \
  -d '{"flow_name":"echo","enabled":true}' "$HUB/api/v1/webhooks/echo"

# Placement: what this runner IS. Hub-asserted, never self-declared.
RID=$(curl -sk -H "$ADMIN" "$HUB/api/v1/runners" \
      | python3 -c "import json,sys;print(json.load(sys.stdin)['runners'][0]['id'])")
curl -sk -X PUT -H "$ADMIN" -H 'Content-Type: application/json' \
  -d '{"labels":{"zone":"internal","workload":"general"}}' "$HUB/api/v1/runners/$RID/labels"
```

Each of those raises the gateway configuration generation, and the next push
carries it. Watch the two converge:

```sh
curl -sk -H "$ADMIN" "$HUB/api/v1/gateways" \
  | python3 -c "import json,sys;[print(g['id'],g['config_version'],g['pushed_version']) for g in json.load(sys.stdin)]"
```

`config_version == pushed_version` means every gateway is serving current hub
state. A gateway never applies an **older** generation than the one it holds —
a push that lost a race would otherwise roll it back onto a stale roster,
serving a runner the hub has since revoked.

## Prove it

The gateway's public listener is plain HTTP inside the cluster; TLS
termination in a real deployment sits in front of it. `minikube service --url`
blocks on the docker driver on macOS, and the images are distroless — no
shell, by design — so port-forward is the way in.

```sh
kubectl --context minikube -n shift-dmz port-forward service/shift-gateway 18453:8443 &
TOKEN=$(…the "token" from the route creation above…)
```

### 1. A request works end to end

```sh
curl -s -X POST http://127.0.0.1:18453/orders \
  -H "Authorization: Bearer $TOKEN" -d '{"id":1,"sku":"ABC"}'
# {"id":1,"sku":"ABC"}

curl -s -o /dev/null -w '%{http_code}\n' -X POST http://127.0.0.1:18453/orders \
  -H 'Authorization: Bearer wrong' -d '{}'
# 401
```

### 1b. A v3 DAG runs over the same path

Deploy this one the same way (`PUT /api/v1/flows/fanout`, publish, expose as a
webhook) and give it a route:

```json
{
  "name": "fanout",
  "start": "in",
  "steps": [
    {"id": "in",  "type": "source", "connector": "@webhook", "action": "ndjson", "onSuccess": "t"},
    {"id": "t",   "type": "tee",    "branches": ["a", "b"]},
    {"id": "a",   "type": "filter", "path": "$.id", "op": "exists", "onComplete": "m"},
    {"id": "b",   "type": "filter", "path": "$.id", "op": "exists", "onComplete": "m"},
    {"id": "m",   "type": "merge",  "inputs": ["a", "b"], "mode": "concat", "onSuccess": "out"},
    {"id": "out", "type": "sink",   "connector": "@response"}
  ]
}
```

Fan-out **and** fan-in in one graph (ADR-0029). The tee duplicates every record
to both branches and the merge concatenates them, so one record in comes back
as two — which makes the topology visible in the response rather than something
to take on trust:

```sh
curl -s -X POST http://127.0.0.1:18453/fanout \
  -H "Authorization: Bearer $FANOUT_TOKEN" -d '{"id":2,"sku":"XYZ"}'
# {"id":2,"sku":"XYZ"}
# {"id":2,"sku":"XYZ"}
```

Graphs like this were validated by the hub and drawable on the canvas but
**not executable** until the general segment compiler landed (issue #59), so
this doubles as the deployment-level check that they now run.

### 2. An unauthenticated caller cannot impersonate a runner

The control listener requires a client certificate on **every** connection.
There is no unauthenticated endpoint on it at all:

```sh
kubectl --context minikube -n shift-internal run probe --rm -i --restart=Never \
  --image=curlimages/curl:latest --command -- \
  curl -sk --max-time 6 -X POST -d '{}' \
  https://shift-gateway-0.shift-gateway-control.shift-dmz.svc.cluster.local:8444/api/v1/gw/poll
# exit 1 — the handshake never completes
```

Without that, anyone who reached the control port would be handed real inbound
payloads and could deliver forged responses to real callers.

### 3. The runner verifies the gateway, not just the CA

A hub-issued gateway certificate carries **no** subject alternative name — a
DMZ box has no stable hostname the hub can commit to at issue time — so the
runner pins the gateway's hub-assigned id, carried alongside the address in the
same discovery answer.

Chain validity alone would not be enough: every runner in the fleet holds a
certificate from the same CA, so one compromised runner could otherwise stand
up a listener, be dialled as a gateway, and be handed inbound payload to
answer.

### 4. The gateway genuinely cannot reach inward

The gateway pod has no shell to test from, so put a probe pod carrying the
gateway's **own label** into the DMZ — the NetworkPolicy selects on
`app: shift-gateway`, so the probe inherits exactly the gateway's egress rules:

```sh
kubectl --context minikube -n shift-dmz run dmz-probe --rm -i --restart=Never \
  --labels=app=shift-gateway --image=curlimages/curl:latest --command -- sh -c '
    curl -sk --max-time 6 https://hubd.shift-internal.svc.cluster.local:8400/readyz || echo "hub: blocked"
    curl -s  --max-time 6 https://example.com                                       || echo "internet: blocked"'
# hub: blocked
# internet: blocked
```

Now repeat step 1. **It still works.** The runner is serving public HTTP
traffic from a pod that the public-facing zone cannot open a connection to, in
either direction, at all — and the gateway is serving it while unable to reach
the hub that configures it.

Delete the probe when done; a leftover pod carrying `app: shift-gateway` is in
the gateway Service's endpoints and will receive real traffic.

### 5. Placement is enforced, not advisory

Placement is **hub-asserted** (ADR-0041): the runner proves WHO it is with a
client certificate, the hub says WHAT it is. A runner cannot label itself,
which is the point — otherwise eligibility would be a claim made by the thing
being placed.

Ask a route for a runner that does not exist:

```sh
curl -sk -X POST -H "$ADMIN" -H 'Content-Type: application/json' \
  -d '{"path":"/prod-only","method":"POST","flow":"echo","selector":{"environment":"production"},"auth_principal":"acme-erp"}' \
  "$HUB/api/v1/routes"

curl -s -o /dev/null -w '%{http_code}\n' -X POST http://127.0.0.1:18453/prod-only \
  -H "Authorization: Bearer $PROD_TOKEN" -d '{}'
# 503
```

Parked, healthy and willing — and not eligible, because the route asks for
`environment=production` and the hub has not asserted that about this runner.
**503 rather than a wrong runner.** Availability and eligibility are separate
questions, and the second one is not advisory.

### 6. No runner ⇒ 503, never a queue

```sh
kubectl --context minikube -n shift-internal scale deployment/shift-runner --replicas=0
curl -s -o /dev/null -w '%{http_code} in %{time_total}s retry-after=%header{retry-after}\n' \
  -X POST http://127.0.0.1:18453/orders -H "Authorization: Bearer $TOKEN" -d '{}'
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

- **Not production manifests.** The Postgres password, the hub admin token and
  the KEK policy are dev defaults sitting in plain sight — exactly like
  everything in `compose.yml`. A real deployment supplies each as a Secret and
  generates them per install (`openssl rand -hex 32`).
- **No TLS on the public listener.** Termination belongs in front of the
  gateway. Every *control* connection — hub→gateway, runner→gateway,
  runner→hub — is mutually authenticated and has no unencrypted mode.
- **Single replicas.** One hub, one runner, one Postgres with no backup. The
  gateway runs two replicas because HA there is a property worth demonstrating
  rather than asserting: each holds its own poll registry, and the runner parks
  on both.
- **The install tokens are handed over by hand** (stage 2). The hub holds every
  fact needed to generate that step; that it does not yet is a gap.
- **Readiness on the gateway is a TCP probe.** The control listener requires a
  client certificate on every connection, so the kubelet — which holds no
  identity in this trust domain — cannot make an application-level check.
  Issuing the orchestrator a control-plane certificate would be a far worse
  trade than a weaker probe.
