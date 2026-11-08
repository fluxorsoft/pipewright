# Serving: private endpoints, model co-hosting and the lookup gateway

Pipewright 1.4.2 adds three things to how a model is served. Each is off until someone turns it on, and a use case
that sets none of them deploys exactly as before: `pipewright deploy-model` creates its endpoint and deploys the
model on its own machines.

| What | Who turns it on | Why |
|---|---|---|
| **Endpoints in Terraform, public or private** (FR-92) | the platform team (starter `serving.yaml`, Enterprise `spec.serving`) | all infrastructure in one place; a private endpoint answers only inside your network |
| **Model co-hosting on shared pools** (FR-105) | the platform declares a pool; an ML team opts in with `model.serving.pool` | several small models share machines instead of paying for a machine each |
| **The lookup gateway** (FR-106) | the ML team (`lookup.mode: gateway`) | an endpoint cannot reach into your network, so something inside it must fetch the features |

The figures below come from a live spike in a secured environment on 2026-11-08 (`docs/evidence/fr92-105-106-spike/`).
Prices are Google's list prices for europe-west2, before discounts.

## The lookup gateway: why Pipewright built it

Online features live in a store. Some stores are Google APIs (BigQuery, Bigtable): a model on an Agent Platform
endpoint reaches them over Google's network with its own identity. Others live inside your network: Cloud SQL on a
private IP, or a store your team runs. A model on an endpoint cannot reach those. The spike checked it from inside a
standard and a private endpoint alike:

| From inside the endpoint, to | Result |
|---|---|
| the lookup service (Cloud Run, internal) | 404: internal ingress refuses a caller from outside the network |
| Cloud SQL's private IP | timed out |
| the network's private address for Google APIs | timed out |
| public Google APIs | reachable |
| the internet | reachable |

Google's documentation says the same: a Private Service Connect endpoint is private for callers coming in, and
private egress from within the endpoint is not supported. Moving the store's service to GKE, instance groups or
Cloud Run functions does not change this: they all sit inside the network.

So Pipewright turns the call around. Instead of the endpoint calling the store, the caller calls Pipewright's lookup
service inside the network, which reads the features and calls the endpoint:

```
                     YOUR NETWORK                                 |  GOOGLE-MANAGED
                                                                  |
 Caller (an app, an agent, a batch job)                           |
   | POST /predict {"entity_ids": [...]}  + ID token              |
   v                                                              |
 +-----------------------------------------------+                |
 | The lookup gateway (Cloud Run, internal only, |                |
 | the launcher image Pipewright already ships)  |                |
 |  1. Cloud Run checks the token; the gateway   |                |
 |     logs the caller's identity                |                |
 |  2. reads the features --------------------------> Cloud SQL,   |
 |                                               |    or your own |
 |                                               |    lookup      |
 |  3. calls the endpoint with them, as its own  |    service     |
 |     identity; retries a busy endpoint         |                |
 +-----------------------------------------------+                |
   |                                                              |
   v                                                              |
 Private endpoint (an address in your subnet) =====================|==> the model's machines
                                                                  |    (no way back into
                                                                  |     your network)
```

What it gives you:

- **Private end to end.** Callers, the store and the endpoint stay inside your network, with IAM at each hop.
- **Who called.** Every request is logged with the caller's identity, the use case, the model version and counts,
  never an ID or a feature value. Fleet View shows callers per use case (Enterprise).
- **Retries.** A prediction is safe to repeat, so the gateway retries 503, 429 and dropped connections (three more
  tries, 0.25 to 2 seconds apart). On a shared pool this took a neighbour's change from about 6% of calls failing
  to 1 in 4,629.
- **Canary unchanged.** The traffic split stays on the endpoint: a 90/10 split through the gateway served 92/8 of
  100 calls.
- **Fast.** From inside the network, four entities a call, 100 calls:

| Path | p50 | p95 |
|---|---|---|
| Gateway to a private endpoint, lookup included | **39 ms** | **52 ms** |
| Gateway to a standard endpoint, lookup included | 93 ms | 115 ms |
| The Cloud SQL lookup alone, through the gateway | 23 ms | 31 ms |

What was ruled out: anything that opens your store or the lookup service to the internet, and endpoints peered with
your network (they allow one model and no traffic split, so no canary).

### Turning it on

```yaml
model:
  serving:
    features: online
    online_store:
      type: cloudsql                 # or http: your own lookup service inside the network (below)
      lookup:
        mode: gateway                # default lookup: the endpoint calls the lookup service itself
        callers:                     # who may call the gateway (run.invoker); required
          - serviceAccount:checkout-app@acme-prod.iam.gserviceaccount.com
        # retries: 3                 # default; 0 turns them off
```

`deploy-model` then creates the endpoint first (the gateway calls it), then the lookup service with the gateway, and
lets the gateway's identity predict on that one endpoint. The callers, not the endpoint, may invoke the service.
"How to call it" points callers at the gateway:

```bash
curl -X POST "$GATEWAY_URL/predict" -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
  -H "Content-Type: application/json" -d '{"entity_ids": ["c-1", "c-2"]}'
# {"predictions": {"c-1": 0.81, "c-2": 0.12}, "not_found": [], "deployed_model": "...", "model_version": "3"}
```

An entity the store does not have is listed in `not_found` and never scored. The model receives rows in its feature
order, as from any caller that sends features, so the serving handler needs no store settings.

### The Pipewright custom solution: the gateway in front of your own store

Many teams run their own lookup service: fed from Pipewright's Parquet export, or a store they already have. Inside
a private network an endpoint cannot reach it either. With `type: http` and `mode: gateway`, Pipewright's gateway
sits in front of it:

```yaml
online_store:
  type: http
  http: { url: "https://features.internal.example.com", audience: "https://features.internal.example.com" }
  lookup:
    mode: gateway
    callers: [serviceAccount:checkout-app@acme-prod.iam.gserviceaccount.com]
    max_instances: 5                 # the gateway's own size, as for the Cloud SQL lookup service
```

Your service answers `POST /lookup {"entity_ids": [...]}` with `{"features": {id: {name: value}}}` (the same contract
as `type: http`), and the gateway calls it with an ID token for `audience`. You build the store; Pipewright brings
the private path, the caller log, the retries and the canary.

## Private endpoints (FR-92)

The platform lists the use cases whose endpoint Terraform creates:

```yaml
# starter serving.yaml (Enterprise: spec.serving, where private is the default)
endpoints:
  churn: { tenant: churn, private: true, callers: [] }
private:
  subnet: 10.61.0.0/28
```

Terraform creates `<use case>-endpoint` with Pipewright's labels (and the CMEK key in Enterprise), and for a private
one: a service connection policy (service class `gcp-vertexai`) so Agent Platform makes the endpoint's address in
your subnet, a private DNS zone for `prediction.p.vertexai.goog` and a record for the endpoint. Terraform ignores the
models on it: `deploy-model` deploys them with machine type and replicas from CI/CD, exactly as before, and never
creates an endpoint for a use case listed here (a public endpoint must never stand in for a private one).

| Item | Governance | Security | Cost |
|---|---|---|---|
| Private endpoint | in Terraform, reviewed as code | inside the network only; callers from allowlisted projects; IAM predict | $0.01 an hour (about $7.30 a month) plus $0.004 to $0.01 per GiB |
| Service connection policy and subnet | one per network | Agent Platform may create addresses only in that subnet | free |
| Private DNS zone | one per network | names resolve only inside it | about $0.20 a month |

A private endpoint presents a certificate Google signs for `*.prediction.p.vertexai.goog`, not one from a public
authority. Google's guidance is to fetch it and trust it, and to call the DNS name, not the IP. The gateway and
`pipewright model predict` do this, checking the certificate names that domain. From outside the network,
`model predict` and the BigQuery ML parity check say the endpoint cannot be reached, and `deploy-model` never rolls a
version back for that reason.

## Model co-hosting on shared pools (FR-105)

Only when an ML team asks for it. A pool is a set of machines several models share, on one endpoint or several:

```yaml
# the platform: starter serving.yaml pools, Enterprise spec.serving.pools
pools:
  churn-shared:
    tenant: churn
    machine: n1-standard-4
    min_replicas: 2
    max_replicas: 3
    image: europe-docker.pkg.dev/vertex-ai/prediction/tf2-cpu.2-15:latest
    network: psc                 # the kind of endpoint it serves
    managed_by: ci               # pipewright serving pools apply, in CI/CD

# the ML team, in the use case
model:
  serving:
    enabled: true
    pool: churn-shared           # leave it out to deploy on the use case's own machines
```

`pipewright serving pools plan --env <env>` shows what would change and what each pool costs;
`pipewright serving pools apply --env <env>` creates missing pools and resizes changed ones in place (never deletes
one); `pipewright serving pools list --env <env>` shows the use cases on each.

What Agent Platform allows, and what Pipewright checks for you:

| Fact (Google's documentation and the spike) | What Pipewright does |
|---|---|
| Only Google's prebuilt TensorFlow (generally available) or PyTorch (Preview) serving image; one image for the whole pool | refuses another image, and a model whose image is not the pool's. Custom serving images stay on dedicated machines (or KServe on GKE, which can serve several models from one image) |
| PyTorch co-hosting is a Preview | allowed outside production; in production only with `policies.serving.pools.allow_preview: true` |
| Each machine runs one model server that loads every model; container environment variables are the pool's (the first model's) | refuses a pool model whose container sets any variable; the model's settings travel in `serving.json` with its files |
| Every deployed model must run as the pool's service account | deploys with it (the tenant's runner by default) |
| A pool serves standard or private endpoints, not both (mixing failed twice) | refuses a use case whose endpoint is the other kind |
| A pool has at least one machine and does not scale to zero | `min_replicas` at least 1; the plan shows the standing cost |
| The Terraform resource cannot set a pool's CMEK key or service account, and every change rebuilds it | `managed_by: ci` is the default; `terraform` is refused where CMEK is required (Enterprise) |
| Every deploy, undeploy or resize restarts all the pool's machines, sometimes minutes after Google reports it done; the other models got about a minute of errors each time | see below |

### What a change to a pool does to its neighbours

| Pool during a change | Calls to untouched models that failed |
|---|---|
| 1 machine | 557 of 9,980 (about 50 seconds of near-total failure per change) |
| 2 machines | 419 of 6,782 (about half the calls, one window per machine) |
| 2 machines, callers through the gateway with retries | 1 of 4,629 |

So Pipewright:

- keeps at least 2 machines on a pool in production (`policies.serving.pools.min_replicas`, default 2 in an
  environment whose name has `prod`, 1 elsewhere with a warning);
- makes changes to a pool one at a time: a lock per pool, so a second deploy waits;
- announces each change to every team on the pool (an event, `serving.pool.change`, before and once it settled; the
  environment's alert channels and Fleet View receive it), and waits for the machines to finish restarting
  (`policies.serving.pools.settle_seconds`, default 360) before the next change runs;
- recommends the gateway's retries, or the same retries in callers that do not use it.

### What it costs

Ten small models on their own `n1-standard-2` (one machine each) cost about $1,147 a month. On one `n1-standard-4`
pool with two machines, about $458, if they fit in its memory. Models on a pool are not isolated from each other: a
busy model slows the others. `doctor` shows each model's share of its pool.

## Who may do what

| Who | Grant | Why |
|---|---|---|
| The gateway's identity (the tenant's lookup identity) | `aiplatform.user` on its one endpoint (resource IAM, set by the launcher) | calls the endpoint |
| The gateway's callers | `run.invoker` on the use case's lookup service | call `/predict` |
| An endpoint's callers (direct) | `aiplatform.user` on that endpoint (Terraform, `callers`) | predict without the gateway |
| CI/CD | create, patch and get deployment resource pools; write the pool's lock under `serving/pools/` in the artifacts bucket | `serving pools apply`, deploys onto pools |
| Fleet View's collector (Enterprise, with `spec.serving`) | `logging.viewer` | counts the gateway's callers from its request logs |

## Lookups on GKE

KServe on private GKE calls a lookup service inside the cluster directly. The guideline and a tested manifest are in
[examples/gke-lookup/](../examples/gke-lookup/).
