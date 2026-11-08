# Spike: private endpoints, shared serving pools and the lookup gateway (FR-92, FR-105, FR-106)

Run 2026-11-08 (00:00 to 02:45 UTC) in `pipewright-enterprise` (us-central1), then torn down. The `ent` guardrails were
applied as project organization policies for the spike (Cloud Run internal ingress and all-traffic egress, no Cloud SQL
public IP, CMEK required for Agent Platform, Artifact Registry, Cloud Run, Cloud SQL and Cloud Storage), with Google
APIs over Private Service Connect (`vpc-sc` bundle) and private DNS, as the blueprint builds them. No GKE.

What ran: Cloud SQL for PostgreSQL 16 (private IP, CMEK, IAM login, 5,000 rows of 8 features); a prototype gateway
on Cloud Run (`code/gw.py`: internal ingress, IAM, CMEK, direct VPC egress) with `POST /lookup` and `POST /predict`;
models in Google's prebuilt PyTorch image (`code/handler.py`: scores a feature row, or reports what it can reach from
inside the endpoint); endpoints: standard and Private Service Connect (with Google's PSC automation), all CMEK;
deployment resource pools with CMEK and their own service account; a probe and a load generator as Cloud Run jobs in the
VPC. Counts, timings, names and outcomes only; no feature values.

## Results

| Question | Result |
|---|---|
| Can a model on an endpoint reach into the VPC? | **No**, from a standard and from a PSC endpoint alike: the internal lookup service answered 404 (internal ingress), Cloud SQL's private IP and the VPC's PSC address for Google APIs timed out. Public Google APIs and **the internet** (HTTP 200) were reachable from inside the endpoints. From the VPC itself (control): Cloud SQL connected in 2.6 ms, the internet was unreachable (no NAT) |
| Does the gateway work? | Yes, with both endpoint types, 0 errors in 500 calls. The gateway read the caller's identity from the ID token Cloud Run had checked |
| Latency from inside the VPC, 4 entities a call, 100 calls each (p50 / p95 ms) | gateway lookup only 22.8 / 30.9; endpoint direct with features: PSC 15.4 / 26.4, standard 69.5 / 90.8; **gateway to PSC endpoint 39.0 / 51.8**; gateway to standard endpoint 92.8 / 114.7 |
| Canary through the gateway | split 90/10 on a PSC endpoint, both versions on one pool: 92/8 of 100 calls |
| A pool with CMEK and a service account under the CMEK policy | Created in 2 s through the API (the Terraform resource cannot set either) |
| Deploying onto a pool | First model 843 s (machines), later models 235 to 559 s. Every deployed model must name the pool's service account (otherwise 400 FAILED_PRECONDITION) |
| Two endpoints on one pool | Two PSC endpoints: yes. A **standard endpoint added to a pool serving a PSC endpoint failed twice** (operation error 13 INTERNAL, about 10 minutes each); a standard endpoint on its own pool: yes |
| Per-model settings on a pool | Each replica runs **one** model server (TorchServe) loading every model of the pool, each from its own files. Model files are per model; **container environment variables are the pool's** (every model ran with the first model's) |
| Resizing a pool | In place with PATCH, models deployed: max 2 to 3 in 95 s, 3 to 2 in 155 s, min 1 to 2 in 90 s |
| What a change does to the other models on the pool | Every deploy, undeploy or resize **rolls all replicas** (each loads all models), sometimes minutes after the operation reports done. Load test through the gateway to untouched models, about 10 calls a second: **min 1**: about 50 s of near-total 503s per change (557 of 9,980 calls failed over 3 changes); **min 2**: about half the calls failed, one window per replica (419 of 6,782); **min 2 with the gateway retrying 503s** (4 tries, 0.25 to 2 s backoff): **1 of 4,629** failed, 116 needed a second try |
| How a pool is billed | Not confirmed: no billing export exists; to read from the billing report (per SKU) after a day |

## What it means for the build

1. FR-106 is required for Cloud SQL with Agent Platform endpoints; the egress limit is confirmed live.
2. The gateway in front of a PSC endpoint is the fastest private path measured (39 ms p50 end to end, with the lookup).
3. Pools: Pipewright creates them through the API (CMEK and identity); the deployed model names the pool's identity;
   one endpoint network type per pool; per-model configuration goes in the model's files, never in container
   environment variables; resizing is in place.
4. A pool change disturbs every model on it. Production pools need min 2 and retries (the gateway's, or the caller's);
   pool changes are announced and serialised.
5. Endpoints can reach the internet from inside the model container: a governance note for the Enterprise docs.

## Files

`runs/`: the probe (egress, latency, canary, caller), the three load tests (per 10 seconds: ok, errors, error kinds),
the findings log with times. `code/`: the gateway prototype, the handler, the helper that made the endpoints and pools.
