# Phase 9 battle test: FR-92, FR-105, FR-106 (and the FR-104 stores they build on), Enterprise ent

Run 2026-11-08 on branch `phase-9`, in `ent` (pipewright-enterprise, us-central1): the private-managed blueprint, lean
(Agent Platform, GKE without Kubeflow for the lookup guideline; no Ray, CI or promotion), CMEK required by
organization policy, Cloud Run internal ingress and all-traffic egress, Google APIs over Private Service Connect, a
deny-all egress firewall, VPC Service Controls in dry run. Launcher built from the branch. Two use cases from the
features example (5,000 entities, 8 features), both on one shared pool (churn-shared: n1-standard-2, min 2, PyTorch
image, CMEK, the tenant's identity), each behind a private endpoint Terraform created:

- churnf: Cloud SQL behind the lookup gateway;
- churnx: the Pipewright custom solution, the gateway in front of a team's own lookup service (team-store-churnx,
  an internal Cloud Run service answering POST /lookup).

| # | Scenario | Result |
|---|---|---|
| S1 | validate with costs before anything exists | PASS: the pool's cost (2 x n1-standard-2, about $229 a month), the PyTorch Preview note, the private endpoint's cost |
| S2 | validate refusals | PASS: a machine with a pool; an unknown pool (names the pools there are); retries over 5; a gateway without callers |
| S3 | serving pools plan / apply / list | PASS: plan showed create and cost; apply created the pool through the API under the CMEK policy with the tenant's identity |
| S4 | Terraform endpoints, private by default | PASS after fixes F1, F2: churnf-endpoint and churnx-endpoint created private with CMEK; the service connection policy, subnet and private zone |
| S5 | deploy onto a Terraform private endpoint | PASS after fix F3 (the SDK lists private endpoints separately) |
| S6 | the gateway as created by the launcher | PASS: internal ingress, the tenant's lookup identity, CMEK, only the named caller may invoke it (the endpoint's identity does not) |
| S7 | one change to a pool at a time | PASS: churnx's deploy waited "for another change to settle (deploy churnf v1, by ...)" and ran after it settled |
| S8 | announcements | PASS: "every machine restarts, so churnf may get errors for about a minute"; announced before, and once settled (6 minutes after the operation) |
| S9 | deploy-model writes the private endpoint's DNS name | PASS: 1253321926-us-central1-486008751903.prediction.p.vertexai.goog -> 10.10.9.2, once Agent Platform had made the address |
| S10 | probe through both gateways from inside the VPC (100 calls, 4 entities and sometimes an unknown one) | PASS: churnf (Cloud SQL) p50 56.8 / p95 91.0 ms, 0 errors; churnx (custom solution) p50 89.1 / p95 156.5 ms, 2 errors on the team store's cold start (see F8); unknown IDs listed in not_found; the answering deployed model named |
| S11 | the public API refuses a private endpoint | PASS: 400 "cannot be accessed through the Public Vertex AI domain" |
| S12 | GKE lookup guideline | PASS: two pods, Workload Identity, IAM database login, the server CA pinned: 50 calls p50 11.4 ms, unknown IDs left out; a pod without the use case's label timed out (NetworkPolicy); logs counts only |
| S13 | pool resize in place under load | resize ran under the lock with both use cases named, settled; the load test through churnf's gateway exposed F7 (not the pool) |
| S14 | canary through the gateway on a pool | PASS: churnf v2 at 10% (a pool change, announced to churnf and churnx, settled): 100 calls through the gateway answered v1 85, v2 15; 0 errors |
| S15 | promote on a pool | PASS: v2 to 100%; removing v1 was a pool change under the lock, announced, settled |
| S16 | neighbours during pool changes, after the fixes | PASS: 45 minutes through churnx's gateway at about 5 calls a second across churnf's canary deploy and its promotion: 17 of 13,059 calls failed (0.13%), all inside the two change windows; no 401 after the token fix (the spike measured about 6% failing per change without retries) |
| S17 | doctor | PASS: the endpoint (private, Terraform, cost), the pool (machines, models, cost and each model's share), the gateway (URL, callers, retries) |
| S18 | Fleet View collectors | PASS: pools (exists, use cases, standing cost), each endpoint private and on its pool, each gateway's callers with calls, failures and retries from its logs |
| S19 | the gateway's logs | PASS: each request with the caller's identity, use case, deployed model and version, counts and timing; never an ID or a value |

## Not covered here

- The starter (open source) serving.yaml was validated and planned in tests, not applied live in this run.
- Pool billing per SKU is still to be read from the billing report (no billing export).

## Found live and fixed (each in core, the starter and Enterprise where Terraform; tests added)

| # | Finding | Fix |
|---|---|---|
| F1 | Terraform `coalesce` refused optional maps of different shapes | for-expressions with `try` (starter and Enterprise) |
| F2 | Agent Platform makes a private endpoint's address only with its first deployed model, so Terraform saw it empty and could not write the DNS record | Terraform keeps the zone; deploy-model writes the record once the address exists; the gateway reads the address from the endpoint and presents the endpoint's name; CI may write records in that zone |
| F3 | the SDK's `Endpoint.list` leaves out Private Service Connect endpoints | deploy-model also asks `PrivateEndpoint.list` |
| F4 | endpoint IAM is on the v1beta1 API only (v1 answers 404) | v1beta1 |
| F5 | a private endpoint checks `aiplatform.endpoints.predict` on the project, not the endpoint: an endpoint-level grant is refused | a custom role with only that permission, on the project, for each tenant's lookup identity (starter and Enterprise); the launcher no longer sets endpoint IAM |
| F6 | Enterprise's deny-all egress blocked the gateway from the private endpoints' subnet (timeouts) | the subnet is in the allowed internal ranges; the network module outputs them; blueprint test |
| F7 | access tokens from the metadata server were reused for a fixed 25 minutes, but the server hands out its current token, which may have minutes left: after about 30 minutes every gateway call failed 401 | reuse until five minutes before `expires_in`; also the lookup service's database login and the serving handler's lookup client (FR-104 code) |
| F8 | the gateway retries the endpoint, not the team's store: the store's cold start (scaled to zero) failed the first calls | documented in the serving example: keep the store warm or raise `http.timeout_ms` |
| F9 | a pool deploy also printed "deploying on n1-standard-2" | removed for pool deploys |
| F11 | undeploy said "the endpoint itself costs nothing", untrue for a private endpoint | says it still costs $0.01 an hour until Terraform removes it |
| F10 | the gateway called the team's store at its URL, the serving handler at `<url>/lookup` | the gateway calls `<url>/lookup` too |
