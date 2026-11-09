# 1.4.2 QA round (phase 9) on the qa branches

Branch `qa` in core, starter and Enterprise, cut from phase-9 on 2026-11-08 after the online store live test
(`../fr104-live-test/`) and the serving battle test (`../fr92-105-106-live-test/`). The QA was the ebook: every chapter
from 13 on re-run live on 1.4.2 as the end-to-end test, four new chapters written live, the GKE appendix re-run, then
Enterprise's secured environment and the Enterprise end-to-end repository. Every bug found was fixed on the qa branches;
design questions were collected and decided at the end.

## Static, at release

| Check | Result |
|---|---|
| Core: ruff (whole repository) and pytest | clean; 903 passed |
| Core: the churn example's own tests, package build and package check | passed |
| Enterprise: ruff and pytest (against core 1.4.2) | clean; 182 passed |
| Enterprise: terraform fmt, validate, terraform test (blueprint 56, Fleet View module), module docs | passed |
| Starter: terraform validate | passed |

## Open source, live (the ebook, on pw-ebook and pw-ebook-prod)

| Chapter | What ran | Found and fixed |
|---|---|---|
| 13 Online features | Bigtable views, online endpoint | FR-110 (`features sync` starts a view's sync) |
| 14 to 19 | Monitoring, retraining, CI, promotion, the closed loop, the app | FR-109 (drain before undeploy), FR-111 (cut-offs of a performance value), FR-112 (`model.undeployed`), FR-113 (`release.live` is a decision), FR-114 (online-trained model refused in request mode), FR-115 (redeploy with the right identity) |
| 20 Build with an agent | The agent's draft from BigQuery | design item 2 (personal data warning) |
| 21 Integrity (new) | Lock, renewer, a swapped file refused, `restore-files` | FR-116 (lineage shows current protection), FR-117 (who wrote a changed file; restore first) |
| 22 Feature contracts (new) | Contract 1.0.0, a retyped column refused, deploy, scoring, training | design item 3 (`sql_files`) |
| 23 Online stores and the gateway (new) | BigQuery with BI Engine, Cloud SQL behind the gateway, the team's own store from the export | FR-118 (gateway accepts request-trained models), FR-119 (export for an http store), design items 4 to 8 |
| 24 Private serving and shared machines (new) | Private endpoints, a pool with two models, a neighbour probe during a pool change | starter: serving manifest types, endpoint `id_salt` |
| Appendix C (GKE) | KServe serving the approved copy, the lookup service on pods, co-hosting with KServe, KubeRay | none |

Also: FR-108 built (a k-means segment size gate, Chapter 8); FR-107 (repeatable synthetic runs) logged, not built.

## Enterprise, live (ent: private-managed with GKE, CMEK, enforced perimeter)

Rebuilt from qa with GKE, Kubeflow, KServe, Cloud SQL, two private endpoints and a shared pool. Ran: training on
Kubeflow and Agent Platform, approvals and integrity, KServe deploy and predict, Ray on KubeRay, the online setup job,
sync, the lookup gateway inside the network (100 calls, 0 errors, p50 59 ms), deploy on the pool with the neighbour
announcement. Found and fixed: Fleet View reads `model.undeployed`; `lookup.gateway` grant without `spec.serving`;
agents from their well-known formats (every apply replaced the CMEK grants); integrity module counts; `id_salt`.
Not run: promotion to ent-prod and Fleet View across both (ent-prod's apply was blocked by the session's permissions;
nothing billable was built there).

## Design decisions (user: "go ahead with recommendations", 2026-11-09)

CONTEXT.md row QA-1.4.2-D: drain on by default; personal data warning; `sql_files`; export schedule for an http
store; readers in either form; starter `lookup.egress` and the launcher's invoker grant on a team store; platform
`export_readers`; undeploy removes the lookup service. Unit-tested (`tests/test_design_items_1_4_2.py`, Enterprise
blueprint test); items 6 to 8 were not run live, since everything was torn down after the decisions.

Everything was torn down on 2026-11-09 (pw-ebook 153, pw-ebook-prod 118, ent 270 destroyed).
