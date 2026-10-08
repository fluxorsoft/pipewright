# Onboarding an ML team to Pipewright

For ML engineers starting a new use case, and for the platform team that prepares their tenant. Pipewright's
ML persona assumes the infrastructure already exists; this guide covers the handshake and the commands.

## 1. Platform team: prepare the tenant (once per team and environment)

| Item | What the ML team receives |
|---|---|
| Tenant name | e.g. `nimbus` |
| Runner service account | runs the team's pipeline steps |
| BigQuery dataset | the team creates its own tables in it with `tables sync` |
| Landing prefix | e.g. `incoming/nimbus/`; files dropped here start the team's runs |
| Online serving allowed | yes / no |
| Environment manifest | published at `gs://<artifacts-bucket>/pipewright/environment.json` |

The shared launcher picks up new tenants from the manifest within a minute; no launcher redeploy is needed.
Schedules also need the Cloud Scheduler API enabled in the project. If the team's inference pipeline proposes
retrains (`retraining.mode: propose` or `candidate`), give the team:

| Item | Why |
|---|---|
| A way to reach the launcher | Public launchers: `roles/run.invoker` on it for the tenant's runner. Private ones: publish on the run requests topic (the blueprint grants it) |
| Approver groups | Registered in the manifest (`approver_groups`): a topic per group whose publishers are its members, pushed to the launcher's `/retraining/decide`. Use cases name the groups they require; `policies.retraining.required_approvers` adds groups to every use case |
| The tick | A Cloud Scheduler job calling `/retraining/tick` hourly (expiry, escalation) and weekly with `{"digest": true}` |
| Optional: a decision feed | A filtered subscription on the events topic (`attributes.tenant` and `attributes.decision_feed`), named in the tenant's `decision_feed` |

Anyone with Pub/Sub publish on the whole project (Owner, Editor, Pub/Sub roles) counts as a member of every
approver group: grant publish per approver topic only. `doctor` checks the groups, the emails and the feed, and
names such project-wide publishers.

## 2. ML team: set up the MLOps repository

```
team-mlops/
├── environments/
│   └── <env>.yaml          # points at the manifest the platform team published
└── usecases/
    └── <usecase>/          # created by `pipewright new-usecase`
        ├── usecase.yaml            what it is: tenant, pipelines, model, approvals
        ├── config/                 triggers, runtime (parameters, steps, tests), tables, data, training,
        │                           evaluation, operations: one concern per file
        ├── environments/           <env>.yaml: overrides per environment
        ├── training_pipeline.py    validate, tune, train, evaluate against the champion, register
        ├── inference_pipeline.py   score new data with the champion, monitor drift, propose retrains
        ├── components/  schemas/  tests/  data/
```

Tables live in BigQuery by default. `outputs.store: gcs` writes them as Parquet under your tenant's outputs
location instead (with BigQuery external tables of the same names, so SQL keeps working), and `both` does both.

Most changes are config: a gate, a slice, the tuning budget, a drift threshold or a machine size is a line in
`config/` or `steps:`, reviewed like any other change. The JSON Schemas in
`src/pipewright/config/schemas/` list every setting; [CONTEXT.md](../CONTEXT.md) explains how they fit together.

`environments/<env>.yaml`:

```yaml
apiVersion: pipewright/v1
kind: Environment
metadata: { name: <env> }
spec:
  manifest: gs://<artifacts-bucket>/pipewright/environment.json
```

Credentials: Application Default Credentials, or `GCLOUD_CONFIG=<gcloud configuration>`. Behind a
TLS-intercepting proxy also set `REQUESTS_CA_BUNDLE`.

## 3. ML team: create, check, release

| Step | Command | What it does |
|---|---|---|
| 1 | `pipewright new-usecase --name <usecase> --tenant <tenant> --owner <team>` | Copies the churn example: two pipelines, components, config, schemas, tests, sample data. Table names get the prefix `<usecase>_` |
| 2 | edit `config/`, `usecase.yaml`, components | Your data contract, features, gates, monitoring; your model code |
| 2b | `pipewright test --env <env>` | Runs `tests.command` (the example's helper unit tests) |
| 3 | `pipewright validate --env <env>` | Schema, overrides, references, files, table schemas |
| 4 | `pipewright doctor --env <env>` | Live checks: tenant resources, landing events, pipeline compiles, table plan, permissions. Each failure says who to ask |
| 5 | `pipewright tables sync --env <env>` | Creates your tables; later applies additive changes only |
| 6 | `git commit` | Releases are named after the commit; uncommitted work is marked `-dirty-` |
| 7 | `pipewright release --env <env>` | Builds your component image, compiles and uploads one template per pipeline, writes the contract, verifies, makes it live |
| 8 | `pipewright schedules sync --env <env>` | Creates your Cloud Scheduler jobs |
| 9 | drop a file in your landing prefix, or wait for the schedule | The shared launcher starts the pipeline the trigger names (landing: training by default) |
| 10 | `pipewright model list` / `model approve --version N --approve` | With `promote_model: manual`, an approved model is a candidate until you make it the default (the champion) |

CI (GitHub Actions): `pipewright ci init --environments dev,prod` writes `pipewright-pr` (validate, test,
doctor, dry runs on every pull request), `pipewright-release` (on merge: tables, release, schedules, alerts for
each changed use case, environment by environment) and `pipewright-operate` (by hand: run, status, rollback,
model approve, deploy, canary). Create a GitHub environment per name with the variables `GCP_WIF_PROVIDER`,
`PIPEWRIGHT_SERVICE_ACCOUNT` and `PIPEWRIGHT_PIP_SPEC` (set `PIPEWRIGHT_SCHEDULES=off` where Cloud Scheduler is not
enabled), and add required reviewers to production. On self-hosted runners use
`pipewright ci init --environments dev,prod --runs-on self-hosted,linux,mlops`; with a service account attached to
the runner, leave `GCP_WIF_PROVIDER` empty, and set `PIPEWRIGHT_SETUP_TOOLS=off` when the image already has Python
3.11 and uv.

Day to day:

```bash
pipewright status   --env <env>                       # what is live
pipewright rollback --env <env> --usecase <usecase>   # back to the previous release
pipewright release  --env <env> --component-image <repo>/<usecase>-components@sha256:...   # config-only change, no rebuild
pipewright deploy-model --env <env> --usecase <usecase> --dry-run          # then --approve to deploy
pipewright deploy-model --env <env> --usecase <usecase> --canary --approve # then --promote or --undeploy-canary
pipewright model list   --env <env> --usecase <usecase>                    # versions; DEFAULT is the champion
pipewright monitor status --env <env> --usecase <usecase>                  # drift, performance, open retrain cases
pipewright retrain show   --env <env> --usecase <usecase>                  # the decision pack of the open case
pipewright retrain approve --env <env> --usecase <usecase> --reason "..."  # or decline / snooze --because ... --days N
pipewright data erase   --env <env> --usecase <usecase> --keys-file ids.txt   # counts; add --confirm to delete
```

Sources: a run's `source_uri` is a `gs://` object (CSV or Parquet) or a `bq://project.dataset.table` table or
view. Teams fed from BigQuery set `triggers.landing: null` and start runs with schedules or the API; the runner
service account needs `roles/bigquery.dataViewer` on the source dataset.

API trigger (when `triggers.api: true`), for upstream jobs that produce data:

```bash
curl -X POST -H "Authorization: Bearer $(gcloud auth print-identity-token)" -H "Content-Type: application/json" \
  -d '{"usecase": "<usecase>", "pipeline": "inference", "source_uri": "gs://...", "run_key": "<unique per batch>"}'   <launcher-url>/run
```

`pipeline` defaults to `training`. The same `run_key` always maps to the same run, so retries never start
duplicates.

In a private environment the launcher only accepts traffic from inside the network (a call from outside gets
404). Use `pipewright run --env <env> --usecase <usecase> --pipeline inference --source-uri gs://... --run-key ...`:
it publishes the request to the environment's run requests topic and the launcher submits it. In components,
query BigQuery with `bigquery_client(project, ml_config)` from `pipewright_runtime` (private environments require
an encryption key on every query), and keep files your pipelines read under your landing or read prefixes.

## 4. What you own, and what you do not

| You own (ML team) | The platform team owns |
|---|---|
| Pipeline code, components, `usecase.yaml` | Projects, network, keys, perimeter (with infosec) |
| Tables inside your dataset | The dataset, service accounts, IAM |
| Releases, schedules, rollbacks | The launcher, Pub/Sub, Artifact Registry |
| Model versions, model deployment | Monitoring, budgets, the environment manifest |
