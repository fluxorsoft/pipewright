# FR-104 grants: every permission the online stores need, and the ones the live test found

Who | Grant | On | Why | Found
---|---|---|---|---
Online store admin (`<prefix>-online-admin`) | IAM database user with `cloudsqlsuperuser` (`database_roles`) | the Cloud SQL instance | creates the database (so `public` is never a tenant's) and the tenants' grants, with no password user | design (spike d)
Online store admin | `roles/cloudsql.client`, `roles/cloudsql.instanceUser` | project | connect over IAM login | design
Online store admin | `roles/storage.objectViewer`, the manifest object only (Enterprise; starter: bucket) | artifacts bucket | the setup job reads the manifest | design
Tenant runner (the sync) | IAM database user; in the database `CONNECT, CREATE` and `USAGE, CREATE` on `public` (by online-setup) | instance, database | creates its schema and tables; owns them, so other tenants cannot write them | design
Tenant runner | `roles/cloudsql.client`, `roles/cloudsql.instanceUser` | project | connect; read the instance's server CA (`cloudsql.instances.get`) | design
Tenant runner | `roles/storage.objectAdmin` on `online-export/<tenant>/` and `online-sync/<tenant>/` | artifacts bucket | writes the extract, manifest and sync record; deletes staging; reads its sync request | design (starter only with isolate_tenant_artifacts)
Lookup identity (`<prefix>-pwl-<tenant>`) | IAM database user; `CONNECT` (online-setup); `USAGE` on the schema and `SELECT` on each table (granted by the sync) | instance, database | the lookup service reads the table | design
Lookup identity | `roles/cloudsql.client`, `roles/cloudsql.instanceUser` | project | connect over IAM login | design
Launcher | `roles/run.admin` | project | runs the sync jobs with overrides; creates and updates lookup services and sets their invokers (Cloud Run cannot scope create by service name: spike d) | design (spike d)
Launcher | `roles/iam.serviceAccountUser` | each lookup identity; each runner (starter) | deploys lookup services as the lookup identity; runs the sync job as the runner | design
Launcher | `roles/storage.objectCreator` on `online-sync/` | artifacts bucket | writes each scheduled sync's request (manifest and released configuration) | **found live** (2026-11-07): an isolated runner could not read the manifest or contracts, so the launcher hands it a request; the launcher then could not write it
Launcher | custom role `bigquery.jobs.listAll` | project | the watch counts BigQuery lookups billed as full table reads (bigquery type) | design (spike d: a byte cap cannot guard)
Tenant runner (endpoint identity) | `roles/run.invoker`, `roles/run.viewer` on its use case's lookup service (set by the launcher) | the service | the served model calls it, and reads its URL once | design
Cloud SQL service agent | `roles/cloudkms.cryptoKeyEncrypterDecrypter` | the cloudsql key (Enterprise) | CMEK on the instance | design (spike d)
Launcher | `roles/cloudsql.viewer` | project | reads the instance's server CA and pins it in each lookup service, so the service verifies the database | **found live** (2026-11-07): the launcher could not read it and the service would have encrypted without verifying; the launcher now refuses to create a service without the CA
Lookup services (Enterprise) | ingress `INGRESS_TRAFFIC_INTERNAL_ONLY`, egress `ALL_TRAFFIC` | Cloud Run | the blueprint's organization policies `run.allowedIngress` and `run.allowedVPCEgress` | **found live** in ent (2026-11-07): the launcher's create was refused until both matched
BigQuery lookups (Enterprise) | `destinationEncryptionConfiguration` with the BigQuery key | each lookup | the CMEK organization policy covers query results | **found live** in ent: HTTP 412 without it
