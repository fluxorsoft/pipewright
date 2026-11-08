# The lookup service on GKE pods (guideline)

Pipewright 1.4.2. Pipewright runs its Cloud SQL lookup service on Cloud Run. Some teams want it, or their own
service, on GKE pods instead: in the same cluster as their KServe models, on nodes they already pay for, with the
cluster's own network policies. This guideline and its manifest (`lookup.yaml`) show how. Pipewright does not
deploy it for you; it keeps syncing the table and grants your reader identity `SELECT`.

## When to use it

| You serve on | Use |
|---|---|
| Agent Platform endpoints | Pipewright's Cloud Run lookup service, and in a private network the lookup gateway (`lookup.mode: gateway`): an endpoint cannot reach into your network, so pods on GKE cannot help it either |
| KServe on private GKE | this guideline: the model's pods call the lookup pods inside the cluster, with no hop out of it |
| Your own service in the network | the lookup gateway in front of it (`type: http` with `lookup.mode: gateway`, the Pipewright custom solution) |

If you also want several models to share pods with your own serving image, KServe on GKE can serve several models
from one image. Agent Platform's shared pools cannot do that: they run Google's prebuilt TensorFlow or PyTorch image only.

## The use case

```yaml
model:
  serving:
    features: online
    online_store:
      type: cloudsql
      lookup:
        provider: own                                           # you run what reads the table
        reader: pw-churn-reader@<project>.iam.gserviceaccount.com  # Pipewright grants it SELECT at each sync
```

## Steps (the platform team, once)

1. Create the reader service account and make it an IAM database user:

   ```bash
   gcloud iam service-accounts create pw-churn-reader --project <project>
   gcloud sql users create pw-churn-reader@<project>.iam --instance <instance> --type cloud_iam_service_account
   gcloud projects add-iam-policy-binding <project> --role roles/cloudsql.client \
     --member serviceAccount:pw-churn-reader@<project>.iam.gserviceaccount.com
   gcloud projects add-iam-policy-binding <project> --role roles/cloudsql.instanceUser \
     --member serviceAccount:pw-churn-reader@<project>.iam.gserviceaccount.com
   ```

2. Let the pods act as it (Workload Identity):

   ```bash
   gcloud iam service-accounts add-iam-policy-binding pw-churn-reader@<project>.iam.gserviceaccount.com \
     --role roles/iam.workloadIdentityUser --member "serviceAccount:<project>.svc.id.goog[<namespace>/pw-lookup]"
   ```

3. The service's configuration, with the instance's server CA pinned (the service verifies the database with it):

   ```bash
   gcloud sql instances describe <instance> --format 'value(serverCaCert.cert)' > server-ca.pem
   python - <<'EOF'
   import json
   cfg = {"host": "<private IP>", "database": "features", "schema": "<schema>", "table": "pw_online_<usecase>",
          "key": "<entity column>", "features": ["<feature>", "..."], "pool_size": 8, "timeout_ms": 1000,
          "user": "pw-churn-reader@<project>.iam", "server_ca": open("server-ca.pem").read()}
   open("lookup.json", "w").write(json.dumps(cfg))
   EOF
   kubectl -n <namespace> create configmap pw-lookup-<usecase> --from-file lookup.json
   ```

   `pipewright resolve --env <env> --usecase <usecase>` shows the schema, table, key and features under
   `online_features`.

4. `kubectl apply -f lookup.yaml` (after replacing every `<...>`), then from a pod labelled
   `pipewright-usecase: <usecase>`:

   ```bash
   curl -s -X POST http://pw-lookup-<usecase>.<namespace>/lookup -d '{"entity_ids": ["<an id>"]}'
   ```

## What it costs and what to watch

- Two pods at 0.5 vCPU and 256 MiB requested sit on nodes you already run; the Cloud SQL instance is the same one.
- Each pod holds up to `pool_size` connections: pods x `pool_size` must fit the instance's connections (the
  Cloud SQL sizing in the online stores guide).
- The service answers inside the cluster without authentication of its own: the NetworkPolicy keeps it to the use
  case's pods, and the database grants keep it to the use case's table. Turn on the cluster's network policies.
- Logs carry counts and timings, never an ID or a value, as on Cloud Run.

Tested in the 1.4.2 battle test on the Enterprise private GKE environment (`docs/evidence/fr92-105-106-live-test/`).
