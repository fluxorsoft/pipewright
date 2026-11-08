# The serving example

The feature tables example served online (Pipewright 1.4.2). `usecase.yaml` shows every serving and online store
key: the keys it sets are this example's choices (Cloud SQL behind the lookup gateway), and the `# default` lines are
what Pipewright fills in when a key is left out. See the [churn](../churn/) example for every other option, and
[docs/SERVING.md](../../docs/SERVING.md) and [docs/ONLINE_STORES.md](../../docs/ONLINE_STORES.md) for what each
costs and why. The features themselves come from feature tables, as in the [features](../features/) example
(`data/load_tables.sh` loads the sample tables).

Start a use case from it:

```bash
pipewright new-usecase --name <name> --tenant <tenant> --owner <team> --from serving
```

`pipewright resolve --env <env> --usecase <name>` shows the result with every default filled in; `pipewright
validate --env <env>` says what the environment offers and what each choice costs before anything is created. This
README is not copied into your use case.

## Every case, on and off

Everything is off until it is set. Each case below changes only the lines shown; the rest of `usecase.yaml` stays.

| Case | Turn it on | Turn it off (the default) | Needs from the platform |
|---|---|---|---|
| Serve online at all | `serving.enabled: true` | `enabled: false` | `online_serving` for the tenant |
| Callers send entity IDs | `serving.features: online` | `features: request` (callers send values) | an online store |
| Bigtable (Feature Store) | leave `online_store` out | | the tenant's online store (`tenants.<t>.features.online_store`) |
| BigQuery with BI Engine | `online_store.type: bigquery` | | `features.online_store.types` has `bigquery`; a BI Engine reservation |
| Cloud SQL, the endpoint calls the lookup service | `type: cloudsql` | | `types` has `cloudsql`; the instance; a public endpoint (Agent Platform calls from outside the network) |
| Cloud SQL behind the lookup gateway | `lookup.mode: gateway` and `lookup.callers` | `mode: lookup` | as above; works with private endpoints |
| Your own store, called by the endpoint | `type: http` and `http.url` | | `types` has `http`; your store reachable from Agent Platform |
| Your own store behind the gateway (the Pipewright custom solution) | `type: http`, `lookup.mode: gateway`, `lookup.callers` | `mode` left out | `types` has `http`; a lookup identity for the tenant |
| Your own lookup service on Cloud SQL | `lookup.provider: own`, `lookup.reader` | `provider: pipewright` | `types` has `cloudsql` |
| Gateway retries | `lookup.retries: 0` to `5` | `retries: 0` turns them off (default 3) | |
| Only changed rows synced | `sync.mode: changed` | `mode: full` | |
| Parquet export for your own store | `export.enabled: true` | `enabled: false` | |
| Co-hosting on a shared pool | `serving.pool: <name>`, and no `machine`, `replicas`, `accelerator` or `scale_to_zero` | leave `pool` out: your own machines | a pool for your tenant (`serving.pools`), Google's TensorFlow or PyTorch image |
| A private endpoint | nothing in the use case | | the platform lists the use case (`serving.endpoints`, `private: true`) |

### Cloud SQL behind the gateway (this example)

```yaml
online_store:
  type: cloudsql
  cloudsql: { schema: my_team }
  lookup:
    mode: gateway
    callers: [serviceAccount:my-app@my-project.iam.gserviceaccount.com]
```

Callers send `POST <lookup service URL>/predict {"entity_ids": [...]}` with an ID token; `deploy-model` prints the
service and how to call it.

### Turn the gateway off: the endpoint calls the lookup service

```yaml
  lookup: {}                       # or leave lookup out: mode lookup, Pipewright's lookup service
```

Callers then call the endpoint with entity IDs. Only where Agent Platform can reach the lookup service (a public
endpoint, and the lookup service's ingress open to it, as the starter sets by default).

### Your own store behind the gateway (the Pipewright custom solution)

```yaml
online_store:
  type: http
  http: { url: "https://features.my-team.example.com" }    # answers POST /lookup {"entity_ids": [...]}
  lookup:
    mode: gateway
    callers: [serviceAccount:my-app@my-project.iam.gserviceaccount.com]
    min_instances: 1               # the gateway's size, as for the Cloud SQL lookup service
```

Keep your store warm (at least one instance), or allow for its cold start: the gateway waits for it up to
`http.timeout_ms` (default 1000) and retries the endpoint, not the store (found in the 1.4.2 live test).

### Co-hosting on a shared pool

```yaml
serving:
  enabled: true
  pool: my-tenant-shared           # no machine, replicas, accelerator or scale_to_zero with it
```

Your serving image must be the pool's (Google's prebuilt TensorFlow, or PyTorch where the platform allows its
Preview), and your model's settings must travel in `serving.json` with its files, never in container variables.
Every change to any model on the pool restarts all its machines: Pipewright makes such changes one at a time and
announces them, and callers through the gateway retry. Leave `pool` out to deploy on your own machines.

## The platform side

The starter's `serving.yaml.example` and `online_store.yaml.example` (open source) and Enterprise's platform file
show every platform key the same way: endpoints in Terraform, public or private; pools with `managed_by: ci` or
`terraform`; `policies.serving.pools` (`allow_preview`, `min_replicas`, `settle_seconds`); which store types an
environment offers.
