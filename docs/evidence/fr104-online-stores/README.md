# Online feature stores compared, live (FR-104)

Evidence for CONTEXT.md FR-104 and section 8.19. Run live on Google Cloud, europe-west2, 2026-11-06, and torn down
the same day. Synthetic tables of 100k and 1M entities x 30 numeric features (no real data). Lookups by entity ID
from a VM in the same region, open-loop at 1, 10 and 50 requests a second for 30 seconds each, connections reused.
Costs: Google's list prices for europe-west2 from the Cloud Billing Catalog API, read the same day, before free tiers.

## Results (p50 / p95 / p99, ms)

| Store | 10 a second | 50 a second | Billed per lookup | Standing cost a month |
|---|---|---|---|---|
| BigQuery, no BI Engine, 1M | 289 / 463 / 620 | 281 / 493 / 696 | the whole table: 245 MiB (about $0.0018) | none |
| BigQuery, no BI Engine, 100k | 143 / 236 / 753 | 131 / 183 / 325 | the whole table: 25 MiB | none |
| BigQuery + BI Engine, 1M | 198 / 320 / 614 | 201 / 265 / 420 | 0 bytes once cached (`biEngineMode: FULL`) | $0.052 per GiB-hour, about $38 per GiB |
| BigQuery + BI Engine, 100k | 147 / 293 / 483 | 141 / 537 / 1,208 | 0 bytes once cached | same |
| Feature Store Bigtable online serving, 1M | 12.5 / 15.7 / 20.9 | 12.4 / 17.4 / 35.9 | none | $1.07 per node-hour, about $781; storage $0.29 per GiB-month |
| Cloud SQL for PostgreSQL, direct, 1M | 3.3 / 3.7 / 4.4 | 3.2 / 3.7 / 4.8 | none | about $61 (1 vCPU, 3.75 GB, zonal, 10 GB disk) |
| Cloud SQL through a Cloud Run lookup service | 18.9 / 21.8 / 25.4 | 18.7 / 20.9 / 27.0 | none | + about $14 for one warm instance |
| Firestore (Standard), direct, 100k | 9.8 / 16.1 / 28.6 | 8.7 / 11.2 / 14.8 | $0.39 per million reads | storage $0.117 per GiB-month; writes $1.17 per million |

Other measurements:
- BigQuery's own job timing for accelerated lookups: 118 to 279 ms server-side, so the end-to-end figure is mostly
  BigQuery's query service, not the network. Engine-only claims of a few milliseconds are not comparable.
- BI Engine warm-up: in the first minute after the reservation was set, about half the lookups were not accelerated
  and were billed the full table; afterwards every sampled lookup was `FULL` and billed 0 bytes.
- Clustering on the entity key did not reduce bytes billed at these sizes.
- Export: the latest-per-entity snapshot query billed 245 MiB once; the extract job to Parquet (Snappy) took 10 s and
  wrote one 194 MiB file for 1M rows; a manifest with each file's generation and SHA-256 is in
  `results/export_result.json`.
- Loads: Cloud SQL 1M rows from Parquet with `COPY` in 55 s; Firestore 100k documents with `batchWrite`
  (16 threads) in 78 s. Bigtable feature view sync: 1M rows in 37 s, 100k in 17 s.

## Lessons found live

1. The first runs (`a*`, `b*`, `c0*`) opened a new HTTPS connection per request: every HTTPS store showed a p50 of
   about 4.7 s at 50 a second. They are superseded by the `k*`, `m*` and `p*` runs with connections reused.
2. The Cloud Run adapter exhausted PostgreSQL's connection slots at 50 a second (one connection per thread, scaled
   out); fixed with a bounded pool (8 per instance) and at most 5 instances.
3. Firestore's `batchWrite` answers 200 while single writes can fail: about 4% of documents were missing, with
   nothing reported. A loader must check every write's status.
4. The first Firestore load failed on the timestamp format from the Parquet reader (`c02`, `k06`); `p01` is the
   valid load. The adapter's Firestore path at 50 a second (`p03`) reconnected per request and is not representative.

## Files

- `vm_agent.py`: the load agent (fixed plan types only, results written to the bucket).
- `lookup_adapter.py`: the `http` contract adapter on Cloud Run (`POST /lookup {store, entity_ids}`), PostgreSQL over
  the Cloud SQL socket with a bounded pool, and Firestore over REST.
- `rest_helper.py`: the REST helper used to set up BigQuery, BI Engine and the Feature Store (`PROJECT`,
  `GCLOUD_CONFIG` from the environment).
- `results/`: every run's raw result, named by run.
