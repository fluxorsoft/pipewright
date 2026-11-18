# Sample Grafana dashboards

Four dashboards for the launcher's OpenTelemetry export (FR-127, `observability.otlp`), one per reader. They use
only the telemetry contract in [docs/TELEMETRY.md](../../docs/TELEMETRY.md): the six metrics and the launcher's
spans. Telemetry carries names, counts and times, never data or personal details.

| File | For | Shows |
|---|---|---|
| `dashboards/pipewright-overview.json` | everyone | runs started, held or refused, retries absorbed, trigger to run p95, decisions waiting, recent runs |
| `dashboards/pipewright-sre.json` | SRE and platform | trigger to run p50/p95/p99 and its distribution, refused share, retries, failed scheduled checks, failed and slow runs |
| `dashboards/pipewright-ml-team.json` | ML teams | their use cases: runs by pipeline, why runs were refused, decisions waiting, runs with every step |
| `dashboards/pipewright-governance.json` | governance and risk | what the rules held or refused and why, decisions waiting, the refused runs' traces |

Telemetry is for operations. The record of decisions stays the event log and the evidence packs.

## Import

In Grafana: **Dashboards > New > Import**, upload a file, then choose a Prometheus data source and a Tempo data
source. The dashboards link to each other (the "Pipewright dashboards" menu, top right). The `usecase` variable
filters every panel.

Any backend that stores OTLP metrics under Prometheus names works: Grafana Cloud, Google Cloud Managed Service for
Prometheus read through Grafana, Mimir, Prometheus 3 with its OTLP receiver.

## Change them

The JSON is generated: edit `make_dashboards.py`, then

```bash
python examples/telemetry/make_dashboards.py
```

## Try them without a platform

`demo_traffic.py` sends what a busy launcher would, for five made-up use cases, through Pipewright's own telemetry
code. The endpoint and headers come from the environment only:

```bash
pip install 'pipewright-mlops[otel]'
export OTEL_EXPORTER_OTLP_ENDPOINT=https://<your OTLP/HTTP endpoint>
export OTEL_EXPORTER_OTLP_HEADERS="Authorization=Basic%20<token>"    # if your backend needs one
python examples/telemetry/demo_traffic.py --minutes 30
```

Runs take 1 to 14 minutes to finish, so give it 15 minutes before the run and step traces fill in.
