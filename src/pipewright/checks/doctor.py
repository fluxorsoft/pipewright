"""pipewright doctor: read-only checks that ML config matches the live environment.

Nothing here creates or changes resources.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from pipewright.build.compile import ML_CONFIG_PARAMETER, check_parameters, compile_all
from pipewright.config import Environment, ResolvedUseCase
from pipewright.errors import ConfigError
from pipewright.gcp import Gcp, error_text
from pipewright.tables.plan import plan_table

OK, INFO, WARN, FAIL = "OK", "INFO", "WARN", "FAIL"

CRM = "https://cloudresourcemanager.googleapis.com/v3"
GCS = "https://storage.googleapis.com/storage/v1"
AR = "https://artifactregistry.googleapis.com/v1"
IAM = "https://iam.googleapis.com/v1"
RUN = "https://run.googleapis.com/v2"
PUBSUB = "https://pubsub.googleapis.com/v1"
BQ = "https://bigquery.googleapis.com/bigquery/v2"
KMS = "https://cloudkms.googleapis.com/v1"


@dataclass
class Result:
    status: str
    check: str
    detail: str = ""
    hint: str = ""


@dataclass
class Report:
    sections: dict[str, list[Result]] = field(default_factory=dict)

    def add(self, section: str, result: Result) -> None:
        self.sections.setdefault(section, []).append(result)

    @property
    def failed(self) -> int:
        return sum(r.status == FAIL for rs in self.sections.values() for r in rs)

    @property
    def warnings(self) -> int:
        return sum(r.status == WARN for rs in self.sections.values() for r in rs)

    def render(self) -> str:
        lines = []
        for section, results in self.sections.items():
            lines.append(f"\n{section}")
            for r in results:
                line = f"  {r.status:<4}  {r.check}"
                if r.detail:
                    line += f": {r.detail}"
                lines.append(line)
                if r.hint:
                    lines.append(f"        -> {r.hint}")
        lines.append(f"\n{self.failed} failed, {self.warnings} warnings")
        return "\n".join(lines)


def _repo_path(url: str, kind: str) -> tuple[str, str, str] | None:
    """(location, project, repository) from an Artifact Registry docker or KFP URL."""
    m = re.match(rf"^(?:https://)?([a-z0-9-]+)-{kind}\.pkg\.dev/([^/]+)/([^/]+)/?$", url)
    return m.groups() if m else None


def _exists(gcp: Gcp, report: Report, section: str, check: str, url: str, hint: str, detail_fn=None):
    resp = gcp.get(url)
    if resp.ok:
        report.add(section, Result(OK, check, detail_fn(resp.body) if detail_fn else ""))
        return resp.body
    status = FAIL
    detail = "not found" if resp.status == 404 else ("no permission to read it" if resp.status == 403 else
                                                    error_text(resp))
    report.add(section, Result(status, check, detail, hint))
    return None


def _test_permissions(gcp: Gcp, url: str, permissions: list[str], body_style: bool = True) -> set[str] | None:
    resp = gcp.post(url, {"permissions": permissions}) if body_style else gcp.get(url, permissions=permissions)
    if not resp.ok:
        return None
    return set(resp.body.get("permissions", []))


# ---------------------------------------------------------- environment --

def check_environment(env: Environment, gcp: Gcp, report: Report) -> None:
    s = f"Environment {env.name} ({env.blueprint or 'no blueprint'}, project {env.project})"
    platform = "ask the platform team"

    who = gcp.identity()
    report.add(s, Result(INFO, "running as", who or "unknown identity"))
    if env.manifest_source:
        report.add(s, Result(OK, "manifest", env.manifest_source))
    if env.manifest_ignored:
        from pipewright import __version__

        report.add(s, Result(WARN, "manifest keys ignored", ", ".join(env.manifest_ignored),
                             f"this Pipewright ({__version__}) does not know them: upgrade to use them"))

    _exists(gcp, report, s, "project", f"{CRM}/projects/{env.project}", "check the project ID and your access",
            lambda b: b.get("state", ""))
    _exists(gcp, report, s, f"artifacts bucket {env.artifacts_bucket}", f"{GCS}/b/{env.artifacts_bucket}", platform,
            lambda b: b.get("location", "").lower())
    if env.landing_bucket:
        _exists(gcp, report, s, f"landing bucket {env.landing_bucket}", f"{GCS}/b/{env.landing_bucket}", platform,
                lambda b: b.get("location", "").lower())

    for kind, url in (("docker", env.artifact_registry["docker"]), ("kfp", env.artifact_registry["kfp"])):
        parts = _repo_path(url, kind)
        if not parts:
            report.add(s, Result(FAIL, f"{kind} repository", f"cannot parse {url}", "expected <region>-"
                                 f"{kind}.pkg.dev/<project>/<repository>"))
            continue
        loc, project, repo = parts
        _exists(gcp, report, s, f"{kind} repository {repo}",
                f"{AR}/projects/{project}/locations/{loc}/repositories/{repo}", platform,
                lambda b: b.get("format", ""))

    launcher = env.launcher or {}
    if launcher.get("service_account"):
        _exists(gcp, report, s, "launcher service account",
                f"{IAM}/projects/-/serviceAccounts/{launcher['service_account']}", platform,
                lambda b: b.get("email", ""))
    if launcher.get("service"):
        _exists(gcp, report, s, f"launcher service {launcher['service']}",
                f"{RUN}/projects/{env.project}/locations/{env.region}/services/{launcher['service']}", platform,
                lambda b: b.get("uri", ""))
    if launcher.get("subscription"):
        sub = _exists(gcp, report, s, f"subscription {launcher['subscription']}",
                      f"{PUBSUB}/projects/{env.project}/subscriptions/{launcher['subscription']}", platform,
                      lambda b: "push to " + b["pushConfig"]["pushEndpoint"] if b.get("pushConfig", {}).get(
                          "pushEndpoint") else "pull")
        if sub is not None and launcher.get("url"):
            endpoint = sub.get("pushConfig", {}).get("pushEndpoint", "")
            if endpoint and not endpoint.startswith(launcher["url"].rstrip("/")):
                report.add(s, Result(WARN, "subscription target", f"pushes to {endpoint}, not the launcher URL "
                                     f"{launcher['url']}", "the launcher may have moved; ask the platform team"))

    if launcher.get("run_requests_topic"):
        _exists(gcp, report, s, "run requests topic", f"{PUBSUB}/{launcher['run_requests_topic']}", platform,
                lambda b: b["name"].rsplit("/", 1)[-1])
    _check_event_log(env, gcp, report, s, platform)
    check_security(env, gcp, report)


def _check_event_log(env: Environment, gcp: Gcp, report: Report, s: str, platform: str) -> None:
    """FR-84: Pub/Sub discards what nobody subscribes to, so an events topic without a subscription keeps no
    record of any decision (promotions, approvals, deployments, rollbacks, retrain decisions)."""
    topic = (env.observability or {}).get("events_topic")
    if not topic:
        return
    resp = gcp.get(f"{PUBSUB}/{topic}/subscriptions")
    if not resp.ok:
        report.add(s, Result(WARN, "event log", f"cannot list the events topic's subscriptions: {error_text(resp)}",
                             "needs pubsub.topics.listSubscriptions (roles/pubsub.viewer)"))
        return
    subs = (resp.body or {}).get("subscriptions") or []
    if not subs:
        report.add(s, Result(FAIL, "event log", f"nothing subscribes to {topic.rsplit('/', 1)[-1]}: every lifecycle "
                             "event, every decision included, is discarded",
                             "keep them in BigQuery (the starter's and Enterprise's evidence dataset, manifest "
                             "observability.evidence_dataset)" + f"; {platform}"))
        return
    dataset = (env.observability or {}).get("evidence_dataset")
    report.add(s, Result(OK, "event log", f"{len(subs)} subscription(s) on {topic.rsplit('/', 1)[-1]}"
                         + (f"; kept in {dataset}.events" if dataset else "")))


def check_security(env: Environment, gcp: Gcp, report: Report) -> None:
    """Private profiles: the CMEK key and network that every job, model and endpoint gets."""
    sec = env.security or {}
    if not sec:
        return
    s = f"Security ({env.name})"
    platform = "ask the platform team"
    key = sec.get("kms_key")
    if key:
        location = key.split("/")[3]
        if location != env.region:
            report.add(s, Result(FAIL, "CMEK key location", f"{location}, but the environment is in {env.region}",
                                 "jobs fail with a CMEK key from another location: " + platform))
        resp = gcp.get(f"{KMS}/{key}")
        if resp.ok:
            state = resp.body.get("primary", {}).get("state", "no primary version")
            report.add(s, Result(OK if state == "ENABLED" else FAIL, "CMEK key", f"{key.rsplit('/', 1)[-1]}: {state}",
                                 "" if state == "ENABLED" else "jobs cannot encrypt with it: " + platform))
        elif resp.status == 403:
            report.add(s, Result(WARN, "CMEK key", "cannot read it (needs cloudkms.cryptoKeys.get); jobs still use it"))
        else:
            report.add(s, Result(FAIL, "CMEK key", "not found" if resp.status == 404 else error_text(resp), platform))
        bucket = _exists(gcp, report, s, "artifacts bucket encryption", f"{GCS}/b/{env.artifacts_bucket}", platform,
                         lambda b: b.get("encryption", {}).get("defaultKmsKeyName", "Google-managed"))
        if bucket is not None and not bucket.get("encryption", {}).get("defaultKmsKeyName"):
            report.add(s, Result(WARN, "artifacts bucket CMEK", "the bucket has no default CMEK key", platform))
    else:
        report.add(s, Result(INFO, "CMEK", "not set: Google-managed encryption"))
    network = sec.get("network")
    report.add(s, Result(OK if network else INFO, "pipeline job network", network or "not peered (Google network)"))
    if network and not sec.get("reserved_ip_ranges"):
        report.add(s, Result(INFO, "reserved IP ranges", "any range of the Private Services Access connection"))
    if sec.get("perimeter"):
        report.add(s, Result(INFO, "VPC Service Controls perimeter", sec["perimeter"]))


# ------------------------------------------------------------- use case --

def check_usecase(uc: ResolvedUseCase, env: Environment, gcp: Gcp, report: Report, compile_check: bool = True):
    s = f"Use case {uc.name} (tenant {uc.tenant})"
    platform = "ask the platform team"
    res = uc.resources

    _exists(gcp, report, s, "runner service account", f"{IAM}/projects/-/serviceAccounts/"
            f"{res['runner_service_account']}", platform, lambda b: b.get("email", ""))
    dataset = res["bigquery_dataset"]
    ds = _exists(gcp, report, s, f"dataset {dataset}", f"{BQ}/projects/{env.project}/datasets/{dataset}", platform,
                 lambda b: b.get("location", ""))

    if uc.triggers.get("landing") and env.landing_bucket:
        resp = gcp.get(f"{GCS}/b/{env.landing_bucket}/notificationConfigs")
        prefix = res.get("landing_prefix") or ""
        if resp.ok:
            hits = [n for n in resp.body.get("items", [])
                    if prefix.startswith(n.get("object_name_prefix", "")) and
                    "OBJECT_FINALIZE" in n.get("event_types", ["OBJECT_FINALIZE"])]
            if hits:
                report.add(s, Result(OK, f"landing events for gs://{env.landing_bucket}/{prefix}",
                                     hits[0].get("topic", "").rsplit("/", 1)[-1]))
            else:
                report.add(s, Result(FAIL, f"landing events for gs://{env.landing_bucket}/{prefix}",
                                     "no notification covers this prefix", platform))
        else:
            report.add(s, Result(WARN, "landing notifications", error_text(resp)))

    if uc.model["serving"]["enabled"] and uc.model["serving"]["target"] == "kserve":
        from pipewright.models import kserve

        try:
            k = kserve.KServe(uc, env, gcp)
            live = k.current()
            running = ((live or {}).get("metadata", {}).get("labels") or {}).get("pipewright.dev/version")
            report.add(s, Result(OK, f"serving: KServe in {k.s['namespace']}",
                                 f"{k.name} serves v{running}" if running else f"{k.name} not deployed yet"))
        except Exception as e:  # noqa: BLE001 - reported, not raised
            report.add(s, Result(FAIL, "serving: KServe", str(e)[:300], "ask the platform team"))
    if uc.triggers["schedules"] and _schedule_backend(env) == "kubernetes":
        from pipewright.schedules import kubernetes

        try:
            k8s = kubernetes.Backend(env, gcp)
            found = k8s.existing(uc.name)
            report.add(s, Result(OK, f"schedules: CronJobs in {k8s.namespace}",
                                 f"{len(found)} for this use case; `schedules sync` manages them"))
        except Exception as e:  # noqa: BLE001 - reported, not raised
            report.add(s, Result(FAIL, "schedules: CronJobs", str(e)[:300], "ask the platform team"))
    elif uc.triggers["schedules"]:
        r = gcp.get(f"https://serviceusage.googleapis.com/v1/projects/{env.project}/services/"
                    "cloudscheduler.googleapis.com")
        enabled = r.ok and r.body.get("state") == "ENABLED"
        report.add(s, Result(OK if enabled else FAIL, "Cloud Scheduler API",
                             "enabled" if enabled else "not enabled: schedules cannot be created",
                             "" if enabled else "ask the platform team to enable cloudscheduler.googleapis.com"))

    if compile_check:
        _check_template(uc, report)
    if ds is not None:
        _check_tables(uc, env, gcp, report)
    _check_ml(uc, env, gcp, report)
    _check_synthetic(uc, env, gcp, report)
    _check_permissions(uc, env, gcp, report)


def _check_synthetic(uc: ResolvedUseCase, env: Environment, gcp: Gcp, report: Report) -> None:
    """synthetic (only when enabled): what is generated and by which engines, and that the learned tier's source
    exists with the contract's columns (with data.features_from, the entity table's own columns)."""
    syn = uc.synthetic
    if not syn:
        return
    from pipewright.config.ml import feature_columns
    from pipewright.runs import sources

    s = f"Synthetic data of {uc.name}"
    engines = sorted({c["engine"] for c in syn.get("candidates") or []}) or ["none (contract tier)"]
    report.add(s, Result(INFO, f"purpose {syn['purpose']}, tier {syn['tier']}",
                         f"engines: {', '.join(engines)}; `pipewright synthetic gates` lists the gates"))
    if "sdv" in engines:
        report.add(s, Result(WARN, "engine sdv", "SDV is under the Business Source Licence 1.1: your team installs it "
                             "(synthetic.requirements) and accepts its terms directly; Pipewright never ships it"))
    src = syn.get("source_uri")
    if syn["tier"] != "learned" or not src or not src.startswith("bq://") or sources.problem(src):
        return
    project, dataset, table = sources.bq_table(src, env.project).split(".")
    resp = gcp.get(f"{BQ}/projects/{project}/datasets/{dataset}/tables/{table}")
    name = f"source {project}.{dataset}.{table}"
    if not resp.ok:
        report.add(s, Result(FAIL, name, error_text(resp), "check synthetic.source_uri, and that the runner may read "
                             "it (it is read as the runner, like training data)"))
        return
    have = {f["name"] for f in (resp.body.get("schema") or {}).get("fields", [])}
    from_features = set(feature_columns(uc.ml["data"].get("features_from")))
    need = [c["name"] for c in uc.ml["data"]["contract"]["columns"] if c["name"] not in from_features]
    missing = [c for c in need if c not in have]
    detail = (f"no column {', '.join(missing)}" if missing else f"{len(need)} contract columns"
              + (f" (+{len(from_features)} from feature tables)" if from_features else ""))
    report.add(s, Result(FAIL if missing else OK, name, detail,
                         "the source must hold the data contract's columns" if missing else ""))


def _check_template(uc: ResolvedUseCase, report: Report) -> None:
    from pipewright.checks import kfp_version

    s = f"Pipelines of {uc.name} ({', '.join(uc.pipelines)})"
    from pipewright.checks import ray_version

    pin = kfp_version.check(uc)
    report.add(s, Result(INFO if pin.skipped else (OK if pin.ok else FAIL), "kfp version", pin.message))
    if uc.ray:
        ray_pin = ray_version.check(uc)
        report.add(s, Result(OK if ray_pin.ok else FAIL, "ray version", ray_pin.message))
        sizes = ", ".join(f"{step}: head {c['head_machine']} + {c['workers']} x {c['worker_machine']}"
                          for step, c in uc.ray["steps"].items())
        report.add(s, Result(OK, "ray clusters", f"a cluster per run ({sizes}), Ray {uc.ray['ray_version']}"))
    try:
        infos = compile_all(uc)          # also applies `steps`: an unknown step name fails here
    except ConfigError as e:
        report.add(s, Result(FAIL, "compiles", str(e)))
        return
    for name, info in infos.items():
        ml = "takes ml_config" if ML_CONFIG_PARAMETER in info.parameters else "no ml_config parameter"
        report.add(s, Result(OK, f"{name} compiles", f"{info.name}, {len(info.parameters)} parameters, {ml}"))
    if uc.steps:
        from pipewright.synthetic import TASKS

        idle = [t for t in uc.steps if t in TASKS and "synthetic" not in uc.pipelines]
        applied = [t for t in uc.steps if t not in idle]
        report.add(s, Result(OK, "steps", f"{', '.join(applied) or 'none'} applied"
                             + (f"; {', '.join(idle)} unused here (synthetic data is off)" if idle else "")))
    problems, notes = check_parameters(uc, infos)
    for p in problems:
        report.add(s, Result(FAIL, "parameters", p))
    if not problems:
        report.add(s, Result(OK, "launcher parameters", "project, region, source_uri, run_id accepted"))
    for n in notes:
        report.add(s, Result(INFO, "default", n))


def _launcher_service(env: Environment, gcp: Gcp) -> str | None:
    """Resource name of the Cloud Run service behind the manifest's launcher URL."""
    url = (env.launcher.get("url") or "").rstrip("/")
    resp = gcp.get(f"{RUN}/projects/{env.project}/locations/{env.region}/services")
    if not resp.ok:
        return None
    for svc in resp.body.get("services", []):
        if url and url in [svc.get("uri", "").rstrip("/"), *[u.rstrip("/") for u in svc.get("urls", [])]]:
            return svc["name"]
    return None


def _check_ml(uc: ResolvedUseCase, env: Environment, gcp: Gcp, report: Report) -> None:
    s = f"ML lifecycle of {uc.name}"
    ml = uc.ml
    if not ml:
        return
    mon, retrain = ml["monitoring"], ml["retraining"]
    on = [k for k in ("drift", "prediction_drift", "performance") if mon[k]["enabled"]]
    report.add(s, Result(INFO, "pipelines", ", ".join(uc.pipelines)
                         + (f"; monitoring {', '.join(on)} -> {mon['output_table']}" if on else "; no monitoring")))
    if ml["registry"]["set_default"] == "manual":
        report.add(s, Result(INFO, "registry", "new versions are candidates: `pipewright model approve` makes one "
                             "the default"))
    erasure = ml["data_protection"].get("erasure")
    if erasure:
        covered = [n for n, t in uc.tables.items() if any(f["name"] == erasure["key_column"] for f in t.fields)]
        report.add(s, Result(OK if covered else WARN, "erasure",
                             f"{erasure['key_column']} in {', '.join(covered)}" if covered else
                             f"no table has {erasure['key_column']}: `pipewright data erase` has nothing to do"))
    report.add(s, Result(INFO if uc.tests_command else WARN, "tests",
                         uc.tests_command or "no tests.command: `pipewright test` skips this use case"))

    # BigQuery sources named in configuration (schedules, retrain source) must exist and be readable.
    from pipewright.runs import sources

    named = [(f"schedule {s['name']}", s.get("source_uri")) for s in uc.triggers["schedules"]]
    named.append(("retraining.source_uri", retrain.get("source_uri")))
    for where, uri in named:
        if not uri or not uri.startswith("bq://") or sources.problem(uri):
            continue
        project, dataset, table = sources.bq_table(uri, env.project).split(".")
        resp = gcp.get(f"{BQ}/projects/{project}/datasets/{dataset}/tables/{table}")
        if resp.ok:
            rows = resp.body.get("numRows")
            report.add(s, Result(OK, f"source for {where}", f"{project}.{dataset}.{table} ({resp.body.get('type', '')}"
                                 + (f", {rows} rows" if rows else "") + ")"))
        else:
            report.add(s, Result(FAIL, f"source for {where}", f"{project}.{dataset}.{table}: {error_text(resp)}",
                                 "check the name, and that the runner service account may read it "
                                 "(roles/bigquery.dataViewer on the dataset)"))

    _check_feature_tables(uc, env, gcp, report)
    _check_online_features(uc, gcp, report)
    _check_retraining(uc, env, gcp, report)
    requests_retrain =retrain["enabled"] and {"drift", "performance"} & set(retrain["when"])
    if not requests_retrain or env.launcher.get("run_requests_topic"):
        return      # over the run requests topic, proposals need no call to the launcher
    runner = uc.resources["runner_service_account"]
    if not env.launcher.get("url"):
        report.add(s, Result(FAIL, "retrain requests", "the manifest has no launcher.url",
                             "ask the platform team"))
        return
    service = _launcher_service(env, gcp)
    if service is None:
        report.add(s, Result(WARN, "retrain requests", f"could not find the Cloud Run service for "
                             f"{env.launcher['url']} to check who may call it"))
        return
    resp = gcp.get(f"{RUN}/{service}:getIamPolicy")
    if not resp.ok:
        report.add(s, Result(WARN, "retrain requests", f"cannot read the launcher's IAM policy: {error_text(resp)}"))
        return
    members = {m for b in resp.body.get("bindings", []) if b.get("role") in ("roles/run.invoker", "roles/run.admin")
               for m in b.get("members", [])}
    ok = f"serviceAccount:{runner}" in members or "allUsers" in members
    report.add(s, Result(OK if ok else WARN, "retrain requests (runner may call the launcher's /run)",
                         "roles/run.invoker granted" if ok else
                         f"{runner} lacks roles/run.invoker on {service.rsplit('/', 1)[-1]}: retrain requests "
                         "will be recorded as failed",
                         "" if ok else "ask the platform team to grant roles/run.invoker on the launcher to the "
                         "tenant's runner service account"))


# BigQuery column types -> the data contract types they can fill.
_CONTRACT_TYPES = {"STRING": {"string"}, "INTEGER": {"integer", "float"}, "INT64": {"integer", "float"},
                   "FLOAT": {"float"}, "FLOAT64": {"float"}, "NUMERIC": {"float"}, "BIGNUMERIC": {"float"},
                   "BOOLEAN": {"boolean", "integer"}, "BOOL": {"boolean", "integer"}, "DATE": {"date"},
                   "TIMESTAMP": {"timestamp"}, "DATETIME": {"timestamp"}}
_TIME_TYPES = {"DATE", "TIMESTAMP", "DATETIME"}


ONLINE_STALE_HOURS = 48


def _check_online_features(uc: ResolvedUseCase, gcp: Gcp, report: Report) -> None:
    """model.serving.features online (FR-51): the online store and each feature view exist, and each view's last
    sync finished within ONLINE_STALE_HOURS (an endpoint would serve older values)."""
    online = uc.ml.get("online_features")
    if not online:
        return
    from datetime import datetime, timezone

    s = f"Online features of {uc.name}"
    region = online["store"].split("/")[3]
    api = f"https://{region}-aiplatform.googleapis.com/v1"
    store = gcp.get(f"{api}/{online['store']}")
    if not store.ok:
        report.add(s, Result(FAIL, "online store", f"{online['store']}: HTTP {store.status}",
                             "the platform team creates it (tenants.<tenant>.features.online_store)"))
        return
    report.add(s, Result(OK, "online store", online["store"].rsplit("/", 1)[-1]
                         + (" (Bigtable)" if "bigtable" in (store.body or {}) else "")))
    now = datetime.now(timezone.utc)
    for view in online["views"]:
        name = f"{online['store']}/featureViews/{view['view']}"
        got = gcp.get(f"{api}/{name}")
        if not got.ok:
            report.add(s, Result(FAIL, f"view {view['view']}", f"HTTP {got.status}",
                                 "the features' owners or the platform team create the view"))
            continue
        syncs = gcp.get(f"{api}/{name}/featureViewSyncs", pageSize=20)
        finished = sorted(((x.get("runTime") or {}).get("endTime") for x in (syncs.body or {}).get(
            "featureViewSyncs", []) if syncs.ok and not (x.get("finalStatus") or {}).get("code")), reverse=True)
        finished = [f for f in finished if f]
        if not finished:
            report.add(s, Result(WARN, f"view {view['view']}", "no finished sync yet: it serves no values",
                                 "wait for its sync schedule, or start a sync"))
            continue
        age = (now - datetime.fromisoformat(finished[0].replace("Z", "+00:00"))).total_seconds() / 3600
        report.add(s, Result(OK if age <= ONLINE_STALE_HOURS else WARN, f"view {view['view']}",
                             f"last sync {finished[0][:16]} ({age:.0f} h ago); {len(view['features'])} features",
                             "" if age <= ONLINE_STALE_HOURS else "values older than the view's sync schedule"))
    _check_champion_store(uc, gcp, report, s, api)


def _check_champion_store(uc: ResolvedUseCase, gcp: Gcp, report: Report, s: str, api: str) -> None:
    """FR-76: the champion reads this environment's online store, not the one it was trained or promoted from."""
    import json

    from pipewright.gcp.storage import gcs_read
    from pipewright.models import online

    parent = f"projects/{uc.platform['project']}/locations/{uc.platform['region']}"
    found = gcp.get(f"{api}/{parent}/models", filter=f'display_name="{uc.model["name"]}"')
    models = (found.body or {}).get("models", []) if found.ok else []
    if not models:
        report.add(s, Result(INFO, "champion's online store", "no model registered yet"))
        return
    default = gcp.get(f"{api}/{models[0]['name']}@default")
    if not default.ok:
        report.add(s, Result(WARN, "champion's online store", f"cannot read the default version: HTTP {default.status}"))
        return
    body = default.body or {}
    env_vars = {e.get("name"): e.get("value") for e in (body.get("containerSpec") or {}).get("env") or []}
    uri = body.get("artifactUri") or ""
    serving_json = None
    if uri.startswith("gs://"):
        bucket, _, prefix = uri[len("gs://"):].partition("/")
        text, _ = gcs_read(gcp, bucket, f"{prefix.rstrip('/')}/serving.json")
        serving_json = json.loads(text) if text else None
    problem = online.check(uc.ml["online_features"], env_vars, serving_json, uc.environment)
    report.add(s, Result(FAIL, "champion's online store", problem, "") if problem else
               Result(OK, "champion's online store", f"v{body.get('versionId')} reads this environment's store"))


def _check_feature_tables(uc: ResolvedUseCase, env: Environment, gcp: Gcp, report: Report) -> None:
    """data.features_from: every feature table exists and has the entity, time and feature columns, with types the
    data contract accepts."""
    spec = uc.ml["data"].get("features_from")
    if not spec:
        return
    from pipewright.runs import sources

    s = f"Feature tables of {uc.name}"
    contract = {c["name"]: c["type"] for c in (uc.ml["data"].get("contract") or {}).get("columns", [])}
    keys = spec["entity"]["columns"]
    allowed = uc.platform.get("feature_datasets")
    for src in spec["sources"]:
        group = None
        if "feature_group" in src:
            src, group = _resolve_group(src, env, gcp, report, s), src["feature_group"]
            if src is None:
                continue
        if sources.problem(src["table"]):
            continue                                   # validate reports it
        project, dataset, table = sources.bq_table(src["table"], env.project).split(".")
        name = f"{project}.{dataset}.{table}" + (f" (feature group {group})" if group else "")
        if group and allowed is not None and f"{project}.{dataset}" not in allowed:
            report.add(s, Result(FAIL, name, f"{project}.{dataset} is not a feature dataset of tenant {uc.tenant} "
                                 f"({', '.join(allowed) or 'none'})", "ask the platform team, or the group's owners"))
            continue
        resp = gcp.get(f"{BQ}/projects/{project}/datasets/{dataset}/tables/{table}")
        if not resp.ok:
            report.add(s, Result(FAIL, name, error_text(resp), "check the name, and that the runner service account "
                                 "may read it (roles/bigquery.dataViewer on the dataset, or an authorized view)"))
            continue
        types = {f["name"]: f["type"] for f in (resp.body.get("schema") or {}).get("fields", [])}
        ts = src.get("time_column", "feature_timestamp")
        cols = src.get("columns") or {}
        need = [*(src.get("entity_columns") or keys), ts, *(cols.get(c, c) for c in src["features"])]
        missing = [c for c in need if c not in types]
        if missing:
            report.add(s, Result(FAIL, name, f"no column {', '.join(missing)}",
                                 "a feature table has the entity columns, a time column and one column per feature"))
            continue
        problems = [] if types[ts] in _TIME_TYPES else [f"time column {ts} is {types[ts]}, not a date or timestamp"]
        prefix, columns = src.get("prefix", ""), src.get("columns") or {}
        for col in src["features"]:
            want, kind = contract.get(prefix + col), types[columns.get(col, col)]
            if want and want not in _CONTRACT_TYPES.get(kind, set()):
                problems.append(f"{col} is {kind}, the contract says {want}")
        kind = resp.body.get("type", "").lower() or "table"
        report.add(s, Result(WARN if problems else OK, name, "; ".join(problems) if problems else
                             f"{kind}, {len(src['features'])} features as of {ts}"))


def _resolve_group(src: dict, env: Environment, gcp: Gcp, report: Report, section: str) -> dict | None:
    """A feature_group source as the table source the Feature Registry says it stands for, or None (reported)."""
    from pipewright.build.templates.pipewright_runtime import group_source

    project, region = src.get("project") or env.project, src.get("region") or env.region
    url = (f"https://{region}-aiplatform.googleapis.com/v1/projects/{project}/locations/{region}"
           f"/featureGroups/{src['feature_group']}")
    resp = gcp.get(url)
    if not resp.ok:
        report.add(section, Result(FAIL, f"feature group {src['feature_group']}", error_text(resp),
                                   "check the group ID, project and region, and that you may read the Feature "
                                   "Registry (roles/aiplatform.featurestoreResourceViewer)"))
        return None
    features, token = [], None
    while True:
        page = gcp.get(f"{url}/features", pageSize=100, **({"pageToken": token} if token else {}))
        if not page.ok:
            report.add(section, Result(FAIL, f"feature group {src['feature_group']}", error_text(page)))
            return None
        features += page.body.get("features", [])
        token = page.body.get("nextPageToken")
        if not token:
            break
    try:
        return group_source(src, resp.body, features)
    except ValueError as e:
        report.add(section, Result(FAIL, f"feature group {src['feature_group']}", str(e),
                                   "register the features in the group, or use the registered names"))
        return None


def _check_retraining(uc: ResolvedUseCase, env: Environment, gcp: Gcp, report: Report) -> None:
    """Retrain decisions: mode, approver groups, who is emailed, the decision feed."""
    s = f"Retrain decisions of {uc.name}"
    r = uc.ml["retraining"]
    if r.get("mode_source") == "enabled":
        report.add(s, Result(WARN, "retraining.enabled", f"the older switch (now mode {r['mode']})",
                             f"set retraining.mode: {r['mode']} instead"))
    if r["mode"] == "off":
        report.add(s, Result(INFO, "mode", "off: drift and performance alerts only, no decision cases"))
        return
    dec, required = r["decisions"], r["approvals"]["required"]
    report.add(s, Result(INFO, "mode", f"{r['mode']}: {', '.join(r['when'])} opens a case; "
                         + ("nothing trains until approved" if r["mode"] == "propose" else
                            "a candidate trains at once and is never promoted without a person")))
    if not required:
        report.add(s, Result(WARN, "approver groups", "none: anyone allowed to call the launcher can approve",
                             "list registered groups in retraining.approvals.required"))
    for group in required:
        topic = (env.approver_groups.get(group) or {}).get("topic")
        if not topic:
            continue      # resolve already failed on unregistered groups
        resp = gcp.get(f"https://pubsub.googleapis.com/v1/{topic}")
        report.add(s, Result(OK if resp.ok else FAIL, f"approver group {group}",
                             topic.rsplit("/", 1)[-1] if resp.ok else f"{topic}: {error_text(resp)}",
                             "" if resp.ok else "ask the platform team"))
    if required:
        broad = _project_publishers(env, gcp)
        if broad:
            report.add(s, Result(WARN, "approver group membership",
                                 f"project-wide roles let {', '.join(broad[:4])}"
                                 + (f" and {len(broad) - 4} more" if len(broad) > 4 else "")
                                 + " publish to every approver group's topic, so they count as members of every group",
                                 "in production, grant Pub/Sub publish per topic only, not owner, editor or "
                                 "pubsub roles on the project"))
    report.add(s, Result(OK if dec["notify"] else WARN, "decision emails",
                         ", ".join(dec["notify"]) if dec["notify"] else "nobody: set retraining.decisions.notify",
                         "then `pipewright alerts sync`" if dec["notify"] else ""))
    if dec["publish"]:
        feed = (env.tenants.get(uc.tenant) or {}).get("decision_feed")
        if not feed:
            report.add(s, Result(WARN, "decision feed", f"decisions.publish is true, but tenant {uc.tenant} has no "
                                 "decision_feed in the manifest", "ask the platform team to provision it"))
        else:
            resp = gcp.get(f"https://pubsub.googleapis.com/v1/{feed}")
            report.add(s, Result(OK if resp.ok else WARN, "decision feed",
                                 feed if resp.ok else f"{feed}: {error_text(resp)}"))
    try:
        from pipewright.retraining.store import CaseStore

        case = CaseStore(gcp, env.artifacts_bucket).current(uc.name)
        report.add(s, Result(INFO, "open case", f"{case['case_id']} {case['state']}" if case else "none"))
    except Exception as e:  # noqa: BLE001
        report.add(s, Result(WARN, "open case", f"cannot read cases: {e}"[:200]))


BROAD_PUBLISH_ROLES = ("roles/owner", "roles/editor", "roles/pubsub.admin", "roles/pubsub.editor",
                       "roles/pubsub.publisher")


def _project_publishers(env: Environment, gcp: Gcp) -> list[str]:
    """People and service accounts whose project-level roles include publishing to any topic (Google's own service
    agents left out). Empty when the policy is unreadable."""
    resp = gcp.post(f"https://cloudresourcemanager.googleapis.com/v1/projects/{env.project}:getIamPolicy", {})
    if not resp.ok:
        return []

    def counts(member: str) -> bool:
        kind, _, email = member.partition(":")
        if kind not in ("user", "group", "serviceAccount", "domain"):
            return False                     # projectOwner:, deleted:, allUsers is reported elsewhere
        return not (kind == "serviceAccount" and not email.endswith(".iam.gserviceaccount.com")
                    or "@gcp-sa-" in email)

    return sorted({m for b in resp.body.get("bindings", []) if b.get("role") in BROAD_PUBLISH_ROLES
                   and not b.get("condition") for m in b.get("members", []) if counts(m)})


def _check_tables(uc: ResolvedUseCase, env: Environment, gcp: Gcp, report: Report) -> None:
    s = f"Tables in {uc.resources['bigquery_dataset']} (plan for `pipewright tables sync`)"
    base = f"{BQ}/projects/{env.project}/datasets/{uc.resources['bigquery_dataset']}/tables"
    for table in uc.tables.values():
        resp = gcp.get(f"{base}/{table.name}")
        if resp.status == 404:
            live = None
        elif resp.ok:
            live = resp.body
        else:
            report.add(s, Result(WARN, table.name, error_text(resp)))
            continue
        plan = plan_table(table, live, uc.retention_days)
        if plan.action == "none":
            report.add(s, Result(OK, table.name, "matches the schema file"))
        elif plan.action == "blocked":
            report.add(s, Result(FAIL, table.name, "; ".join(plan.blockers[:-1]), plan.blockers[-1]))
        else:
            report.add(s, Result(INFO, table.name, f"{plan.action}: " + "; ".join(plan.changes)))

    resp = gcp.get(base, maxResults=1000)
    if resp.ok:
        managed = set(uc.tables)
        others = sorted(t["tableReference"]["tableId"] for t in resp.body.get("tables", [])
                        if t["tableReference"]["tableId"] not in managed)
        if others:
            report.add(s, Result(INFO, "not managed by this use case", ", ".join(others)))


def _schedule_backend(env: Environment) -> str:
    return (getattr(env, "schedules", None) or {}).get("backend") or "cloud_scheduler"


def _check_permissions(uc: ResolvedUseCase, env: Environment, gcp: Gcp, report: Report) -> None:
    s = "Permissions of the identity running doctor (what `release` will need)"
    wanted = ["aiplatform.pipelineJobs.create", "aiplatform.models.upload"]
    if uc.model["serving"]["enabled"] and uc.model["serving"]["target"] != "kserve":
        wanted += ["aiplatform.endpoints.deploy"]
    if uc.triggers["schedules"] and _schedule_backend(env) == "cloud_scheduler":
        wanted += ["cloudscheduler.jobs.create", "cloudscheduler.jobs.update"]
    if uc.ml and uc.ml["monitoring"]["alerts"]["notify"]:
        wanted += ["monitoring.alertPolicies.create", "monitoring.notificationChannels.create"]
    granted = _test_permissions(gcp, f"{CRM}/projects/{env.project}:testIamPermissions", wanted)
    if granted is None:
        report.add(s, Result(WARN, "project permissions", "could not test"))
    else:
        missing = [p for p in wanted if p not in granted]
        report.add(s, Result(FAIL if missing else OK, "project", ("missing " + ", ".join(missing)) if missing else
                             ", ".join(wanted), "ask the platform team to grant the MLOps CI identity" if missing
                             else ""))

    bucket_perms = ["storage.objects.create", "storage.objects.get"]
    granted = _test_permissions(gcp, f"{GCS}/b/{env.artifacts_bucket}/iam/testPermissions", bucket_perms, False)
    if granted is not None:
        missing = [p for p in bucket_perms if p not in granted]
        report.add(s, Result(FAIL if missing else OK, "artifacts bucket", ("missing " + ", ".join(missing)) if
                             missing else "write contracts and pointers"))

    for kind in ("docker", "kfp"):
        parts = _repo_path(env.artifact_registry[kind], kind)
        if not parts:
            continue
        loc, project, repo = parts
        granted = _test_permissions(gcp, f"{AR}/projects/{project}/locations/{loc}/repositories/{repo}"
                                    ":testIamPermissions", ["artifactregistry.repositories.uploadArtifacts"])
        if granted is not None:
            ok = "artifactregistry.repositories.uploadArtifacts" in granted
            report.add(s, Result(OK if ok else FAIL, f"{kind} repository {repo}",
                                 "upload" if ok else "cannot upload"))

    if (uc.triggers["schedules"] and _schedule_backend(env) == "cloud_scheduler"
            and env.launcher.get("service_account")):
        granted = _test_permissions(gcp, f"{IAM}/projects/-/serviceAccounts/{env.launcher['service_account']}"
                                    ":testIamPermissions", ["iam.serviceAccounts.actAs"])
        ok = granted is not None and "iam.serviceAccounts.actAs" in granted
        report.add(s, Result(OK if ok else FAIL, "act as launcher SA (schedules call /run as it)",
                             "" if ok else "missing iam.serviceAccounts.actAs"))
    report.add(s, Result(INFO, "BigQuery table permissions", "checked when `tables sync` runs (dataset-level IAM)"))


def run(env: Environment, usecases: list[ResolvedUseCase], gcp: Gcp | None = None,
        compile_check: bool = True) -> Report:
    gcp = gcp or Gcp()
    report = Report()
    check_environment(env, gcp, report)
    for uc in usecases:
        check_usecase(uc, env, gcp, report, compile_check)
    return report
