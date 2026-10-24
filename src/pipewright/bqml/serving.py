"""Online serving of a BigQuery ML version (BigQuery ML phase 2, decision 6 in the Enterprise BQML design).

Model Registry deploys a BigQuery ML registry entry to an endpoint with its preprocessing (TRANSFORM, the training
means for missing values), so the endpoint scores as `ML.PREDICT` does. Pipewright does not take that on trust:
after a deploy, the run's kept test predictions (the input rows and `ML.PREDICT`'s score) are sent to the
endpoint, and only answers from the new deployed model are compared. `deploy-model` refuses the version, moving
traffic back, when a score differs by more than TOLERANCE.

At an endpoint no SQL runs: callers send the prepared features (prepare.sql's output), as the rows here are.
"""

from __future__ import annotations

import json
import re

from pipewright.errors import ConfigError

TOLERANCE = 1e-6
ROWS = 25


def _part(name: str) -> str:            # as bqml_runtime.base_name and run_part name a run's tables
    return re.sub(r"[^a-z0-9_]", "_", name.lower())


def _q(name: str) -> str:
    return "`" + name.replace("`", "") + "`"


def _test_table(uc, labels: dict) -> str:
    return (f"{uc.platform['project']}.{uc.resources['bigquery_dataset']}."
            f"{_part(uc.name)}_bqml_test_{_part(labels['pipewright-run'])}")


def feature_columns(gcp, uc, labels: dict) -> list[str]:
    """The model's inputs, from BigQuery (with TRANSFORM: the inputs of the TRANSFORM clause)."""
    project, dataset = uc.platform["project"], uc.resources["bigquery_dataset"]
    r = gcp.get(f"https://bigquery.googleapis.com/bigquery/v2/projects/{project}/datasets/{dataset}/models/"
                f"{labels['pipewright-bq-model']}")
    if not r.ok:
        raise ConfigError(f"BigQuery model {labels['pipewright-bq-model']}: HTTP {r.status}")
    return [c["name"] for c in (r.body or {}).get("featureColumns") or []]


def expected_rows(gcp, uc, env, labels: dict, columns: list[str]) -> list[tuple[dict, object]]:
    """(instance, ML.PREDICT's score or label) for up to ROWS test rows, in a fixed order."""
    from pipewright.gcp.bigquery import query, rows

    task = uc.ml["data"]["label"].get("task", "binary")
    score = {"binary": "_pw_score", "regression": "_pw_prediction"}.get(task, "_pw_label")
    instance = f"TO_JSON_STRING(STRUCT({', '.join(_q(c) for c in columns)}))"
    sql = (f"SELECT {instance} AS instance, {score} AS expected FROM `{_test_table(uc, labels)}` "
           f"WHERE {score} IS NOT NULL ORDER BY FARM_FINGERPRINT({instance}) LIMIT {ROWS}")
    found = rows(query(gcp, env.project, sql, [], kms_key=env.security.get("bigquery_kms_key")))
    return [(json.loads(r["instance"]), r["expected"]) for r in found]


def endpoint_value(prediction: dict, uc):
    """The comparable value of one endpoint answer: the positive class probability, the prediction, or the label."""
    label = uc.ml["data"]["label"]
    task, name = label.get("task", "binary"), label["column"]
    if task == "regression":
        value = prediction.get(f"predicted_{name}")
        return float(value[0] if isinstance(value, list) else value)
    if task == "binary":
        values = [str(v) for v in prediction.get(f"{name}_values") or []]
        probs = prediction.get(f"{name}_probs") or []
        positive = str(label.get("positive", 1))
        if positive not in values:
            raise ConfigError(f"the endpoint's answer has no probability for {name}={positive}")
        return float(probs[values.index(positive)])
    value = prediction.get(f"predicted_{name}")
    return str(value[0] if isinstance(value, list) else value)


def compare(expected: list, got: list, task: str) -> float:
    """The largest difference (scores) or the share of differing labels (multi-class)."""
    if task == "multiclass":
        return sum(str(a) != str(b) for a, b in zip(expected, got, strict=True)) / max(len(expected), 1)
    return max((abs(float(a) - float(b)) for a, b in zip(expected, got, strict=True)), default=0.0)


def parity(gcp, uc, env, labels: dict, endpoint, deployed_id: str, attempts: int = 40) -> dict:
    """Endpoint scores of the new deployed model against ML.PREDICT on the run's test rows."""
    columns = feature_columns(gcp, uc, labels)
    cases = expected_rows(gcp, uc, env, labels, columns)
    if not cases:
        raise ConfigError(f"no kept test predictions for run {labels.get('pipewright-run')} "
                          f"({_test_table(uc, labels)}): train again, then deploy")
    instances, expected = [c[0] for c in cases], [c[1] for c in cases]
    task = uc.ml["data"]["label"].get("task", "binary")
    for _ in range(attempts):              # with a canary, the endpoint answers from either deployed model
        response = endpoint.predict(instances=instances)
        if str(getattr(response, "deployed_model_id", "")) != str(deployed_id):
            continue
        got = [endpoint_value(p, uc) for p in response.predictions]
        difference = compare(expected, got, task)
        return {"rows": len(instances), "max_difference": difference,
                "ok": difference <= (0.0 if task == "multiclass" else TOLERANCE)}
    raise ConfigError(f"the endpoint never answered from the new deployed model {deployed_id} in {attempts} "
                      "requests: parity not checked")
