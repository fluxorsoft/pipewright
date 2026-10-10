#!/usr/bin/env bash
# Smoke test of an installed Pipewright (from the wheel, not the source tree): scaffold a use case from the packaged
# example, validate and resolve it against a local manifest, and generate the CI workflows. No cloud access.
#   scripts/smoke_test.sh <pipewright executable>
set -euo pipefail
PW="${1:-pipewright}"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
cd "$WORK"
mkdir -p environments
cat > environments/dev.manifest.json <<'JSON'
{
  "apiVersion": "pipewright/v1", "environment": "dev", "blueprint": "poc@0.1.0", "orchestrator": "vertex",
  "project": "smoke-project", "region": "europe-west2",
  "artifact_registry": {"docker": "europe-west2-docker.pkg.dev/smoke-project/images",
                        "kfp": "https://europe-west2-kfp.pkg.dev/smoke-project/pipelines"},
  "artifacts_bucket": "smoke-artifacts", "landing_bucket": "smoke-landing",
  "launcher": {"url": "https://launcher.example", "service_account": "launcher@smoke-project.iam.gserviceaccount.com"},
  "tenants": {"smoke": {"runner_service_account": "runner@smoke-project.iam.gserviceaccount.com",
                        "bigquery_dataset": "smoke", "landing_prefix": "incoming/smoke/",
                        "outputs_uri": "gs://smoke-artifacts/outputs/smoke/"}}
}
JSON
cat > environments/dev.yaml <<'YAML'
apiVersion: pipewright/v1
kind: Environment
metadata: { name: dev }
spec: { manifest: dev.manifest.json }
YAML
"$PW" --version
"$PW" new-usecase --name smoke-churn --tenant smoke --owner smoke-team
"$PW" new-usecase --name smoke-lite --tenant smoke --owner smoke-team --from minimal
"$PW" new-usecase --name smoke-ray --tenant smoke --owner smoke-team --from ray
test -f .pipewright-schemas/usecase.schema.json
"$PW" explain data.split.strategy | grep -q "allowed:   random, stratified, time"
"$PW" validate --env dev
"$PW" resolve --env dev > resolved.yaml && grep -q "^  name: smoke-churn-classifier" resolved.yaml
"$PW" ci init --environments dev
test -f .github/workflows/pipewright-release.yml
echo "smoke test passed"
