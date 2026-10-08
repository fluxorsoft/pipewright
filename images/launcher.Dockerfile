# Pipewright launcher (Cloud Run). Installs the built wheel, so the launcher runs exactly the package version
# app teams install. Build context: the repository root, after `uv build --wheel --out-dir dist`:
#   docker build -f images/launcher.Dockerfile -t <repo>/pipewright-launcher:<version> .
# Behind a TLS-inspecting proxy, pass its CA bundle as a build secret (used by pip only, never stored in a layer):
#   docker build --secret id=pip_ca,src=<ca-bundle.pem> ...
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PORT=8080

WORKDIR /app
COPY dist/ /tmp/dist/
RUN --mount=type=secret,id=pip_ca,required=false set -e; \
    if [ -s /run/secrets/pip_ca ]; then export PIP_CERT=/run/secrets/pip_ca; fi; \
    wheels=$(ls /tmp/dist/pipewright-*-py3-none-any.whl); \
    [ "$(echo "$wheels" | wc -l)" = "1" ] || { echo "expected exactly one pipewright wheel in dist/: $wheels"; exit 1; }; \
    pip install "${wheels}[gcp,launcher]"; \
    rm -rf /tmp/dist; \
    python -c "import flask, gunicorn, yaml; from google.cloud import aiplatform; import pipewright.launcher.routing"

ARG VERSION=unknown
LABEL dev.pipewright.component="launcher" dev.pipewright.version="${VERSION}" \
      org.opencontainers.image.version="${VERSION}"

# A home directory: gunicorn's control server writes a socket there.
RUN useradd --uid 10001 --create-home app
USER app

# The app loads the manifest and live contracts at import: an unreadable manifest fails the revision.
# Timeout 300 s: deploy the Cloud Run service with the same request timeout (a cold start plus a submit can
# take over a minute).
CMD exec gunicorn --bind :${PORT} --workers 1 --threads 8 --timeout 300 pipewright.launcher.app:app
