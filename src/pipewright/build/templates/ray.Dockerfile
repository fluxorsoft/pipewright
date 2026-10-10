# Pipewright Ray image: what a use case's Ray clusters run. Generated build context:
#   requirements.txt   the use case's components.ray_requirements (may be empty)
#   components/        the use case's component modules and pipewright_runtime.py
# The base is a Ray on Vertex AI image (prebuilt, or the platform's own built on one), so the cluster's Ray
# matches the version the platform set. Ray jobs import the component modules from /app/components.
ARG BASE_IMAGE
FROM ${BASE_IMAGE}

ENV PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

COPY requirements.txt /tmp/pipewright-ray-requirements.txt
# PIPEWRIGHT_BUILD_CA_BUNDLE (a TLS-inspecting proxy's CA) arrives as a build secret: used by pip, never stored.
RUN --mount=type=secret,id=pip_ca,required=false \
    if [ -s /run/secrets/pip_ca ]; then export PIP_CERT=/run/secrets/pip_ca; fi; \
    if grep -qv '^\s*\(#.*\)\?$' /tmp/pipewright-ray-requirements.txt; then \
      pip install -r /tmp/pipewright-ray-requirements.txt; fi

COPY components/ /app/components/
# Add, never replace: Ray on Vertex AI images put their own start-up hooks on PYTHONPATH (/var/sitecustomize).
ENV PYTHONPATH=/app/components${PYTHONPATH:+:${PYTHONPATH}}

ARG RELEASE_ID=unknown
ARG USECASE=unknown
ENV PIPEWRIGHT_RELEASE=${RELEASE_ID}
LABEL dev.pipewright.usecase="${USECASE}" \
      dev.pipewright.release="${RELEASE_ID}" \
      dev.pipewright.image="ray"
