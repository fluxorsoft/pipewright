# Pipewright Ray image: what a use case's Ray clusters run. Generated build context:
#   requirements.txt   the use case's components.ray_requirements (may be empty)
#   components/        the use case's component modules and pipewright_runtime.py
# The base is a Ray on Vertex AI image (prebuilt, or the platform's own built on one), or on GKE (KubeRay) the
# platform's Ray image (manifest ray.image, e.g. a mirrored rayproject/ray), so the cluster's Ray matches the
# version the platform set. Ray jobs import the component modules from /app/components.
ARG BASE_IMAGE
FROM ${BASE_IMAGE}

ENV PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

COPY requirements.txt /tmp/pipewright-ray-requirements.txt
# PIPEWRIGHT_BUILD_CA_BUNDLE (a TLS-inspecting proxy's CA) arrives as a build secret: used by pip, never stored.
# Readable by any user (mode 0444): some base images, such as rayproject/ray, build as a non-root user.
RUN --mount=type=secret,id=pip_ca,required=false,mode=0444 \
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
