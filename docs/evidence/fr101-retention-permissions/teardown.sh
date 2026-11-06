#!/usr/bin/env bash
# Deletes the spike's service accounts and custom roles at once. The bucket holds one Locked-mode object until about
# 90 minutes after the run (Locked retention cannot be shortened by anyone); delete the bucket after that with
#   gcloud storage rm -r --all-versions gs://$SPIKE_BUCKET
set -euo pipefail
: "${PROJECT:?}" "${SPIKE_SUFFIX:?}" "${SPIKE_BUCKET:?}"
C=${GCLOUD_CONFIG:+--configuration=$GCLOUD_CONFIG}; export CLOUDSDK_CORE_DISABLE_PROMPTS=1
for r in get ret retupd; do
  gcloud iam service-accounts delete "sp-lp-$r-$SPIKE_SUFFIX@$PROJECT.iam.gserviceaccount.com" --project="$PROJECT" --quiet $C || true
  gcloud iam roles delete "spikeLock_${r}_$SPIKE_SUFFIX" --project="$PROJECT" --quiet $C || true
done
gcloud storage rm -r --all-versions "gs://$SPIKE_BUCKET" $C || echo "bucket kept until its Locked object's date passes"
