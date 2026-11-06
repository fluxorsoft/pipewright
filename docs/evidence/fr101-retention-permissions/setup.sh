#!/usr/bin/env bash
# Phase 7 (FR-101) retention-permission spike: a throwaway bucket with object retention and versioning, three custom
# roles and three service accounts, each role granted on the bucket for objects under models/ only, and the caller
# allowed to impersonate them. Usage: PROJECT=<project> IMPERSONATOR=user:<you> [GCLOUD_CONFIG=<config>] ./setup.sh
set -euo pipefail
: "${PROJECT:?}" "${IMPERSONATOR:?}"
REGION=${REGION:-europe-west2}; SUF=$(date +%m%d%H%M); B="$PROJECT-spike-lockperm-$SUF"
C=${GCLOUD_CONFIG:+--configuration=$GCLOUD_CONFIG}; export CLOUDSDK_CORE_DISABLE_PROMPTS=1
gcloud storage buckets create "gs://$B" --project="$PROJECT" --location="$REGION" --uniform-bucket-level-access \
  --enable-per-object-retention $C
gcloud storage buckets update "gs://$B" --versioning $C
for r in get ret retupd; do
  case $r in
    get)    PERMS=storage.objects.get ;;
    ret)    PERMS=storage.objects.get,storage.objects.setRetention ;;
    retupd) PERMS=storage.objects.get,storage.objects.setRetention,storage.objects.update ;;
  esac
  gcloud iam roles create "spikeLock_${r}_$SUF" --project="$PROJECT" --permissions="$PERMS" --title="spike lock $r" --stage=GA $C
  gcloud iam service-accounts create "sp-lp-$r-$SUF" --project="$PROJECT" --display-name="spike lock perms $r" $C
  SA="sp-lp-$r-$SUF@$PROJECT.iam.gserviceaccount.com"
  gcloud storage buckets add-iam-policy-binding "gs://$B" --member="serviceAccount:$SA" \
    --role="projects/$PROJECT/roles/spikeLock_${r}_$SUF" \
    --condition="title=models-only,expression=resource.name.startsWith(\"projects/_/buckets/$B/objects/models/\")" $C
  gcloud iam service-accounts add-iam-policy-binding "$SA" --project="$PROJECT" --member="$IMPERSONATOR" \
    --role=roles/iam.serviceAccountTokenCreator $C
done
echo "export PROJECT=$PROJECT SPIKE_SUFFIX=$SUF SPIKE_BUCKET=$B ${GCLOUD_CONFIG:+GCLOUD_CONFIG=$GCLOUD_CONFIG}"
