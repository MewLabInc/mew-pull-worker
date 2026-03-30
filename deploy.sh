#!/usr/bin/env bash
set -e

cd ~/mew-pull-worker

node --check index.js

git add -A
git commit -m "${1:-checkpoint}" || true

gcloud run deploy mew-pull-worker \
  --source . \
  --region europe-west1 \
  --service-account mew-worker@mew-prod-488113.iam.gserviceaccount.com \
  --set-env-vars BUILD_ID=manual-$(date -u +%Y%m%d-%H%M%S) \
  --set-secrets MEW_WORKER_CONFIG=mew-worker-config:latest
