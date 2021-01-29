#!/usr/bin/env bash
# Synchronizes site assets into the bucket read by protosaur.dev

set -ex

cd "$(dirname "$0")/.."

BUCKET=${BUCKET:-"gs://etl-graph"}

npm run build
gsutil -m rsync -r -d -x data/ public/ $BUCKET/site/
