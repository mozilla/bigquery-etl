#!/usr/bin/env bash
# Synchronizes assets into the bucket read by protosaur.dev

set -ex

cd "$(dirname "$0")/.."

BUCKET=${BUCKET:-"gs://etl-graph"}

gsutil -m rsync -r -d public/data $BUCKET/site/data
