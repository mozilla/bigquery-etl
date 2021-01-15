#!/usr/bin/env bash

set -ex

BUCKET="gs://etl-graph"

npm run build
gsutil -m rsync -r -d public/ $BUCKET/site/
