#!/usr/bin/env bash

set -ex

PROJECT="etl-graph"
BUCKET="gs://etl-graph"

gcloud config set project $PROJECT
gsutil cp index.html $BUCKET/site/
gsutil cp data/edges.json $BUCKET/site/data/
