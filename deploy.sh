#!/usr/bin/env bash

set -ex

BUCKET="gs://etl-graph"

gsutil cp index.html $BUCKET/site/
gsutil cp data/edges.json $BUCKET/site/data/
