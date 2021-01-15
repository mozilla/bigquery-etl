#!/usr/bin/env bash

set -ex

BUCKET="gs://etl-graph"

gsutil -m rsync -r public/ $BUCKET/site/
