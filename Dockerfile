ARG PYTHON_VERSION=3.11
# Google Cloud SDK pinned for stability, look for breaking changes when upgrading https://docs.cloud.google.com/sdk/docs/release-notes
ARG GOOGLE_CLOUD_SDK_VERSION=552.0.0

# --platform=linux/amd64 added to prevent pulling ARM images when run on Apple Silicon
FROM --platform=linux/amd64 python:${PYTHON_VERSION}-slim-bullseye AS base
WORKDIR /app

# build typed-ast in separate stage because it requires gcc and libc-dev
FROM base AS python-deps
RUN apt-get update -qqy && apt-get install -qqy gcc libc-dev
COPY requirements.txt requirements-private.txt* ./
# use --no-deps to work around https://github.com/pypa/pip/issues/9644
RUN pip install --no-deps -r requirements.txt && \
    if [ -f requirements-private.txt ]; then pip install --no-deps -r requirements-private.txt; fi

# gbstats, the statistics engine the Highwind experiment analysis runs on, taken from GrowthBook's
# published image at a frozen digest. Only the Python package is copied; scipy, the one dependency
# this image lacked, is declared in requirements.in.
FROM growthbook/growthbook@sha256:48c506939021bb04cf16457f1814b1699c1c2747a0618c73e7a2ffcff31c4ee1 AS gbstats

FROM google/cloud-sdk:${GOOGLE_CLOUD_SDK_VERSION}-alpine AS google-cloud-sdk

FROM base
# add bash for entrypoint
RUN mkdir -p /usr/share/man/man1 && apt-get update -qqy && apt-get install -qqy bash git jq
COPY --from=google-cloud-sdk /google-cloud-sdk /google-cloud-sdk
ENV PATH /google-cloud-sdk/bin:$PATH
COPY --from=python-deps /usr/local /usr/local
COPY --from=gbstats /opt/venv/lib/python3.11/site-packages/gbstats /opt/gbstats/gbstats
COPY --from=gbstats /opt/venv/lib/python3.11/site-packages/gbstats-0.8.0.dist-info \
  /opt/gbstats/gbstats-0.8.0.dist-info
ENV PYTHONPATH=/opt/gbstats
COPY .bigqueryrc /root/
COPY . .
RUN pip install . \
  && python -m playwright install --with-deps firefox  # some jobs require a browser to be installed
ENTRYPOINT ["/app/script/entrypoint"]
