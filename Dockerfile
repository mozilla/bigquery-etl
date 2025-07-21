ARG PYTHON_VERSION=3.11
# pin Google Cloud SDK to old version due to https://stackoverflow.com/questions/76159439/job-failing-with-error-gcloud-crashed-attributeerror-bool-object-has-no-at
ARG GOOGLE_CLOUD_SDK_VERSION=417.0.0

# --platform=linux/amd64 added to prevent pulling ARM images when run on Apple Silicon
FROM --platform=linux/amd64 python:${PYTHON_VERSION}-slim-bullseye AS base
WORKDIR /app

# build typed-ast in separate stage because it requires gcc and libc-dev
FROM base AS python-deps
RUN apt-get update -qqy && apt-get install -qqy gcc libc-dev
COPY requirements.txt ./
# use --no-deps to work around https://github.com/pypa/pip/issues/9644
RUN pip install --no-deps -r requirements.txt

FROM google/cloud-sdk:${GOOGLE_CLOUD_SDK_VERSION}-alpine AS google-cloud-sdk

FROM base
# add bash for entrypoint
RUN mkdir -p /usr/share/man/man1 && apt-get update -qqy && apt-get install -qqy bash git jq
COPY --from=google-cloud-sdk /google-cloud-sdk /google-cloud-sdk
ENV PATH /google-cloud-sdk/bin:$PATH
COPY --from=python-deps /usr/local /usr/local
COPY .bigqueryrc /root/
COPY . .
RUN pip install .
ENTRYPOINT ["/app/script/entrypoint"]
