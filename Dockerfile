ARG PYTHON_VERSION=3.10

# use buster image because the default bullseye image released 2021-08-17
# sha256:ffb6539b4b233743c62170989024c6f56dcefa69a83c4bd9710d4264b19a98c0
# has updated coreutils that require a newer linux kernel than provided by CircleCI, per
# https://forums.docker.com/t/multiple-projects-stopped-building-on-docker-hub-operation-not-permitted/92570/6
# and https://forums.docker.com/t/multiple-projects-stopped-building-on-docker-hub-operation-not-permitted/92570/11
# --platform=linux/amd64 added to prevent pulling ARM images when run on Apple Silicon
FROM --platform=linux/amd64 python:${PYTHON_VERSION}-slim-buster AS base
WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    PYTHONFAULTHANDLER=1 \
    PYSETUP_PATH="/opt/pysetup" \
    VENV_PATH="/opt/pysetup/.venv" \
    POETRY_HOME="/opt/poetry" \
    POETRY_VIRTUALENVS_IN_PROJECT=true \
    POETRY_NO_INTERACTION=1

ENV PATH="$POETRY_HOME/bin:$VENV_PATH/bin:$PATH"

# build typed-ast in separate stage because it requires gcc and libc-dev
FROM base AS python-deps

ARG POETRY_VERSION=1.3.1

WORKDIR $PYSETUP_PATH

RUN apt-get update -qqy && apt-get install -qqy gcc libc-dev curl

COPY . .

# install poetry
RUN curl -sSL https://install.python-poetry.org | python - --version $POETRY_VERSION&&\
    poetry install -vvv --only main

# download java dependencies in separate stage because it requires maven
FROM base AS java-deps
# man directory is removed in upstream debian:buster-slim, but needed by jdk install
RUN mkdir -p /usr/share/man/man1 && apt-get update -qqy && apt-get install -qqy maven
COPY pom.xml ./
COPY src src
RUN mvn package

FROM base
# add bash for entrypoint and jdk for jni access to zetasql
RUN mkdir -p /usr/share/man/man1 && apt-get update -qqy && apt-get install -qqy bash default-jdk-headless
COPY --from=google/cloud-sdk:alpine /google-cloud-sdk /google-cloud-sdk
ENV PATH /google-cloud-sdk/bin:$PATH
COPY --from=java-deps /app/target/dependency /app/target/dependency
COPY --from=java-deps /app/target/*.jar /app/target/
COPY --from=python-deps $VENV_PATH $VENV_PATH
COPY .bigqueryrc /root/
COPY . .
ENTRYPOINT ["/app/script/entrypoint"]
