ARG PYTHON_VERSION=3.8

# use buster image because the default bullseye image released 2021-08-17
# sha256:ffb6539b4b233743c62170989024c6f56dcefa69a83c4bd9710d4264b19a98c0
# has updated coreutils that require a newer linux kernel than provided by CircleCI, per
# https://forums.docker.com/t/multiple-projects-stopped-building-on-docker-hub-operation-not-permitted/92570/6
# and https://forums.docker.com/t/multiple-projects-stopped-building-on-docker-hub-operation-not-permitted/92570/11
# --platform=linux/amd64 added for Apple Silicon support
FROM --platform=linux/amd64 python:${PYTHON_VERSION}-slim-buster AS base
WORKDIR /app

# build typed-ast in separate stage because it requires gcc and libc-dev
FROM base AS python-deps
RUN apt-get update -qqy && apt-get install -qqy gcc libc-dev
COPY java-requirements.txt ./
RUN pip install -r java-requirements.txt
COPY requirements.txt ./
RUN pip install -r requirements.txt

# download java dependencies in separate stage because it requires maven
FROM base AS java-deps
# man directory is removed in upstream debian:buster-slim, but needed by jdk install
RUN mkdir -p /usr/share/man/man1 && apt-get update -qqy && apt-get install -qqy maven
COPY pom.xml ./
RUN mvn dependency:copy-dependencies

FROM base
# add bash for entrypoint and jdk for jni access to zetasql
RUN mkdir -p /usr/share/man/man1 && apt-get update -qqy && apt-get install -qqy bash default-jdk-headless
COPY --from=google/cloud-sdk:alpine /google-cloud-sdk /google-cloud-sdk
ENV PATH /google-cloud-sdk/bin:$PATH
COPY --from=java-deps /app/target/dependency /app/target/dependency
COPY --from=python-deps /usr/local /usr/local
COPY .bigqueryrc /root/
COPY . .
RUN pip install .
ENTRYPOINT ["/app/script/entrypoint"]
