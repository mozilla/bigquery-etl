ARG PYTHON_VERSION=3.8

# build typed-ast in separate stage because it requires gcc and libc-dev
FROM python:${PYTHON_VERSION}-slim
RUN apt-get update -qqy && apt-get install -qqy gcc libc-dev
COPY requirements.txt ./
RUN pip install -r requirements.txt

# download java dependencies in separate stage because it requires maven
FROM python:${PYTHON_VERSION}-slim
# man is directory removed in upstream debian:buster-slim, but needed by jdk install
RUN mkdir -p /usr/share/man/man1 && apt-get update -qqy && apt-get install -qqy maven
WORKDIR /app
COPY pom.xml ./
RUN mvn dependency:copy-dependencies

FROM python:${PYTHON_VERSION}-slim
# add bash for entrypoint and jdk for jni access to zetasql
RUN mkdir -p /usr/share/man/man1 && apt-get update -qqy && apt-get install -qqy bash default-jdk-headless
COPY --from=google/cloud-sdk:alpine /google-cloud-sdk /google-cloud-sdk
ENV PATH /google-cloud-sdk/bin:$PATH
COPY --from=1 /app/target/dependency /app/target/dependency
COPY --from=0 /usr/local /usr/local
WORKDIR /app
COPY .bigqueryrc /root/
COPY . .
RUN pip install .
ENTRYPOINT ["/app/script/entrypoint"]
