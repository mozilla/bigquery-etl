ARG PYTHON_VERSION=3.8

# build typed-ast in separate stage because it requires gcc and libc-dev
FROM python:${PYTHON_VERSION}-slim
COPY requirements.txt constraints.txt ./
RUN apt-get update -qqy && apt-get install -qqy gcc libc-dev && \
    pip install -r requirements.txt

FROM python:${PYTHON_VERSION}-slim
# add bash for entrypoing and python2 for google-cloud-sdk
RUN apt-get update -qqy && apt-get install -qqy bash python
COPY --from=google/cloud-sdk:alpine /google-cloud-sdk /google-cloud-sdk
ENV PATH /google-cloud-sdk/bin:$PATH
COPY --from=0 /usr/local /usr/local
WORKDIR /app
COPY .bigqueryrc /root/
COPY . .
ENTRYPOINT ["/app/script/entrypoint"]
