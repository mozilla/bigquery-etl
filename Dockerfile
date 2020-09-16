ARG PYTHON_VERSION=3.8

# build typed-ast in separate stage because it requires gcc and libc-dev
FROM python:${PYTHON_VERSION}-slim
RUN apt-get update -qqy && apt-get install -qqy gcc libc-dev
COPY requirements.txt ./
RUN pip install -r requirements.txt

FROM python:${PYTHON_VERSION}-slim
# add bash for entrypoint
RUN apt-get update -qqy && apt-get install -qqy bash
COPY --from=google/cloud-sdk:alpine /google-cloud-sdk /google-cloud-sdk
ENV PATH /google-cloud-sdk/bin:$PATH
COPY --from=0 /usr/local /usr/local
WORKDIR /app
COPY .bigqueryrc /root/
COPY . .
RUN pip install .
ENTRYPOINT ["/app/script/entrypoint"]
