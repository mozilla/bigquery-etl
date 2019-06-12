ARG PYTHON_VERSION=3.7

# build typed-ast in separate stage because it requires gcc and libc-dev
FROM python:${PYTHON_VERSION}-alpine
COPY requirements.txt constraints.txt ./
RUN apk add --no-cache gcc libc-dev && \
    pip install -r requirements.txt

FROM python:${PYTHON_VERSION}-alpine
# add bash for entrypoing and python2 for google-cloud-sdk
RUN apk add --no-cache bash python2
COPY --from=google/cloud-sdk:alpine /google-cloud-sdk /google-cloud-sdk
ENV PATH /google-cloud-sdk/bin:$PATH
COPY --from=0 /usr/local /usr/local
WORKDIR /app
COPY .bigqueryrc /root/
COPY . .
RUN [ "python", "script/generate_sql" ]
RUN mv target/sql/ sql/
ENTRYPOINT ["/app/script/entrypoint"]
