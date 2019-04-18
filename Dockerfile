ARG PYTHON_VERSION=3.7

# build typed-ast in separate stage because it requires gcc and libc-dev
FROM python:${PYTHON_VERSION}-alpine
COPY constraints.txt .
RUN apk add --no-cache gcc libc-dev && \
    grep typed-ast constraints.txt | pip install -r /dev/stdin

FROM python:${PYTHON_VERSION}-alpine
RUN apk add --no-cache bash
COPY --from=google/cloud-sdk:alpine /google-cloud-sdk /google-cloud-sdk
ENV PATH /google-cloud-sdk/bin:$PATH
WORKDIR /app
COPY --from=0 /usr/local/lib/python3.7/site-packages /usr/local/lib/python3.7/site-packages
COPY requirements.txt constraints.txt ./
RUN pip install -r requirements.txt
COPY . .
ENTRYPOINT ["/app/script/entrypoint"]
