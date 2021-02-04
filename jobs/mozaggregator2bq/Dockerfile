FROM gcr.io/google.com/cloudsdktool/cloud-sdk

RUN echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list && \
    curl https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -

RUN apt update && apt -y install jq postgresql

WORKDIR /app

# check if dependencies change, otherwise reuse layers
COPY requirements.txt .
RUN pip3 install -r requirements.txt

ADD . .
