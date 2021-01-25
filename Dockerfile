FROM gcr.io/google.com/cloudsdktool/cloud-sdk

RUN apt-get update && apt-get -y install npm

# upgrade node to the latest version
RUN npm install -g n && n stable

WORKDIR /app

# check if dependencies change, otherwise reuse layers
COPY requirements.txt .
RUN pip3 install -r requirements.txt
COPY package.json .
COPY package-lock.json .
RUN npm install

ADD . .
RUN npm run build

CMD scripts/scrape.sh && scripts/deploy-data.sh
