FROM python:3
MAINTAINER Chelsea Troy <ctroy@mozilla.com>

WORKDIR /usr/app/src

RUN pip install --upgrade pip
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .
