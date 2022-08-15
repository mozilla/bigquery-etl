FROM python:3.7
MAINTAINER Chelsea Troy <ctroy@mozilla.com>

WORKDIR /usr/app/src

RUN pip install --upgrade pip
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY data_validation.py ./
COPY data_validation_job.py ./

CMD [ "python", "./data_validation_job.py", "--data_validation_origin", "mozdata.search_terms_unsanitized_analysis.prototype_data_validation_metrics" ]
