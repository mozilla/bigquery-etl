FROM python:3
MAINTAINER Chelsea Troy <ctroy@mozilla.com>

WORKDIR /usr/app/src

RUN pip install --upgrade pip
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY src/data_validation.py ./
COPY src/data_validation_job.py ./
COPY tests/test_data_validation.py ./

CMD [ "python", "./data_validation_job.py", "--data_validation_origin", "mozdata.search_terms_unsanitized_analysis.prototype_data_validation_metrics" ]
