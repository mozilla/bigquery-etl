# Desktop and Mobile MAU Dashboard

This job contains scripts that generate a static webpage containing dashboards
for either desktop or mobile product MAU.  The webpage is then uploaded to
GCS to be used by [ProtoDash](https://github.com/mozilla/protodash).

## Usage

This script is intended to be run in a docker container.
Build the docker image with:

```sh
docker build -t desktop-mobile-mau-2020 .
```

To run locally, install dependencies with:

```sh
pip install -r requirements.txt
```

Run the desktop/mobile scripts with: 

```sh   
python3 -m mobile_mau.mobile_mau --project=test-project --bucket-name=test-bucket

python3 -m desktop_mau.desktop_mau_dau --project=test-project --bucket-name=test-bucket
```

## Development

Run tests with:

```sh
pytest
```

`flake8` and `black` are included for code linting and formatting:

```sh
pytest --black --flake8
```

or

```sh
flake8 python_template_job/ tests/
black --diff python_template_job/ tests/
```
