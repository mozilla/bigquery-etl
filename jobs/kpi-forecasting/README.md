# Desktop, Mobile and Pocket KPI Forecast Automation

This job contains scripts for projecting year-end KPI values for Desktop 
QCDOU, Mobile CDOU and Pocket () [ProtoDash](https://github.com/mozilla/protodash).

## Usage

This script is intended to be run in a docker container.
Build the docker image with:

```sh
docker build -t kpi-forecasting .
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
