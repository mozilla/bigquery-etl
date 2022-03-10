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

### YAML Configs

For consistency, keys are lowercased

* target - platform you wish to run, current accepted values are 'desktop', 'mobile' and 'pocket'
* query_name - the name of the .sql file in the sql_queries folder to use to pull data from
* columns - will cut down query to only the columns included in this list. Rather than try to be so supremely flexible that it ends up making more work down the road to comply to an API spec, this repo is set up to handle the desktop, mobile and pocket scripts as they currently exist. If you wish to add a new forecast, model it after Mobile
* forecast_parameters - model fit parameters, must conform to prophet API spec

## Development

Run manually with:

```
python kpi_forecasting.py -c yaml/{desired platform}.yaml 
```
