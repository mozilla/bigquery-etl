# Desktop, Mobile and Pocket KPI Forecast Automation

This job contains scripts for projecting year-end KPI values for Desktop 
QCDOU, Mobile CDOU and Pocket ().

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

Run the desktop/mobile/pocket scripts with: 

```sh   
python kpi_forecasting.py -c yaml/desktop.yaml

python kpi_forecasting.py -c yaml/mobile.yaml

(TODO) python kpi_forecasting.py -c yaml/pocket.yaml
```

### YAML Configs

For consistency, keys are lowercased

* target: platform you wish to run, current accepted values are 'desktop', 'mobile' and 'pocket'
* query_name: the name of the .sql file in the sql_queries folder to use to pull data from
* columns: will cut down query to only the columns included in this list. Rather than try to be so supremely flexible that it ends up making more work down the road to comply to an API spec, this repo is set up to handle the desktop, mobile and pocket scripts as they currently exist. If you wish to add a new forecast, model it after Mobile
* forecast_parameters: model fit parameters, must conform to prophet API spec
* dataset_project: the project to use for pulling data from, e.g. mozdata
* write_project: project that results will be written too, e.g. moz-fx-data-bq-data-science
* output_table: table to write results to, if testing consider something like {your-name}.automation_experiment
* forecast_variable: the variable you are actually forecasting, e.g. QDOU or DAU

## Development

./kpi_forecasting.py is the main control script
/Utils contains the bulk of the python code
/yaml contains configuration yaml files
/sql_queries contains the queries to pull data from bigquery
