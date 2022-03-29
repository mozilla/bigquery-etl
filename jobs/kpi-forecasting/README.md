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

### On SQL Queries And Preprocessing

One of the challenges in automating multiple forecasts is that data formats may not always be consistent. This is a challenge precisely because Prophet demands that data conform to a very strict standard of two columns; 'ds' and 'y' as its primary inputs. Other columns can be added as regressors, but often these are trivial to implement in Python but challenging to implement in SQL, or vice versa, depending on their specifics. 

Rather than try and force things to conform to an ultra-strict standard to solve a wide variety of forecasts, this repo focuses ONLY on KPIs, and is organized in such a way to make that easier to maintain.

The idiomatic way to handle this is to use the "columns" key in the YAML to specify which columns to bring into the FitForecast section of this stack, and to write a specific handler for regressors in that section. Your query should return things in this column order:

| date | variable to predict | regressor_00 | regressor_01 | etc |
|------|---------------------|--------------|--------------|-----|

Failure to conform to this order will result in failure as prophet model inputs are taken by POSITION, rather than by key. You will want to maintain this order in your YAML columns list as well.

Is this generic? No. Nor does it try to be. However, because of the limited number of cases, it makes more sense to handle the cases this repo is trying to cover rather than hypothetical future ones.

### YAML Configs

For consistency, keys are lowercased

* target: platform you wish to run, current accepted values are 'desktop', 'mobile' and 'pocket'
* query_name: the name of the .sql file in the sql_queries folder to use to pull data from
* columns: will cut down query to only the columns included in this list. Rather than try to be so supremely flexible that it ends up making more work down the road to comply to an API spec, this repo is set up to handle the desktop, mobile and pocket scripts as they currently exist. If you wish to add a new forecast, model it after Mobile
* forecast_parameters: model fit parameters, must conform to prophet API spec
* dataset_project: the project to use for pulling data from, e.g. mozdata
* write_project: project that results will be written too, e.g. moz-fx-data-bq-data-science
* output_table: table to write results to, if testing consider something like {your-name}.automation_experiment
* confidences_table: table to write confidences too, if confidences is not None. if it is, will be ignored
* forecast_variable: the variable you are actually forecasting, e.g. QDOU or DAU
* holidays: boolean - include holidays (if set to False holidays will always show zero, but the columns will still exist)
* stop_date: date to stop the forecast at
* confidences: aggregation unit for confidence intervals, can be ds_month, ds_year or None

## Development

./kpi_forecasting.py is the main control script
/Utils contains the bulk of the python code
/yaml contains configuration yaml files
/sql_queries contains the queries to pull data from bigquery
