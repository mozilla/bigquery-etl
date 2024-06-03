# Python Template Job

This dockerized Python job has many scripts that integrate with many different third-party applications.

* **workday_xmatters.py**: this script syncronizes employee data from Workday to XMatters system.    
    * Expected environment variables :  
        * Workday API: XMATTERS_INTEG_WORKDAY_USERNAME, XMATTERS_INTEG_WORKDAY_PASSWORD
        * XMatters API:  XMATTERS_USERNAME, XMATTERS_PASSWORD, XMATTERS_URL, XMATTERS_SUPERVISOR_ID
        * Google Maps API: MOZGEO_GOOGLE_API_KEY
        
 
## Usage

This script is intended to be run in a docker container.
Build the docker image with:

```sh
docker build -t eam-integrations .
```

To run locally, install dependencies with:

```sh
pip install -r requirements.txt
```

Run the script with 

```sh   
>python .\workday_xmatters.py -h
usage: workday_xmatters.py [-h] [-l LEVEL] [-f]

Sync up XMatters with Workday

options:
  -h, --help            show this help message and exit
  -l LEVEL, --level LEVEL
                        log level (debug, info, warning, error, or critical)
  -f, --force           force changes even if there are a lot
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
flake8 eam-integrations/ tests/
black --diff eam-integrations/ tests/
```
