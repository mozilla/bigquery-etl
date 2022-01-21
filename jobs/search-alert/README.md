# Search alert

This script aims to identify and record certain abnormalities in daily search metrics at country level.

## Usage

This script is intended to be run in a docker container.
Build the docker image with:

```sh
docker build -t python-template-job .
```

To run locally, install dependencies with:

```sh
pip install -r requirements.txt
```

Run the script with 

```sh   
python search_alert/main.py --dry_run
```
