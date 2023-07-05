# Python Template Job

## Usage

This script is intended to be run in a docker container.
Build the docker image with:

```sh
docker build -t client-generation .
```

To run locally, install dependencies with:

```sh
pip install -r requirements.txt
```

Run the script with 

```sh   
python3 -m client_regeneration.main
```

or as a container

```sh
docker run client-regeneration python client_regeneration/main.py --seed 10
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
flake8 client_regeneration/ tests/
black --diff client_regeneration/ tests/
```
