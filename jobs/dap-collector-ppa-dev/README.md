# PPA Dev DAP Collection Job

This job collects data from a DAP collector for advertising dev tasks and puts them into BQ. This job is based on the dap-collector job.

For more information on the Privacy Preserving Advertising Dev Environemnt see https://docs.google.com/document/d/1lUIMtKuTGtJuuZNXwZ9BOdvWjryfn1c_d8zDbg28TY0/edit#heading=h.tz2u9iko0a50

## Usage

This script is intended to be run in a docker container.
Build the docker image with:

```sh
docker build -t dap-collector-ppa-dev .
```

To run locally, install dependencies with (in jobs/dap-collector):

```sh
git clone --depth 1 https://github.com/divviup/janus.git --branch '0.7.20'
cd janus
cargo build -r -p janus_tools --bin collect
cp target/release/collect ../dap_collector_ppa_dev/
cd ..
pip install -r requirements.txt
```

Run the script with (needs gcloud auth):

```sh
python3 main.py --date=... --project=... --ad-table-id=... --report-table-id=... --auth-token=... --hpke-private-key=... --task-config-url=... --ad-config-url=...
```
