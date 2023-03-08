# DAP Collection Job

This job collects data from a DAP collector and puts them into BQ. For more
information on DAP see https://github.com/ietf-wg-ppm/draft-ietf-ppm-dap

For more information on Privacy Preserving Measurement in Firefox see https://bugzilla.mozilla.org/show_bug.cgi?id=1775035

## Usage

This script is intended to be run in a docker container.
Build the docker image with:

```sh
podman build -t dap-collector .
```

To run locally, install dependencies with (in jobs/dap-collector):

```sh
git clone --depth 1 https://github.com/divviup/janus.git --branch 0.2.7 --single-branch
cd janus
cargo build -p janus_collector --example collect
cd ..
pip install -r requirements.txt
```

Run the script with (needs gcloud auth):

```sh
AUTH_TOKEN="…" HPKE_PRIVATE_KEY="…" python dap_collector/main.py
```
