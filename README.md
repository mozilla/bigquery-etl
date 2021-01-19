# etl-graph

## Quickstart

Run the crawler.

```bash
# optional: virtualenv
python3 -m venv venv
source venv/bin/activate
pip-compile
pip install -r requirements.txt

# Generate table entities and resolve view references. This is no longer necessary
# and is an artifact of the exploratory phase of the project.
python -m etl-graph crawl

# generate edgelist from query logs
python -m etl-graph query-logs query_log_edges
python -m etl-graph query-logs query_log_nodes

# generate final index
python -m etl-graph index
```

Start the web client for visualization.

```bash
npm run dev
```

Deploy to hosting.

```bash
./deploy.sh
```
