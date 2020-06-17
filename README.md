# etl-graph

## Quickstart

Run the crawler.

```bash
# optional: virtualenv
python3 -m venv venv
source venv/bin/activate

# generate table entities and resolve view references
python -m etl-graph crawl

# generate edgelist from query logs
python -m etl-graph query-logs

# generate final index
python -m etl-graph index
```

Start the web client for visualization.

```bash
python3 -m http.server
open index.html
```

Deploy to hosting.

```bash
./deploy.sh
```
