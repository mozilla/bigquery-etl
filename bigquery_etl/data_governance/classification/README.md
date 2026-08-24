# Column classification

Labels the columns of BigQuery tables with a category from the Mozilla data taxonomy. A Gemini model classifies each column using its name, description, profiling statistics, sample values, and linked telemetry metadata.
Results are stored in `column_classifications_v1` for auditing and analysis.

Tracked in [DENG-11453](https://mozilla-hub.atlassian.net/browse/DENG-11453).

## How it runs

The pipeline has four `bqetl` steps invoked by Airflow:

```sh
./bqetl data_governance profile  --date 2026-08-20 moz-fx-data-shared-prod.telemetry_derived
./bqetl data_governance lineage  --date 2026-08-20
./bqetl data_governance probes   --date 2026-08-20
./bqetl data_governance classify moz-fx-data-shared-prod.telemetry_derived
```

The `profile`, `lineage`, and `probes` steps run the data governance metadata jobs
against the dataset being classified. Their outputs serve as classification inputs.

For each classified column, `classify` builds a prompt, makes one model call, and records the predicted label, confidence, reasoning, and the inputs it was given. Sample values are sanitized with DLP before being sent to the model.

The pipeline is resumable and can be re-run on the same dataset. Columns already classified with the same model are skipped unless `--refresh` is passed. Rows are written in batches and each column's existing row is deleted before the new one is appended, so re-classification replaces it.

Each step writes to a table in the `data_governance_metadata_derived` dataset.

## Reusing the metadata jobs

The weekly metadata pipeline profiles telemetry datasets and replaces an entire date partition. To classify other datasets without disturbing these outputs, this pipeline invokes the same jobs independently and writes to interim `classification_*` tables. These copies will be removed once the profiler supports additive writes.

## Code structure

Start in `../cli.py`, which defines the four subcommands. The first three delegate to `upstream.py`; `classify` enters `runner.py`.

For classification, `runner.py` reads BigQuery inputs through `inputs.py`, scrubs samples through `sanitize.py`, builds prompts in `prompt.py`, and calls Vertex through `models.py`. `taxonomy_v1.json` is the label set included in each prompt.
