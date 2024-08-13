"""Braze Currents - Firefox - Click."""

from bigquery_etl.newtab_merino import export_newtab_merino_extract_to_gcs

if __name__ == "__main__":
    export_newtab_merino_extract_to_gcs()
