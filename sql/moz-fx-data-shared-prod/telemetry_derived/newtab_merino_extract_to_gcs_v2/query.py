"""Extract New Tab engagement query results and write the combined JSON to a single file."""

from bigquery_etl.newtab_merino import export_newtab_merino_table_to_gcs

if __name__ == "__main__":
    export_newtab_merino_table_to_gcs()
