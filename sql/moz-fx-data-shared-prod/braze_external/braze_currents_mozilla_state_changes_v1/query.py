"""Braze Currents - Mozilla - State Changes."""

from bigquery_etl.braze_currents import import_braze_current_from_bucket

if __name__ == "__main__":
    import_braze_current_from_bucket()
