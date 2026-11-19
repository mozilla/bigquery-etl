"""Export referral install totals to a CSV file in the Website team's GCS bucket."""

from bigquery_etl.referral_export import export_referral_totals_to_gcs

if __name__ == "__main__":
    export_referral_totals_to_gcs()
