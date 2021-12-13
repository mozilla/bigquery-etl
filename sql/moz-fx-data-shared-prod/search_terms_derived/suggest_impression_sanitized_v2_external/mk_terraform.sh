#!/bin/bash

# This is a script intended to be run from the query directory;
# it reads in the query.sql and outputs a terraform configuration for
# a BQ data tranfer that we then commit separately to cloudops-infra.

cd "$(dirname "$0")"

cat <<EOF
resource "google_bigquery_data_transfer_config" "query_config" {
  display_name           = "suggest-sanitization-v2"
  location               = "US"
  data_source_id         = "suggest-sanitization-v2"
  schedule               = "every day at 02:00"
  destination_dataset_id = google_bigquery_dataset.my_dataset.dataset_id
  service_account_name   = "foo"
  params = {
    destination_table = "suggest_impression_sanitized_v2"
    write_disposition = "WRITE_TRUNCATE"
    query             = <<EOT
$(cat query.sql | sed 's/@submission_date/DATE_SUB(@run_date, INTERVAL 1 DAY)/g')
EOT
  }
}

EOF
