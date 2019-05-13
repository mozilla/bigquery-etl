#!/bin/bash

## Affected period to delete is
## 2019-05-04T11:00:00Z to 2019-05-11T11:00:00Z

## Deletion in these buckets requires ops credentials.

function delete_hours() {
    local target_dir=$1

    gsutil -m rm -r $target_dir/2019-05-04/11/
    gsutil -m rm -r $target_dir/2019-05-04/12/
    gsutil -m rm -r $target_dir/2019-05-04/13/
    gsutil -m rm -r $target_dir/2019-05-04/14/
    gsutil -m rm -r $target_dir/2019-05-04/15/
    gsutil -m rm -r $target_dir/2019-05-04/16/
    gsutil -m rm -r $target_dir/2019-05-04/17/
    gsutil -m rm -r $target_dir/2019-05-04/18/
    gsutil -m rm -r $target_dir/2019-05-04/19/
    gsutil -m rm -r $target_dir/2019-05-04/20/
    gsutil -m rm -r $target_dir/2019-05-04/21/
    gsutil -m rm -r $target_dir/2019-05-04/22/
    gsutil -m rm -r $target_dir/2019-05-04/23/

    for day in $(seq -f "%02g" 05 10); do
        gsutil -m rm -r $target_dir/2019-05-${day}/
    done

    gsutil -m rm -r $target_dir/2019-05-11/00/
    gsutil -m rm -r $target_dir/2019-05-11/01/
    gsutil -m rm -r $target_dir/2019-05-11/02/
    gsutil -m rm -r $target_dir/2019-05-11/03/
    gsutil -m rm -r $target_dir/2019-05-11/04/
    gsutil -m rm -r $target_dir/2019-05-11/05/
    gsutil -m rm -r $target_dir/2019-05-11/06/
    gsutil -m rm -r $target_dir/2019-05-11/07/
    gsutil -m rm -r $target_dir/2019-05-11/08/
    gsutil -m rm -r $target_dir/2019-05-11/09/
    gsutil -m rm -r $target_dir/2019-05-11/10/
}

# telemetry-raw_gcs-sink
# Error:  gs://moz-fx-data-stage-landfill/telemetry-error (Empty)
# Main: gs://moz-fx-data-stage-landfill/telemetry
delete_hours gs://moz-fx-data-stage-landfill/telemetry

# telemetry-decoded_gcs-sink
# Error:  gs://moz-fx-data-stage-data/telemetry-decoded-error (Empty)
# Main: gs://moz-fx-data-stage-data/telemetry-decoded
delete_hours gs://moz-fx-data-stage-data/telemetry-decoded

# telemetry-error_gcs-sink
# Error:  gs://moz-fx-data-stage-data/telemetry-error (Empty)
# Main: gs://moz-fx-data-stage-data/telemetry
delete_hours gs://moz-fx-data-stage-data/telemetry

# telemetry-decoded_bq-sink
# Main: output to BQ, so not applicable
# Error:  gs://moz-fx-data-stage-data/telemetry-bq-sink-error
delete_hours gs://moz-fx-data-stage-data/telemetry-bq-sink-error

# structured-raw_gcs-sink
# Error:  gs://moz-fx-data-stage-landfill/structured-error (Empty)
# Main: gs://moz-fx-data-stage-landfill/structured
delete_hours gs://moz-fx-data-stage-landfill/structured

# structured-decoded_gcs-sink
# Error:  gs://moz-fx-data-stage-data/structured-decoded-error (Empty)
# Main: gs://moz-fx-data-stage-data/structured-decoded
delete_hours gs://moz-fx-data-stage-data/structured-decoded

# structured-error_gcs-sink
# Error:  gs://moz-fx-data-stage-data/structured-error (Empty)
# Main: gs://moz-fx-data-stage-data/structured
delete_hours gs://moz-fx-data-stage-data/structured

# structured-decoded_bq-sink
# Main: output to BQ, so not applicable
# Error:  gs://moz-fx-data-stage-data/structured-bq-sink-error
delete_hours gs://moz-fx-data-stage-data/structured-bq-sink-error
