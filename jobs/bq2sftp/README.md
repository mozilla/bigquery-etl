# Export from BigQuery to SFTP

See https://bugzilla.mozilla.org/show_bug.cgi?id=1729524 for more
context on the need for this. Airflow 2.0 has a
[GCS to SFTP operator](https://airflow.apache.org/docs/apache-airflow-providers-google/stable/operators/transfer/gcs_to_sftp.html)
that we could use, but at time of writing we're in the midst of upgrading
and introducing new dependencies could be difficult.
This container has the additional advantage of not needing to stage results
to a temporary GCS location/

All inputs to this task are via environment variables rather than command-line
arguments.

The `KNOWN_HOSTS` variable should be a string containing the full `.ssh/known_hosts`
entry for the SFTP server being accessed. It should look something like:

```
KNOWN_HOSTS="hostname ssh-rsa AAAAB3NzaC1y...qBJu"
```

The `SUBMISSION_DATE` should be supplied if we only want to export a single
partition of a table. If it's not set, the whole table will be exported.

Example local invocation for testing:

```
docker build -t bq2sftp .
docker run -v ~/.config:/app/.config \
  -e SFTP_USERNAME="" \
  -e SFTP_PASSWORD="" \
  -e SFTP_HOST="" \
  -e SFTP_PORT="" \
  -e KNOWN_HOSTS="" \
  -e SRC_TABLE="" \
  -e DST_PATH="" \
  -e SUBMISSION_DATE="" \
  bq2sftp \
  files/testfile.csv.gz
```
