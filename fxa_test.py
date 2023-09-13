from google.cloud import bigquery
import json
import copy
from datetime import date, timedelta

client = bigquery.Client()


new_log_tables = ["mozdata.tmp.akomar_fxa_gcp_stdout_events_v1","mozdata.tmp.akomar_fxa_gcp_stderr_events_v1"]
for table_id in new_log_tables:
    try:
        print("Deleting table '{}'.".format(table_id))
        client.delete_table(table_id)
        print("Deleted table '{}'.".format(table_id))
    except:
        print("Table '{}' not found.".format(table_id))


# days to backfill new tables
start_date = date(2023, 9, 7)
end_date = date(2023, 9, 12)
delta = timedelta(days=1)
dates = []
while start_date <= end_date:
    dates.append(start_date.strftime("%Y-%m-%d"))
    start_date += delta

new_log_tables_queries = [("sql/mozdata/tmp/akomar_fxa_gcp_stdout_events_v1/query.sql", "mozdata.tmp.akomar_fxa_gcp_stdout_events_v1"),
                              ("sql/mozdata/tmp/akomar_fxa_gcp_stderr_events_v1/query.sql", "mozdata.tmp.akomar_fxa_gcp_stderr_events_v1")]
for query_path, table_id in new_log_tables_queries:
    for date in dates:
        with open(query_path, "r") as f:
            print(f"Running query '{query_path}' for {date}.")
            query = f.read()
            job_config = bigquery.QueryJobConfig(
                    destination=table_id,
                    write_disposition="WRITE_APPEND",
                )
            job_config.query_parameters = [
                bigquery.ScalarQueryParameter("submission_date", "DATE", date),
            ]
            query_job = client.query(query, job_config=job_config)
            
            query_job.result()  # Wait for job to complete.
            print("Query results loaded to the table {}".format(query_job.destination))


print("Creating view...")
view_query_path = "sql/mozdata/tmp/akomar_fxa_all_events/view.sql"
with open(view_query_path, "r") as f:
    view_query = f.read()
    view_query_job = client.query(view_query)
    view_query_job.result()  # Wait for job to complete.
    print("View created.")