The aggregation test for `clients_daily` was removed in
https://github.com/mozilla/bigquery-etl/pull/2117
due to the difficulty of updating the case to account for new fields
and the increasing query complexity which was preventing successful
runs in the test environment.
