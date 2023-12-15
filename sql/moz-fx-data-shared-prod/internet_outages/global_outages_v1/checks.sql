#fail
{{ min_row_count(1000, where="DATE(datetime) = @submission_date") }}

#fail
{{ is_unique(columns=["datetime", "city", "country"], where="DATE(`datetime`) = @submission_date") }}

#fail
/*
  This statement used to contain the following fields,
  but these are sometimes missing from country/city combinations
  See https://sql.telemetry.mozilla.org/queries/96541/source
  and bug 1868674

  "avg_tls_handshake_time"
  "count_dns_failure"
*/
{{ not_null(columns=[
  "datetime",
  "city",
  "country",
  "proportion_undefined",
  "proportion_timeout",
  "proportion_abort",
  "proportion_unreachable",
  "proportion_terminated",
  "proportion_channel_open",
  "avg_dns_success_time",
  "missing_dns_success",
  "avg_dns_failure_time",
  "missing_dns_failure",
  "ssl_error_prop",

], where="DATE(`datetime`) = @submission_date") }}

#warn
{{ value_length(column="country", expected_length=2, where="DATE(`datetime`) = @submission_date") }}
