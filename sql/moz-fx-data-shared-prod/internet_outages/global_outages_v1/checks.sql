#fail
{{ min_row_count(1000, where="DATE(datetime) = @submission_date") }}

#fail
{{ is_unique(columns=["datetime", "city", "country"], where="DATE(`datetime`) = @submission_date") }}

#fail
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
  "count_dns_failure",
  "ssl_error_prop",
  "avg_tls_handshake_time"

], where="DATE(`datetime`) = @submission_date") }}

#warn
{{ value_length(columns=["country"], expected_length=2, where="DATE(`datetime`) = @submission_date") }}
