#warn
{{ min_row_count(500, where="DATE(`datetime`) = @submission_date") }}

#warn
{{ is_unique(columns=["country","city","geo_subdivision1","geo_subdivision2", "isp_name", "datetime"], where="DATE(`datetime`) = @submission_date") }}

#warn
{{ not_null(columns=[
  "country",
  "city",
  "datetime",
], where="DATE(`datetime`) = @submission_date") }}

#warn
{{ value_length(column="country", expected_length=2, where="DATE(`datetime`) = @submission_date") }}
