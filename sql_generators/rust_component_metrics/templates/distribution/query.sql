SELECT
    submission_date,
    "{{ application }}" as application,
    channel,
    q[1] as q001,
    q[10] as q01,
    q[50] as q05,
    q[500] as q50,
    q[950] as q95,
    q[990] as q99,
    q[999] as q999
FROM (
  SELECT
      normalized_channel AS channel,
      DATE(submission_timestamp) AS submission_date,
      APPROX_QUANTILES(CAST(values.key AS INT64), 1000) as q
  FROM `mozdata.{{ dataset_name }}.metrics`
  CROSS JOIN UNNEST(metrics.{{ metric.table }}.{{ category }}_{{ metric.name }}.values) as values 
  -- This generates multiple rows based on the `value` field.  This is needed to make the `APPROX_QUANTILES`
  -- weigh `value.key` correctly.
  CROSS JOIN UNNEST(GENERATE_ARRAY(1, `values`.value))
  GROUP BY 1, 2
)
WHERE
  submission_date = @submission_date
