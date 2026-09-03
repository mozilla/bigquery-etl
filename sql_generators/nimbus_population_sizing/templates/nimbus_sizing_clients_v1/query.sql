SELECT
  *
FROM
  `{{ source_table }}`
WHERE
{% if is_desktop -%}
  DATE(submission_timestamp)
  BETWEEN DATE_SUB(@submission_date, INTERVAL 6 DAY)
  AND @submission_date
  AND sample_id < 10
  AND client_info.client_id IS NOT NULL
QUALIFY
  ROW_NUMBER() OVER (PARTITION BY client_info.client_id ORDER BY submission_timestamp DESC) = 1
{%- else %}
  submission_date
  BETWEEN DATE_SUB(@submission_date, INTERVAL 6 DAY)
  AND @submission_date
  AND ABS(MOD(FARM_FINGERPRINT(client_id), 100)) < 10
  AND client_id IS NOT NULL
QUALIFY
  ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY submission_date DESC) = 1
{%- endif %}
