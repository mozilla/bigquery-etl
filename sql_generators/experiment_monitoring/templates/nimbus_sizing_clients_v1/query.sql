{%- if dataset == "firefox_desktop" -%}
SELECT
  *
FROM
  `moz-fx-data-shared-prod.firefox_desktop.nimbus_targeting_context`
WHERE
  DATE(submission_timestamp)
  BETWEEN DATE_SUB(@submission_date, INTERVAL 6 DAY)
  AND @submission_date
  AND sample_id < 10
  AND client_info.client_id IS NOT NULL
QUALIFY
  ROW_NUMBER() OVER (PARTITION BY client_info.client_id ORDER BY submission_timestamp DESC) = 1
{%- else -%}
{%- if dataset == "org_mozilla_fenix" -%}
{%- set source_dataset = "fenix_derived" -%}
{%- else -%}
{%- set source_dataset = dataset + "_derived" -%}
{%- endif -%}
SELECT
  *
FROM
  `moz-fx-data-shared-prod.{{ source_dataset }}.nimbus_recorded_targeting_context_v1`
WHERE
  submission_date
  BETWEEN DATE_SUB(@submission_date, INTERVAL 6 DAY)
  AND @submission_date
  AND ABS(MOD(FARM_FINGERPRINT(client_id), 100)) < 10
  AND client_id IS NOT NULL
QUALIFY
  ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY submission_date DESC) = 1
{%- endif %}
