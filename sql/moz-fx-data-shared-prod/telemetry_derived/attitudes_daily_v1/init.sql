CREATE TABLE IF NOT EXISTS `moz-fx-data-shared-prod.telemetry_derived.attitudes_daily_v1`
PARTITION BY submission_date
CLUSTER BY question_key
AS SELECT
    *,
    cast(null as DATE) as `date`,
    cast(null as STRING) as question,
    cast(null as STRING) as question_key,
    cast(null as STRING) as value,
    cast(null as STRING) as survey_id
FROM telemetry.clients_daily
WHERE FALSE
