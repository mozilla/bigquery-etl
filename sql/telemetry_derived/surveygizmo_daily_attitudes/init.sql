CREATE TABLE IF NOT EXISTS
  `moz-fx-data-shared-prod.telemetry_derived.survey_gizmo_daily_attitudes` (
    date DATE,
    shield_id STRING,
    question STRING,
    question_key STRING,
    value STRING
  )
PARTITION BY
  date
CLUSTER BY
  question_key
