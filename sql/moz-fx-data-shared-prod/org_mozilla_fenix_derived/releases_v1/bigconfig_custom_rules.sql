-- {
--   "name": "custom_check_name",
--   "alert_conditions": "count",
--   "range": {
--     "min": 2,
--     "max": 10
--   },
--   "collections": ["Test"],
--   "owner": "asf",
--   "schedule": "Default Schedule - 13:00 UTC"
-- }
SELECT
  ROUND((COUNTIF(NOT REGEXP_CONTAINS(version, r"^[0-9]+\..+$"))) / COUNT(*) * 100, 2) AS perc
FROM {{ project_id }}.{{ dataset_id }}.{{ table_name }};

-- {
--   "name": "custom_check_name_2",
--   "alert_conditions": "value",
--   "range": {
--     "min": 2,
--     "max": 10
--   },
--   "collections": ["Test"],
--   "owner": "asf",
--   "schedule": "Default Schedule - 13:00 UTC"
-- }
SELECT
  ROUND((COUNTIF(product != "fenix")) / COUNT(*) * 100, 2) AS perc
FROM {{ project_id }}.{{ dataset_id }}.{{ table_name }};

