-- {
--   "name": "Fenix releases version format",
--   "alert_conditions": "value",
--   "range": {
--     "min": 0,
--     "max": 1
--   },
--   "collections": ["Test"],
--   "owner": "",
--   "schedule": "Default Schedule - 13:00 UTC"
-- }
SELECT
  ROUND((COUNTIF(NOT REGEXP_CONTAINS(version, r"^[0-9]+\..+$"))) / COUNT(*) * 100, 2) AS perc
FROM
  `{{ project_id }}.{{ dataset_id }}.{{ table_name }}`;

-- {
--   "name": "Fenix releases product check",
--   "alert_conditions": "value",
--   "range": {
--     "min": 0,
--     "max": 1
--   },
--   "collections": ["Test"],
--   "owner": "",
--   "schedule": "Default Schedule - 13:00 UTC"
-- }
SELECT
  ROUND((COUNTIF(product != "fenix")) / COUNT(*) * 100, 2) AS perc
FROM
  `{{ project_id }}.{{ dataset_id }}.{{ table_name }}`;
