-- {'name': 'zyte_cache_failures_critical', 'alert_type': 'count', 'range': {'max': 200}, 'collections': ['Home Newtab'], 'schedule': 'Default Schedule - 13:00 UTC'}
SELECT
  zyte_cache_failure AS value
FROM
  `{{ project_id }}.{{ dataset_id }}.{{ table_name }}`
WHERE
  submission_date = CURRENT_DATE()
  AND zyte_cache_failure > 200;

-- {'name': 'rss_feed_items_failures_critical', 'alert_type': 'count', 'range': {'max': 200}, 'collections': ['Home Newtab'], 'schedule': 'Default Schedule - 13:00 UTC'}
SELECT
  rss_feed_items_failure AS value
FROM
  `{{ project_id }}.{{ dataset_id }}.{{ table_name }}`
WHERE
  submission_date = CURRENT_DATE()
  AND rss_feed_items_failure > 200;
