SELECT
  * EXCEPT (submission_date)
FROM
  {{ app_id }}_derived.event_types_history_v1
WHERE
  submission_date = @submission_date
