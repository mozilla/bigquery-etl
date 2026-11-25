#fail
{{ is_unique(["taskId"], where="submission_date = @submission_date") }}

#warn
{{ not_null([
  "taskId",
  "submission_date",
  "created",
  "deadline",
  "expires",
  "extra",
  "metadata",
  "payload",
  "priority",
  "projectId",
  "provisionerId",
  "requires",
  "retries",
  "schedulerId",
  "tags",
  "taskGroupId",
  "taskQueueId",
  "workerType"
  ], where="submission_date = @submission_date") }}

#warn
{{ min_row_count(1, where="submission_date = @submission_date") }}

#warn
SELECT
  IF(
    COUNTIF(JSON_TYPE(extra) != "object" OR JSON_TYPE(payload) != "object") > 0,
    ERROR("extra or payload is not a JSON object, the legacy value did not parse."),
    NULL
  )
FROM
  `{{ project_id }}.{{ dataset_id }}.{{ table_name }}`
WHERE
  submission_date = @submission_date;
