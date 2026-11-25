#fail
{{ is_unique(["task_id"], where="submission_date = @submission_date") }}

#warn
{{ not_null([
  "task_id",
  "submission_date",
  "scheduler_id",
  "task_group_id",
  "task_queue_id"
  ], where="submission_date = @submission_date") }}

#warn
{{ min_row_count(1, where="submission_date = @submission_date") }}
