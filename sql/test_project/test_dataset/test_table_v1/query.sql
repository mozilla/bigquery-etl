-- Test query with issues for Claude to review
  SELECT
    *
  FROM
    `project.dataset.table`
  WHERE
    user_id = '{{ user_input }}'
