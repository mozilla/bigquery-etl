-- Test query for Claude review
-- Intentionally has some issues Claude should catch
SELECT
  *
FROM
  `project.dataset.events`
WHERE
  user_id = '{{ user_input }}'
