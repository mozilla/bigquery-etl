SELECT
  first_column,
  {{ column }}.*
FROM
  dataset.{{ table }}
