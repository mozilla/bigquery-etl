-- Checks generated using a template for shredder mitigation.
WITH {{ previous_version_cte | default('previous_version') }} AS (
  {{ previous_version | default('SELECT 1')  }}
),
{{ new_version_cte | default('new_version') }} AS (
  {{ new_version | default('SELECT 1')  }}
)
SELECT
  *
FROM
  {{ previous_version_cte | default('previous_version') }}
EXCEPT DISTINCT
SELECT
  *
FROM
  {{ new_version_cte | default('new_version') }}
