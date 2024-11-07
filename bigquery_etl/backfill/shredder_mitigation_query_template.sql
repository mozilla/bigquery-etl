-- Query generated using a template for shredder mitigation.
WITH {{ new_version_cte | default('new_version') }} AS (
  {{ new_version | default('SELECT 1')  }}
),
{{ new_agg_cte | default('new_agg') }} AS (
  {{ new_agg | default('SELECT 1')  }}
),
{{ previous_agg_cte | default('previous_agg') }} AS (
  {{ previous_agg | default('SELECT 1')  }}
),
{{  shredded_cte | default('shredded') }} AS (
  {{ shredded | default('SELECT 1')  }}
)
SELECT
  {{ final_select | default('1') }}
FROM
  {{ new_version_cte | default('new_version') }}
UNION ALL
SELECT
  {{ final_select | default('1') }}
FROM
  {{ shredded_cte | default('shredded') }}
