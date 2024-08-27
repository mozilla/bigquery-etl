-- Query generated from a template that mitigates the effect of shredder.
WITH {{ new_version_cte }} AS (
  {{ new_version }}
),
{{ new_agg_cte }} AS (
  {{ new_agg }}
),
{{ previous_agg_cte }} AS (
  {{ previous_agg }}
),
{{  shredded_cte }} AS (
  {{ shredded }}
)
SELECT
    {{ final_select }}
FROM
    {{ new_version_cte }}
UNION ALL
SELECT
  {{ final_select}}
FROM
    {{ shredded_cte }}
;
