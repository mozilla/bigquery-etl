# fail
{{ is_unique(["country", "state"]) }}

# fail
{{ min_row_count(1000) }}

# fail
-- Each country should have a single state function
SELECT
  mozfun.assert.equals(1, COUNT(DISTINCT state_function))
FROM
  fenix_derived.ltv_state_values_v1
GROUP BY
  country;
# fail
-- There should be more than 2 countries present
SELECT
  `mozfun.assert.true`(COUNT(DISTINCT country) > 2)
FROM
  fenix_derived.ltv_state_values_v1;
