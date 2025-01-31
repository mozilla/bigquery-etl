{{ header }}
CREATE OR REPLACE VIEW
  `{{ project_id }}.{{ dataset }}.{{ name }}`
AS
SELECT
  *,
  {% if 'adjust_network' in product_attribution_fields %}
    `moz-fx-data-shared-prod.udf.organic_vs_paid_mobile`(adjust_network) AS paid_vs_organic,
  {% else %}
    "Organic" AS paid_vs_organic,
  {% endif %}
  -- Checking if the client was seen more than once in the first 2 - 7 days
  -- and if they had more than 0 searches within the time window (3 days).
  IF(num_days_seen_day_2_7 > 1 AND search_count > 0, TRUE, FALSE) AS is_activated,
  -- This is based on the more recent DAU definition that takes duration into consideration.
  IF(num_days_active_day_2_7 > 1 AND search_count > 0, TRUE, FALSE) AS is_early_engagement,
FROM
  `{{ project_id }}.{{ dataset }}_derived.{{ name }}_{{ version }}`
