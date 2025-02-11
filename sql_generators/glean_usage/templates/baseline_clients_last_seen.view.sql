{{ header }}

CREATE OR REPLACE VIEW
  `{{ project_id }}.{{ last_seen_view }}`
AS
SELECT
  {% if app_name == "firefox_desktop" %}
    `moz-fx-data-shared-prod`.udf.pos_of_trailing_set_bit(days_desktop_active_bits) AS days_since_desktop_active,
  {% endif %}
  {% for ut in usage_types %}
    `moz-fx-data-shared-prod`.udf.pos_of_trailing_set_bit(days_{{ ut }}_bits) AS days_since_{{ ut }},
  {% endfor %}
  *
FROM
  `{{ project_id }}.{{ last_seen_table }}`
