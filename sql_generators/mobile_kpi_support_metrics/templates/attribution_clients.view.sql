{{ header }}
CREATE OR REPLACE VIEW
  `{{ project_id }}.{{ dataset }}.{{ name }}`
AS
SELECT
  submission_date,
  client_id,
  sample_id,
  {% if 'install_source' in product_attribution_group_names %}
  install_source,
  {% endif %}
  {% if 'adjust' in product_attribution_group_names %}
  adjust_info.*,
  {% endif %}
  {% if 'play_store' in product_attribution_group_names %}
  play_store_info.*,
  {% endif %}
  {% if 'meta' in product_attribution_group_names %}
  meta_info.*,
  {% endif %}
  {% if 'adjust_network' in product_attribution_fields %}
    `moz-fx-data-shared-prod.udf.organic_vs_paid_mobile`(adjust_info.adjust_network) AS paid_vs_organic,
  {% else %}
    "Organic" AS paid_vs_organic,
  {% endif %}
  {% if 'is_suspicious_device_client' in product_attribution_group_names %}
  is_suspicious_device_client,
  {% endif %}
  {% if app_name == "fenix" %}
  distribution_id,
  {% endif %}
FROM
  `{{ project_id }}.{{ dataset }}_derived.{{ name }}_{{ version }}`
