{{ header }}
CREATE OR REPLACE VIEW
  `{{ project_id }}.{{ dataset }}.{{ name }}`
AS
SELECT
  client_id,
  sample_id,
  {% if 'install_source' in product_attribution_group_names %}
  install_source,
  {% endif %}
  {% if 'adjust' in product_attribution_group_names %}
  adjust_info.* EXCEPT(submission_timestamp),
  adjust_info.submission_timestamp AS adjust_attribution_timestamp,
  {% endif %}
  {% if 'play_store' in product_attribution_group_names %}
  play_store_info.* EXCEPT(submission_timestamp),
  play_store_info.submission_timestamp AS play_store_attribution_timestamp,
  {% endif %}
  {% if 'meta' in product_attribution_group_names %}
  meta_info.* EXCEPT(submission_timestamp),
  meta_info.submission_timestamp AS meta_attribution_timestamp,
  {% endif %}
  {% if 'adjust_network' in product_attribution_fields %}
    `moz-fx-data-shared-prod.udf.organic_vs_paid_mobile`(adjust_network) AS paid_vs_organic,
  {% else %}
    "Organic" AS paid_vs_organic,
  {% endif %}
FROM
  `{{ project_id }}.{{ dataset }}.{{ name }}_{{ version }}`
