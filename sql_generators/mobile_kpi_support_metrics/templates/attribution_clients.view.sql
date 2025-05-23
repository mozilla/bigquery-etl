{{ header }}
CREATE OR REPLACE VIEW
  `{{ project_id }}.{{ dataset }}.{{ name }}`
AS
SELECT
  submission_date,
  client_id,
  sample_id,
  normalized_channel,
  {% for attribution_group in product_attribution_groups if attribution_group.name in ["adjust", "play_store", "meta"] %}
  {% for attribution_field in attribution_group.fields %}
    {% if app_name == "fenix" and attribution_field.name == "adjust_campaign" %}
      CASE
        WHEN adjust_info.adjust_network IN ('Google Organic Search', 'Organic')
          THEN 'Organic'
        ELSE NULLIF(adjust_info.adjust_campaign, "")
      END AS adjust_campaign,
    {% elif attribution_field.type == "STRING" %}
      NULLIF({{ attribution_group.name }}_info.{{ attribution_field.name }}, "") AS {{ attribution_field.name }},
    {% else %}
      {{ attribution_group.name }}_info.{{ attribution_field.name }},
    {% endif %}
  {% endfor %}
  {% endfor %}
  {% if 'install_source' in product_attribution_group_names %}
  install_source,
  {% endif %}
  {% if 'is_suspicious_device_client' in product_attribution_group_names %}
  is_suspicious_device_client,
  {% endif %}
  {% if 'distribution_id' in product_attribution_group_names %}
  distribution_id,
  {% endif %}
  {% if 'adjust_network' in product_attribution_fields %}
    `moz-fx-data-shared-prod.udf.organic_vs_paid_mobile`(adjust_info.adjust_network) AS paid_vs_organic,
  {% else %}
    "Organic" AS paid_vs_organic,
  {% endif %}
  FROM
  `{{ project_id }}.{{ dataset }}_derived.{{ name }}_{{ version }}`
