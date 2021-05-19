-- Generated via ./bqetl glean_usage generate
CREATE OR REPLACE VIEW
    `{{ project_id }}.{{ target_view }}`
AS
{% for (dataset, channel) in datasets -%}
{% if not loop.first -%}
UNION ALL
{% endif -%}

SELECT 
    e.*
    EXCEPT (events, metrics)
    REPLACE(
        {% if app_name == "fenix" -%}
        mozfun.norm.fenix_app_info("{{ dataset }}", client_info.app_build).channel AS normalized_channel,
        {% else -%}
        "{{ channel }}" AS normalized_channel,
        {% endif -%}
        -- Order of ping_info fields differs between tables; we're verbose here for compatibility
        STRUCT (
            ping_info.end_time,
            ping_info.experiments,
            ping_info.ping_type,
            ping_info.seq,
            ping_info.start_time,
            ping_info.reason,
            ping_info.parsed_start_time,
            ping_info.parsed_end_time
        ) AS ping_info
    ),
    event.timestamp AS event_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event.extra AS event_extra,
FROM `{{ project_id }}.{{ dataset }}.events` AS e
CROSS JOIN UNNEST(e.events) AS event
{% endfor %}
