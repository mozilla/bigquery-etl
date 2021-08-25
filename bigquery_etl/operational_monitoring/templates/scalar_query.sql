{{ header }}

SELECT
    @submission_date AS submission_date,
    client_id,
    SAFE.SUBSTR(application.build_id, 0, 8) AS build_id,
    {% for dimension in dimensions %}
      CAST({{ dimension.sql }} AS STRING) AS {{ dimension.name }},
    {% endfor %}
    mozfun.map.get_key(
      environment.experiments,
      "{{slug}}"
    ).branch AS branch,
    ARRAY<
        STRUCT<
            name STRING,
            agg_type STRING,
            value INT64
        >
    >[
      {% for probe in probes %}
        (
            "{{ probe.name }}",
            "MAX",
            MAX(CAST({{ probe.sql }} AS INT64))
        ),
        (
            "{{ probe.name }}",
            "SUM",
            SUM(CAST({{ probe.sql }} AS INT64))
        )
        {{ "," if not loop.last else "" }}
      {% endfor %}
    ] AS metrics,
FROM
    `{{source}}`
WHERE
    DATE(submission_timestamp) >= DATE_SUB(@submission_date, INTERVAL 60 DAY)
GROUP BY
    submission_date,
    client_id,
    build_id,
    {% for dimension in dimensions %}
      {{ dimension.name }},
    {% endfor %}
    branch
