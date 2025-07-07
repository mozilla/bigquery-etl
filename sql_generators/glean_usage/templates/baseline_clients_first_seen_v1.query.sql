{{ header }}
{% from "macros.sql" import core_clients_first_seen %}

{% raw %}
{% if is_init() %}
{% endraw %}
WITH
  baseline AS (
    SELECT
      client_info.client_id,
      -- Some Glean data from 2019 contains incorrect sample_id, so we
      -- recalculate here; see bug 1707640
      udf.safe_sample_id(client_info.client_id) AS sample_id,
      DATE(MIN(submission_timestamp)) as submission_date,
      DATE(MIN(submission_timestamp)) as first_seen_date,
      ARRAY_AGG(
        client_info.attribution 
        ORDER BY submission_timestamp DESC LIMIT 1
      )[OFFSET(0)] AS attribution,
      ARRAY_AGG(
        client_info.distribution 
        ORDER BY submission_timestamp DESC LIMIT 1
      )[OFFSET(0)] AS `distribution`,
      {% if app_name == "firefox_desktop" %}
      ARRAY_AGG(
        metrics.object.glean_attribution_ext 
        ORDER BY submission_timestamp DESC LIMIT 1
      )[OFFSET(0)] AS attribution_ext,
      ARRAY_AGG(
        metrics.object.glean_distribution_ext 
        ORDER BY submission_timestamp DESC LIMIT 1
      )[OFFSET(0)] AS distribution_ext,
      mozfun.stats.mode_last(
        ARRAY_AGG(
          metrics.uuid.legacy_telemetry_client_id 
          ORDER BY submission_timestamp ASC
          )
      ) AS legacy_telemetry_client_id,
      mozfun.stats.mode_last(
        ARRAY_AGG(
          metrics.uuid.legacy_telemetry_profile_group_id 
          ORDER BY submission_timestamp ASC
          )
      ) AS legacy_telemetry_profile_group_id
      {% endif %}
    FROM
      `{{ baseline_table }}`
    -- initialize by looking over all of history
    WHERE
      DATE(submission_timestamp) > "2010-01-01"
    GROUP BY
      client_id,
      sample_id
  )
{% if fennec_id %}
  ,
  {{ core_clients_first_seen(migration_table) }}
  SELECT
    client_id,
    submission_date,
    COALESCE(core.first_seen_date, baseline.first_seen_date) AS first_seen_date,
    sample_id,
    attribution,
    `distribution`
  FROM baseline
  LEFT JOIN _core_clients_first_seen AS core
  USING (client_id)
{% else %}
  SELECT * FROM baseline
{% endif %}

{% raw %}
{% else %}
{% endraw %}

WITH
{% if fennec_id %}
{{ core_clients_first_seen(migration_table) }},
_baseline AS (
  -- extract the client_id into the top level for the `USING` clause
  SELECT
    client_info.client_id,
    -- Some Glean data from 2019 contains incorrect sample_id, so we
    -- recalculate here; see bug 1707640
    udf.safe_sample_id(client_info.client_id) AS sample_id,
    ARRAY_AGG(client_info.attribution ORDER BY submission_timestamp DESC LIMIT 1)[
      OFFSET(0)
    ] AS attribution,
    ARRAY_AGG(client_info.distribution ORDER BY submission_timestamp DESC LIMIT 1)[
      OFFSET(0)
    ] AS `distribution`,
    {% if app_name == "firefox_desktop" %}
    ARRAY_AGG(
      metrics.object.glean_attribution_ext 
      ORDER BY submission_timestamp DESC LIMIT 1
    )[OFFSET(0)] AS attribution_ext,
    ARRAY_AGG(
      metrics.object.glean_distribution_ext 
      ORDER BY submission_timestamp DESC LIMIT 1
    )[OFFSET(0)] AS distribution_ext,
      mozfun.stats.mode_last(
        ARRAY_AGG(
          metrics.uuid.legacy_telemetry_client_id 
          ORDER BY submission_timestamp ASC
          )
      ) AS legacy_telemetry_client_id,
      mozfun.stats.mode_last(
        ARRAY_AGG(
          metrics.uuid.legacy_telemetry_profile_group_id 
          ORDER BY submission_timestamp ASC
          )
      ) AS legacy_telemetry_profile_group_id
    {% endif %}
  FROM
    `{{ baseline_table }}`
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND client_info.client_id IS NOT NULL -- Bug 1896455
  GROUP BY 
    client_id,
    sample_id
),
_current AS (
  SELECT
    @submission_date as submission_date,
    COALESCE(first_seen_date, @submission_date) as first_seen_date,
    sample_id,
    client_id,
    attribution,
    `distribution`,
    {% if app_name == "firefox_desktop" %}
    attribution_ext,
    distribution_ext,
    legacy_telemetry_client_id,
    legacy_telemetry_profile_group_id
    {% endif %}
  FROM
    _baseline
  LEFT JOIN
    _core_clients_first_seen
    USING (client_id)
),
_previous AS (
  SELECT
    fs.submission_date,
    IF(
      core IS NOT NULL AND core.first_seen_date <= fs.first_seen_date,
      core.first_seen_date,
      fs.first_seen_date
    ) AS first_seen_date,
    sample_id,
    client_id,
    attribution,
    `distribution`,
    {% if app_name == "firefox_desktop" %}
    attribution_ext,
    distribution_ext,
    legacy_telemetry_client_id,
    legacy_telemetry_profile_group_id,
    {% endif %}
  FROM
    `{{ first_seen_table }}` fs
  LEFT JOIN
    _core_clients_first_seen core
    USING (client_id)
  WHERE
    fs.first_seen_date > "2010-01-01"
    AND fs.first_seen_date < @submission_date
)
{% else %}
_current AS (
  SELECT
    @submission_date as submission_date,
    @submission_date as first_seen_date,
    sample_id,
    client_info.client_id,
    ARRAY_AGG(
      client_info.attribution 
      ORDER BY submission_timestamp DESC LIMIT 1
    )[OFFSET(0)] AS attribution,
    ARRAY_AGG(
      client_info.distribution 
      ORDER BY submission_timestamp DESC LIMIT 1
    )[OFFSET(0)] AS `distribution`,
    {% if app_name == "firefox_desktop" %}
    ARRAY_AGG(
      metrics.object.glean_attribution_ext 
      ORDER BY submission_timestamp DESC LIMIT 1
    )[OFFSET(0)] AS attribution_ext,
    ARRAY_AGG(
      metrics.object.glean_distribution_ext 
      ORDER BY submission_timestamp DESC LIMIT 1
    )[OFFSET(0)] AS distribution_ext,
          mozfun.stats.mode_last(
        ARRAY_AGG(
          metrics.uuid.legacy_telemetry_client_id 
          ORDER BY submission_timestamp ASC
          )
      ) AS legacy_telemetry_client_id,
    mozfun.stats.mode_last(
      ARRAY_AGG(
        metrics.uuid.legacy_telemetry_profile_group_id 
        ORDER BY submission_timestamp ASC
        )
    ) AS legacy_telemetry_profile_group_id
    {% endif %}
  FROM
    `{{ baseline_table }}`
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND client_info.client_id IS NOT NULL -- Bug 1896455
  GROUP BY 
    submission_date,
    first_seen_date,
    sample_id,
    client_info.client_id
),
  -- query over all of history to see whether the client_id has shown up before
_previous AS (
  SELECT
    submission_date,
    first_seen_date,
    sample_id,
    client_id,
    attribution,
    `distribution`,
    {% if app_name == "firefox_desktop" %}
    attribution_ext,
    distribution_ext,
    legacy_telemetry_client_id,
    legacy_telemetry_profile_group_id,
    {% endif %}
  FROM
    `{{ first_seen_table }}`
  WHERE
    first_seen_date > "2010-01-01"
    AND first_seen_date < @submission_date
)
{% endif %}

, _joined AS (
  --Switch to using separate if statements instead of 1
  --because dry run is struggling to validate the final struct
  SELECT
    IF(
      _previous.client_id IS NULL
      OR _previous.first_seen_date >= _current.first_seen_date,
      _current,
      _previous
    ).*
  FROM
    _current
  FULL JOIN
    _previous
    USING (client_id)
)

-- added this as the result of bug#1788650
SELECT
  submission_date,
  first_seen_date,
  sample_id,
  client_id,
  attribution,
  `distribution`,
  {% if app_name == "firefox_desktop" %}
  attribution_ext,
  distribution_ext,
  legacy_telemetry_client_id,
  legacy_telemetry_profile_group_id,
  {% endif %}
FROM _joined
QUALIFY
  IF(
    COUNT(*) OVER (PARTITION BY client_id) > 1,
    ERROR("duplicate client_id detected"),
    TRUE
  )

{% raw %}
{% endif %}
{% endraw %}
