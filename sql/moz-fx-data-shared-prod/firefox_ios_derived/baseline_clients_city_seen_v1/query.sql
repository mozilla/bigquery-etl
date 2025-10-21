 -- Query generated via sql_generators.baseline_clients_city_seen.
 -- this mimics the logic used in baseline_clients_daily_v1.
WITH base_org_mozilla_ios_firefox AS (
  -- Live table dedup logic mimics copy_deduplicate
  SELECT
    submission_timestamp,
    DATE(submission_timestamp) AS submission_date,
    LOWER(client_info.client_id) AS client_id,
    sample_id,
    metadata.geo.city AS city,
    metadata.geo.subdivision1 AS subdivision1,
    metadata.geo.subdivision2 AS subdivision2,
    metadata.geo.country AS country
  FROM
    {% if is_init() %}
      `moz-fx-data-shared-prod.org_mozilla_ios_firefox_stable.baseline_v1`
    {% else %}
      `moz-fx-data-shared-prod.org_mozilla_ios_firefox_live.baseline_v1`
    {% endif %}
  WHERE
    client_info.client_id IS NOT NULL
    {% if is_init() %}
      AND sample_id >= @sample_id
      AND sample_id < @sample_id + @sampling_batch_size
      AND DATE(submission_timestamp) <= CURRENT_DATE()
    {% else %}
      AND DATE(submission_timestamp) = @submission_date
      AND 'automation' NOT IN (
        SELECT
          TRIM(t)
        FROM
          UNNEST(SPLIT(metadata.header.x_source_tags, ',')) t
      )
      QUALIFY
        ROW_NUMBER() OVER (PARTITION BY document_id ORDER BY submission_timestamp) = 1
    {% endif %}
),
overactive_org_mozilla_ios_firefox AS (
  -- Find client_ids with over 150 000 pings in a day,
  -- which could cause errors in the next step due to aggregation overflows.
  SELECT
    submission_date,
    client_id
  FROM
    base_org_mozilla_ios_firefox
  WHERE
    {% if is_init() %}
      submission_date >= '2018-01-01'
    {% else %}
      submission_date = @submission_date
    {% endif %}
  GROUP BY
    submission_date,
    client_id
  HAVING
    COUNT(*) > 150000
),
clients_daily_org_mozilla_ios_firefox AS (
  SELECT
    "org_mozilla_ios_firefox" AS app_id,
    submission_date,
    client_id,
    sample_id,
    `moz-fx-data-shared-prod.udf.mode_last`(
      ARRAY_AGG(STRUCT(city, subdivision1, subdivision2, country) ORDER BY submission_timestamp)
    ) AS geo
  FROM
    base_org_mozilla_ios_firefox
  LEFT JOIN
    overactive_org_mozilla_ios_firefox
    USING (submission_date, client_id)
  WHERE
    overactive_org_mozilla_ios_firefox.client_id IS NULL
    -- `mode_last` can result in struct with all null values if it’s most frequent (or latest among ties).
    -- This exclude structs with all null values so there will always be one non-NULL field.
    AND COALESCE(city, subdivision1, subdivision2, country) IS NOT NULL
    {% if is_init() %}
      AND submission_date >= '2018-01-01'
    {% else %}
      AND submission_date = @submission_date
    {% endif %}
  GROUP BY
    submission_date,
    client_id,
    sample_id
),
{% if is_init() %}
  clients_city_first_seen_org_mozilla_ios_firefox AS (
    SELECT
      app_id,
      client_id,
      sample_id,
      submission_date AS first_seen_city_date,
      geo.city AS first_seen_city,
      geo.subdivision1 AS first_seen_subdivision1,
      geo.subdivision2 AS first_seen_subdivision2,
      geo.country AS first_seen_country
    FROM
      clients_daily_org_mozilla_ios_firefox
    WHERE
      geo.city IS NOT NULL
    QUALIFY
      ROW_NUMBER() OVER (PARTITION BY client_id, sample_id ORDER BY submission_date) = 1
  ),
  clients_city_last_seen_org_mozilla_ios_firefox AS (
    SELECT
      app_id,
      client_id,
      sample_id,
      submission_date AS last_seen_city_date,
      geo.city AS last_seen_city,
      geo.subdivision1 AS last_seen_subdivision1,
      geo.subdivision2 AS last_seen_subdivision2,
      geo.country AS last_seen_country
    FROM
      clients_daily_org_mozilla_ios_firefox
    WHERE
      geo.city IS NOT NULL
    QUALIFY
      ROW_NUMBER() OVER (PARTITION BY client_id, sample_id ORDER BY submission_date DESC) = 1
  ),
{% else %}
  _previous_org_mozilla_ios_firefox AS (
    SELECT
      *
    FROM
      `moz-fx-data-shared-prod.firefox_ios_derived.baseline_clients_city_seen_v1`
    WHERE
      app_id = "org_mozilla_ios_firefox"
  ),
  _current_org_mozilla_ios_firefox AS (
    SELECT
      app_id,
      client_id,
      sample_id,
      submission_date AS first_seen_city_date,
      geo.city AS first_seen_city,
      geo.subdivision1 AS first_seen_subdivision1,
      geo.subdivision2 AS first_seen_subdivision2,
      geo.country AS first_seen_country,
      submission_date AS last_seen_city_date,
      geo.city AS last_seen_city,
      geo.subdivision1 AS last_seen_subdivision1,
      geo.subdivision2 AS last_seen_subdivision2,
      geo.country AS last_seen_country
    FROM
      clients_daily_org_mozilla_ios_firefox
    WHERE
      geo.city IS NOT NULL
  ),
{% endif %}
base_org_mozilla_ios_firefoxbeta AS (
  -- Live table dedup logic mimics copy_deduplicate
  SELECT
    submission_timestamp,
    DATE(submission_timestamp) AS submission_date,
    LOWER(client_info.client_id) AS client_id,
    sample_id,
    metadata.geo.city AS city,
    metadata.geo.subdivision1 AS subdivision1,
    metadata.geo.subdivision2 AS subdivision2,
    metadata.geo.country AS country
  FROM
    {% if is_init() %}
      `moz-fx-data-shared-prod.org_mozilla_ios_firefoxbeta_stable.baseline_v1`
    {% else %}
      `moz-fx-data-shared-prod.org_mozilla_ios_firefoxbeta_live.baseline_v1`
    {% endif %}
  WHERE
    client_info.client_id IS NOT NULL
    {% if is_init() %}
      AND sample_id >= @sample_id
      AND sample_id < @sample_id + @sampling_batch_size
      AND DATE(submission_timestamp) <= CURRENT_DATE()
    {% else %}
      AND DATE(submission_timestamp) = @submission_date
      AND 'automation' NOT IN (
        SELECT
          TRIM(t)
        FROM
          UNNEST(SPLIT(metadata.header.x_source_tags, ',')) t
      )
      QUALIFY
        ROW_NUMBER() OVER (PARTITION BY document_id ORDER BY submission_timestamp) = 1
    {% endif %}
),
overactive_org_mozilla_ios_firefoxbeta AS (
  -- Find client_ids with over 150 000 pings in a day,
  -- which could cause errors in the next step due to aggregation overflows.
  SELECT
    submission_date,
    client_id
  FROM
    base_org_mozilla_ios_firefoxbeta
  WHERE
    {% if is_init() %}
      submission_date >= '2018-01-01'
    {% else %}
      submission_date = @submission_date
    {% endif %}
  GROUP BY
    submission_date,
    client_id
  HAVING
    COUNT(*) > 150000
),
clients_daily_org_mozilla_ios_firefoxbeta AS (
  SELECT
    "org_mozilla_ios_firefoxbeta" AS app_id,
    submission_date,
    client_id,
    sample_id,
    `moz-fx-data-shared-prod.udf.mode_last`(
      ARRAY_AGG(STRUCT(city, subdivision1, subdivision2, country) ORDER BY submission_timestamp)
    ) AS geo
  FROM
    base_org_mozilla_ios_firefoxbeta
  LEFT JOIN
    overactive_org_mozilla_ios_firefoxbeta
    USING (submission_date, client_id)
  WHERE
    overactive_org_mozilla_ios_firefoxbeta.client_id IS NULL
    -- `mode_last` can result in struct with all null values if it’s most frequent (or latest among ties).
    -- This exclude structs with all null values so there will always be one non-NULL field.
    AND COALESCE(city, subdivision1, subdivision2, country) IS NOT NULL
    {% if is_init() %}
      AND submission_date >= '2018-01-01'
    {% else %}
      AND submission_date = @submission_date
    {% endif %}
  GROUP BY
    submission_date,
    client_id,
    sample_id
),
{% if is_init() %}
  clients_city_first_seen_org_mozilla_ios_firefoxbeta AS (
    SELECT
      app_id,
      client_id,
      sample_id,
      submission_date AS first_seen_city_date,
      geo.city AS first_seen_city,
      geo.subdivision1 AS first_seen_subdivision1,
      geo.subdivision2 AS first_seen_subdivision2,
      geo.country AS first_seen_country
    FROM
      clients_daily_org_mozilla_ios_firefoxbeta
    WHERE
      geo.city IS NOT NULL
    QUALIFY
      ROW_NUMBER() OVER (PARTITION BY client_id, sample_id ORDER BY submission_date) = 1
  ),
  clients_city_last_seen_org_mozilla_ios_firefoxbeta AS (
    SELECT
      app_id,
      client_id,
      sample_id,
      submission_date AS last_seen_city_date,
      geo.city AS last_seen_city,
      geo.subdivision1 AS last_seen_subdivision1,
      geo.subdivision2 AS last_seen_subdivision2,
      geo.country AS last_seen_country
    FROM
      clients_daily_org_mozilla_ios_firefoxbeta
    WHERE
      geo.city IS NOT NULL
    QUALIFY
      ROW_NUMBER() OVER (PARTITION BY client_id, sample_id ORDER BY submission_date DESC) = 1
  ),
{% else %}
  _previous_org_mozilla_ios_firefoxbeta AS (
    SELECT
      *
    FROM
      `moz-fx-data-shared-prod.firefox_ios_derived.baseline_clients_city_seen_v1`
    WHERE
      app_id = "org_mozilla_ios_firefoxbeta"
  ),
  _current_org_mozilla_ios_firefoxbeta AS (
    SELECT
      app_id,
      client_id,
      sample_id,
      submission_date AS first_seen_city_date,
      geo.city AS first_seen_city,
      geo.subdivision1 AS first_seen_subdivision1,
      geo.subdivision2 AS first_seen_subdivision2,
      geo.country AS first_seen_country,
      submission_date AS last_seen_city_date,
      geo.city AS last_seen_city,
      geo.subdivision1 AS last_seen_subdivision1,
      geo.subdivision2 AS last_seen_subdivision2,
      geo.country AS last_seen_country
    FROM
      clients_daily_org_mozilla_ios_firefoxbeta
    WHERE
      geo.city IS NOT NULL
  ),
{% endif %}
base_org_mozilla_ios_fennec AS (
  -- Live table dedup logic mimics copy_deduplicate
  SELECT
    submission_timestamp,
    DATE(submission_timestamp) AS submission_date,
    LOWER(client_info.client_id) AS client_id,
    sample_id,
    metadata.geo.city AS city,
    metadata.geo.subdivision1 AS subdivision1,
    metadata.geo.subdivision2 AS subdivision2,
    metadata.geo.country AS country
  FROM
    {% if is_init() %}
      `moz-fx-data-shared-prod.org_mozilla_ios_fennec_stable.baseline_v1`
    {% else %}
      `moz-fx-data-shared-prod.org_mozilla_ios_fennec_live.baseline_v1`
    {% endif %}
  WHERE
    client_info.client_id IS NOT NULL
    {% if is_init() %}
      AND sample_id >= @sample_id
      AND sample_id < @sample_id + @sampling_batch_size
      AND DATE(submission_timestamp) <= CURRENT_DATE()
    {% else %}
      AND DATE(submission_timestamp) = @submission_date
      AND 'automation' NOT IN (
        SELECT
          TRIM(t)
        FROM
          UNNEST(SPLIT(metadata.header.x_source_tags, ',')) t
      )
      QUALIFY
        ROW_NUMBER() OVER (PARTITION BY document_id ORDER BY submission_timestamp) = 1
    {% endif %}
),
overactive_org_mozilla_ios_fennec AS (
  -- Find client_ids with over 150 000 pings in a day,
  -- which could cause errors in the next step due to aggregation overflows.
  SELECT
    submission_date,
    client_id
  FROM
    base_org_mozilla_ios_fennec
  WHERE
    {% if is_init() %}
      submission_date >= '2018-01-01'
    {% else %}
      submission_date = @submission_date
    {% endif %}
  GROUP BY
    submission_date,
    client_id
  HAVING
    COUNT(*) > 150000
),
clients_daily_org_mozilla_ios_fennec AS (
  SELECT
    "org_mozilla_ios_fennec" AS app_id,
    submission_date,
    client_id,
    sample_id,
    `moz-fx-data-shared-prod.udf.mode_last`(
      ARRAY_AGG(STRUCT(city, subdivision1, subdivision2, country) ORDER BY submission_timestamp)
    ) AS geo
  FROM
    base_org_mozilla_ios_fennec
  LEFT JOIN
    overactive_org_mozilla_ios_fennec
    USING (submission_date, client_id)
  WHERE
    overactive_org_mozilla_ios_fennec.client_id IS NULL
    -- `mode_last` can result in struct with all null values if it’s most frequent (or latest among ties).
    -- This exclude structs with all null values so there will always be one non-NULL field.
    AND COALESCE(city, subdivision1, subdivision2, country) IS NOT NULL
    {% if is_init() %}
      AND submission_date >= '2018-01-01'
    {% else %}
      AND submission_date = @submission_date
    {% endif %}
  GROUP BY
    submission_date,
    client_id,
    sample_id
),
{% if is_init() %}
  clients_city_first_seen_org_mozilla_ios_fennec AS (
    SELECT
      app_id,
      client_id,
      sample_id,
      submission_date AS first_seen_city_date,
      geo.city AS first_seen_city,
      geo.subdivision1 AS first_seen_subdivision1,
      geo.subdivision2 AS first_seen_subdivision2,
      geo.country AS first_seen_country
    FROM
      clients_daily_org_mozilla_ios_fennec
    WHERE
      geo.city IS NOT NULL
    QUALIFY
      ROW_NUMBER() OVER (PARTITION BY client_id, sample_id ORDER BY submission_date) = 1
  ),
  clients_city_last_seen_org_mozilla_ios_fennec AS (
    SELECT
      app_id,
      client_id,
      sample_id,
      submission_date AS last_seen_city_date,
      geo.city AS last_seen_city,
      geo.subdivision1 AS last_seen_subdivision1,
      geo.subdivision2 AS last_seen_subdivision2,
      geo.country AS last_seen_country
    FROM
      clients_daily_org_mozilla_ios_fennec
    WHERE
      geo.city IS NOT NULL
    QUALIFY
      ROW_NUMBER() OVER (PARTITION BY client_id, sample_id ORDER BY submission_date DESC) = 1
  )
{% else %}
  _previous_org_mozilla_ios_fennec AS (
    SELECT
      *
    FROM
      `moz-fx-data-shared-prod.firefox_ios_derived.baseline_clients_city_seen_v1`
    WHERE
      app_id = "org_mozilla_ios_fennec"
  ),
  _current_org_mozilla_ios_fennec AS (
    SELECT
      app_id,
      client_id,
      sample_id,
      submission_date AS first_seen_city_date,
      geo.city AS first_seen_city,
      geo.subdivision1 AS first_seen_subdivision1,
      geo.subdivision2 AS first_seen_subdivision2,
      geo.country AS first_seen_country,
      submission_date AS last_seen_city_date,
      geo.city AS last_seen_city,
      geo.subdivision1 AS last_seen_subdivision1,
      geo.subdivision2 AS last_seen_subdivision2,
      geo.country AS last_seen_country
    FROM
      clients_daily_org_mozilla_ios_fennec
    WHERE
      geo.city IS NOT NULL
  )
{% endif %}
{% if is_init() %}
  SELECT
    app_id,
    client_id,
    sample_id,
    first_seen_city_date,
    first_seen_city,
    first_seen_subdivision1,
    first_seen_subdivision2,
    first_seen_country,
    last_seen_city_date,
    last_seen_city,
    last_seen_subdivision1,
    last_seen_subdivision2,
    last_seen_country
  FROM
    clients_city_first_seen_org_mozilla_ios_firefox cfs
  FULL OUTER JOIN
    clients_city_last_seen_org_mozilla_ios_firefox cls
    USING (client_id, sample_id, app_id)
  UNION ALL
  SELECT
    app_id,
    client_id,
    sample_id,
    first_seen_city_date,
    first_seen_city,
    first_seen_subdivision1,
    first_seen_subdivision2,
    first_seen_country,
    last_seen_city_date,
    last_seen_city,
    last_seen_subdivision1,
    last_seen_subdivision2,
    last_seen_country
  FROM
    clients_city_first_seen_org_mozilla_ios_firefoxbeta cfs
  FULL OUTER JOIN
    clients_city_last_seen_org_mozilla_ios_firefoxbeta cls
    USING (client_id, sample_id, app_id)
  UNION ALL
  SELECT
    app_id,
    client_id,
    sample_id,
    first_seen_city_date,
    first_seen_city,
    first_seen_subdivision1,
    first_seen_subdivision2,
    first_seen_country,
    last_seen_city_date,
    last_seen_city,
    last_seen_subdivision1,
    last_seen_subdivision2,
    last_seen_country
  FROM
    clients_city_first_seen_org_mozilla_ios_fennec cfs
  FULL OUTER JOIN
    clients_city_last_seen_org_mozilla_ios_fennec cls
    USING (client_id, sample_id, app_id)
{% else %}
  SELECT
-- _p.* fields are NULL for clients that are not yet captured in the baseline_city_seen derived table.
    IF(_p.app_id IS NULL, _c.app_id, _p.app_id) AS app_id,
    IF(_p.client_id IS NULL, _c.client_id, _p.client_id) AS client_id,
    IF(_p.sample_id IS NULL, _c.sample_id, _p.sample_id) AS sample_id,
    IF(
      _p.client_id IS NULL,
      _c.first_seen_city_date,
      _p.first_seen_city_date
    ) AS first_seen_city_date,
    IF(_p.client_id IS NULL, _c.first_seen_city, _p.first_seen_city) AS first_seen_city,
    IF(
      _p.client_id IS NULL,
      _c.first_seen_subdivision1,
      _p.first_seen_subdivision1
    ) AS first_seen_subdivision1,
    IF(
      _p.client_id IS NULL,
      _c.first_seen_subdivision2,
      _p.first_seen_subdivision2
    ) AS first_seen_subdivision2,
    IF(_p.client_id IS NULL, _c.first_seen_country, _p.first_seen_country) AS first_seen_country,
    IF(
      _p.client_id IS NULL
      OR _p.last_seen_city_date < _c.last_seen_city_date,
      _c.last_seen_city_date,
      _p.last_seen_city_date
    ) AS last_seen_city_date,
    IF(
      _p.client_id IS NULL
      OR _p.last_seen_city_date < _c.last_seen_city_date,
      _c.last_seen_city,
      _p.last_seen_city
    ) AS last_seen_city,
    IF(
      _p.client_id IS NULL
      OR _p.last_seen_city_date < _c.last_seen_city_date,
      _c.last_seen_subdivision1,
      _p.last_seen_subdivision1
    ) AS last_seen_subdivision1,
    IF(
      _p.client_id IS NULL
      OR _p.last_seen_city_date < _c.last_seen_city_date,
      _c.last_seen_subdivision2,
      _p.last_seen_subdivision2
    ) AS last_seen_subdivision2,
    IF(
      _p.client_id IS NULL
      OR _p.last_seen_city_date < _c.last_seen_city_date,
      _c.last_seen_country,
      _p.last_seen_country
    ) AS last_seen_country
  FROM
    _current_org_mozilla_ios_firefox AS _c
  FULL JOIN
    _previous_org_mozilla_ios_firefox AS _p
    USING (client_id, sample_id, app_id)
  UNION ALL
  SELECT
-- _p.* fields are NULL for clients that are not yet captured in the baseline_city_seen derived table.
    IF(_p.app_id IS NULL, _c.app_id, _p.app_id) AS app_id,
    IF(_p.client_id IS NULL, _c.client_id, _p.client_id) AS client_id,
    IF(_p.sample_id IS NULL, _c.sample_id, _p.sample_id) AS sample_id,
    IF(
      _p.client_id IS NULL,
      _c.first_seen_city_date,
      _p.first_seen_city_date
    ) AS first_seen_city_date,
    IF(_p.client_id IS NULL, _c.first_seen_city, _p.first_seen_city) AS first_seen_city,
    IF(
      _p.client_id IS NULL,
      _c.first_seen_subdivision1,
      _p.first_seen_subdivision1
    ) AS first_seen_subdivision1,
    IF(
      _p.client_id IS NULL,
      _c.first_seen_subdivision2,
      _p.first_seen_subdivision2
    ) AS first_seen_subdivision2,
    IF(_p.client_id IS NULL, _c.first_seen_country, _p.first_seen_country) AS first_seen_country,
    IF(
      _p.client_id IS NULL
      OR _p.last_seen_city_date < _c.last_seen_city_date,
      _c.last_seen_city_date,
      _p.last_seen_city_date
    ) AS last_seen_city_date,
    IF(
      _p.client_id IS NULL
      OR _p.last_seen_city_date < _c.last_seen_city_date,
      _c.last_seen_city,
      _p.last_seen_city
    ) AS last_seen_city,
    IF(
      _p.client_id IS NULL
      OR _p.last_seen_city_date < _c.last_seen_city_date,
      _c.last_seen_subdivision1,
      _p.last_seen_subdivision1
    ) AS last_seen_subdivision1,
    IF(
      _p.client_id IS NULL
      OR _p.last_seen_city_date < _c.last_seen_city_date,
      _c.last_seen_subdivision2,
      _p.last_seen_subdivision2
    ) AS last_seen_subdivision2,
    IF(
      _p.client_id IS NULL
      OR _p.last_seen_city_date < _c.last_seen_city_date,
      _c.last_seen_country,
      _p.last_seen_country
    ) AS last_seen_country
  FROM
    _current_org_mozilla_ios_firefoxbeta AS _c
  FULL JOIN
    _previous_org_mozilla_ios_firefoxbeta AS _p
    USING (client_id, sample_id, app_id)
  UNION ALL
  SELECT
-- _p.* fields are NULL for clients that are not yet captured in the baseline_city_seen derived table.
    IF(_p.app_id IS NULL, _c.app_id, _p.app_id) AS app_id,
    IF(_p.client_id IS NULL, _c.client_id, _p.client_id) AS client_id,
    IF(_p.sample_id IS NULL, _c.sample_id, _p.sample_id) AS sample_id,
    IF(
      _p.client_id IS NULL,
      _c.first_seen_city_date,
      _p.first_seen_city_date
    ) AS first_seen_city_date,
    IF(_p.client_id IS NULL, _c.first_seen_city, _p.first_seen_city) AS first_seen_city,
    IF(
      _p.client_id IS NULL,
      _c.first_seen_subdivision1,
      _p.first_seen_subdivision1
    ) AS first_seen_subdivision1,
    IF(
      _p.client_id IS NULL,
      _c.first_seen_subdivision2,
      _p.first_seen_subdivision2
    ) AS first_seen_subdivision2,
    IF(_p.client_id IS NULL, _c.first_seen_country, _p.first_seen_country) AS first_seen_country,
    IF(
      _p.client_id IS NULL
      OR _p.last_seen_city_date < _c.last_seen_city_date,
      _c.last_seen_city_date,
      _p.last_seen_city_date
    ) AS last_seen_city_date,
    IF(
      _p.client_id IS NULL
      OR _p.last_seen_city_date < _c.last_seen_city_date,
      _c.last_seen_city,
      _p.last_seen_city
    ) AS last_seen_city,
    IF(
      _p.client_id IS NULL
      OR _p.last_seen_city_date < _c.last_seen_city_date,
      _c.last_seen_subdivision1,
      _p.last_seen_subdivision1
    ) AS last_seen_subdivision1,
    IF(
      _p.client_id IS NULL
      OR _p.last_seen_city_date < _c.last_seen_city_date,
      _c.last_seen_subdivision2,
      _p.last_seen_subdivision2
    ) AS last_seen_subdivision2,
    IF(
      _p.client_id IS NULL
      OR _p.last_seen_city_date < _c.last_seen_city_date,
      _c.last_seen_country,
      _p.last_seen_country
    ) AS last_seen_country
  FROM
    _current_org_mozilla_ios_fennec AS _c
  FULL JOIN
    _previous_org_mozilla_ios_fennec AS _p
    USING (client_id, sample_id, app_id)
{% endif %}
