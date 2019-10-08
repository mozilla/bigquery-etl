CREATE TEMP FUNCTION
  udf_mode_last(list ANY TYPE) AS ((
    SELECT
      _value
    FROM
      UNNEST(list) AS _value
    WITH
    OFFSET
      AS
    _offset
    GROUP BY
      _value
    ORDER BY
      COUNT(_value) DESC,
      MAX(_offset) DESC
    LIMIT
      1 ));
--
CREATE TEMP FUNCTION
  udf_contains_tier1_country(x ANY TYPE) AS ( --
    EXISTS(
    SELECT
      country
    FROM
      UNNEST(x) AS country
    WHERE
      country IN ( --
        'United States',
        'France',
        'Germany',
        'United Kingdom',
        'Canada')) );
  --
  -- This UDF is also only applicable in the context of this query.
CREATE TEMP FUNCTION
  udf_contains_registration(x ANY TYPE) AS ( --
    EXISTS(
    SELECT
      event_type
    FROM
      UNNEST(x) AS event_type
    WHERE
      event_type IN ( --
        'fxa_reg - complete')) );
  --
WITH
  base AS (
    SELECT
      * REPLACE(
        -- cert_signed is specific to sync, but these events do not have the
        -- 'service' field populated, so we fill in the service name for this special case.
        IF(service IS NULL AND event_type = 'fxa_activity - cert_signed', 'sync', service) AS service)
    FROM
      `moz-fx-data-shared-prod.telemetry.fxa_content_auth_oauth_events_v1`
    WHERE
      user_id IS NOT NULL
      AND event_type NOT IN ( --
      'fxa_email - bounced',
      'fxa_email - click',
      'fxa_email - sent',
      'fxa_reg - password_blocked',
      'fxa_reg - password_common',
      'fxa_reg - password_enrolled',
      'fxa_reg - password_missing',
      'fxa_sms - sent',
      'mktg - email_click',
      'mktg - email_open',
      'mktg - email_sent',
      'sync - repair_success',
      'sync - repair_triggered')
  ),
  windowed AS (
  SELECT
    DATE(`timestamp`) AS submission_date,
    user_id,
    service,
    ROW_NUMBER() OVER w1_unframed AS _n,
    udf_mode_last(ARRAY_AGG(country) OVER w1) AS country,
    udf_mode_last(ARRAY_AGG(language) OVER w1) AS language,
    udf_mode_last(ARRAY_AGG(app_version) OVER w1) AS app_version,
    udf_mode_last(ARRAY_AGG(os_name) OVER w1) AS os_name,
    udf_mode_last(ARRAY_AGG(os_version) OVER w1) AS os_version,
    udf_contains_tier1_country(ARRAY_AGG(country) OVER w1) AS seen_in_tier1_country,
    udf_contains_registration(ARRAY_AGG(event_type) OVER w1) AS registered
  FROM
    base
  WHERE
    service IS NOT NULL
    -- Reprocess all dates by running this query with --parameter=submission_date:DATE:NULL
    AND (@submission_date IS NULL OR @submission_date = DATE(`timestamp`))
  WINDOW
    w1 AS (
    PARTITION BY
      user_id,
      service,
      DATE(`timestamp`)
    ORDER BY
      `timestamp` --
      ROWS BETWEEN UNBOUNDED PRECEDING
      AND UNBOUNDED FOLLOWING),
    -- We must provide a modified window for ROW_NUMBER which cannot accept a frame clause.
    w1_unframed AS (
    PARTITION BY
      user_id,
      service,
      DATE(`timestamp`)
    ORDER BY
      `timestamp`) )
SELECT
  * EXCEPT (_n)
FROM
  windowed
WHERE
  _n = 1
