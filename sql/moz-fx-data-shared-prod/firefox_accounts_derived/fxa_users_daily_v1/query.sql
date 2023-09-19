-- This UDF is only applicable in the context of this query;
-- telemetry data accepts countries as two-digit codes, but FxA
-- data includes long-form country names. The logic here is specific
-- to the FxA data.
CREATE TEMP FUNCTION udf_contains_tier1_country(x ANY TYPE) AS (
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
        'Canada'
      )
  )
);

-- This UDF is also only applicable in the context of this query.
CREATE TEMP FUNCTION udf_contains_registration(x ANY TYPE) AS (
  EXISTS(
    SELECT
      event_type
    FROM
      UNNEST(x) AS event_type
    WHERE
      event_type IN ( --
        'fxa_reg - complete'
      )
  )
);

WITH windowed AS (
  SELECT
    DATE(`timestamp`) AS submission_date,
    user_id,
    ROW_NUMBER() OVER w1_unframed AS _n,
    udf.mode_last(ARRAY_AGG(country) OVER w1) AS country,
    udf.mode_last(ARRAY_AGG(`language`) OVER w1) AS language,
    udf.mode_last(ARRAY_AGG(app_version) OVER w1) AS app_version,
    udf.mode_last(ARRAY_AGG(os_name) OVER w1) AS os_name,
    udf.mode_last(ARRAY_AGG(os_version) OVER w1) AS os_version,
    udf_contains_tier1_country(ARRAY_AGG(country) OVER w1) AS seen_in_tier1_country,
    udf_contains_registration(ARRAY_AGG(event_type) OVER w1) AS registered,
    COUNTIF(
      NOT (event_type = 'fxa_rp - engage' AND service = 'fx-monitor')
    ) OVER w1 = 0 AS monitor_only
  FROM
    firefox_accounts.fxa_all_events
  WHERE
    fxa_log IN ('auth', 'auth_bounce', 'content', 'oauth')
    AND user_id IS NOT NULL
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
      'sync - repair_triggered'
    )
    -- Reprocess all dates by running this query with --parameter=submission_date:DATE:NULL
    AND (@submission_date IS NULL OR @submission_date = DATE(`timestamp`))
  WINDOW
    w1 AS (
      PARTITION BY
        user_id,
        DATE(`timestamp`)
      ORDER BY
        `timestamp` --
      ROWS BETWEEN
        UNBOUNDED PRECEDING
        AND UNBOUNDED FOLLOWING
    ),
    -- We must provide a modified window for ROW_NUMBER which cannot accept a frame clause.
    w1_unframed AS (
      PARTITION BY
        user_id,
        DATE(`timestamp`)
      ORDER BY
        `timestamp`
    )
)
SELECT
  * EXCEPT (_n)
FROM
  windowed
WHERE
  _n = 1
