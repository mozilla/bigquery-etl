CREATE OR REPLACE TABLE
  `moz-fx-data-shared-prod.firefox_accounts_derived.fxa_users_services_first_seen_v1`
PARTITION BY
  DATE(first_service_timestamp)
CLUSTER BY
  service,
  user_id
AS
WITH fxa_content_auth_oauth AS (
  SELECT
    `timestamp`,
    user_id,
    IF(service IS NULL AND event_type = 'fxa_activity - cert_signed', 'sync', service) AS service,
    os_name,
    flow_id,
    event_type,
    country,
    entrypoint,
  FROM
    `moz-fx-data-shared-prod.firefox_accounts.fxa_all_events`
  WHERE
    fxa_log IN ('content', 'auth', 'oauth')
),
  -- use a window function to look within each USER and SERVICE for the first value of service, os, and country.
  -- also, get the first value of flow_id for later use and create a boolean column that is true if the first instance of a service usage includes a registration.
  -- [kimmy] the variable first_service_timestamp_last is named so because it is actually the last timestamp recorder in the user's first flow,
  -- NOT the first timestamp in their first flow.
  -- it's used later on to order by service, so i'm keeping it here and just renaming it.
first_services AS (
  SELECT
    ROW_NUMBER() OVER w1_unframed AS _n,
    user_id,
    service,
      -- using mode_last with w1_reversed to get mode_first
    udf.mode_last(ARRAY_AGG(`timestamp`) OVER w1_reversed) AS first_service_timestamp_last,
    udf.mode_last(ARRAY_AGG(flow_id) OVER w1_reversed) AS first_service_flow,
    LOGICAL_OR(IFNULL(event_type = 'fxa_reg - complete', FALSE)) OVER w1_reversed AS did_register
  FROM
    fxa_content_auth_oauth
  WHERE
    (
      (event_type IN ('fxa_login - complete', 'fxa_reg - complete') AND service IS NOT NULL)
      OR (event_type LIKE r'fxa\_activity%')
    )
    AND DATE(`timestamp`) >= '2019-03-01'
    AND user_id IS NOT NULL
  WINDOW
    -- We must provide a window with `ORDER BY timestamp DESC` so that udf.mode_last actually aggregates mode first.
    w1_reversed AS (
      PARTITION BY
        user_id,
        service
      ORDER BY
        `timestamp` DESC
      ROWS BETWEEN
        UNBOUNDED PRECEDING
        AND UNBOUNDED FOLLOWING
    ),
    -- We must provide a modified window for ROW_NUMBER which cannot accept a frame clause.
    w1_unframed AS (
      PARTITION BY
        user_id,
        service
      ORDER BY
        `timestamp`
    )
),
  -- we need this next section because `did_register` will be BOTH true and false within the flows that the user registered on.
  -- this dedupes the rows from above and sets did_register to true only on flows that included a registration
  -- I've verified that `date(first_service_timestamp), count(distinct user_id) where did_register = true group by 1`  matches the counts of registrations per day in amplitude.
first_services_g AS (
  SELECT
    * EXCEPT (_n)
  FROM
    first_services
  WHERE
    _n = 1
),
  -- sadly, `entrypoint` is null on registration complete and login complete events.
  -- this means we have to use first_service_flow to join back on the original source table's flow_id,
  -- and take the first occurrence of `entrypoint` within the flow that the user first appeared in the service on.
flows AS (
  SELECT DISTINCT
    s.first_service_flow,
    FIRST_VALUE(f.entrypoint) OVER (
      PARTITION BY
        f.flow_id
      ORDER BY
        f.`timestamp`
      ROWS BETWEEN
        UNBOUNDED PRECEDING
        AND UNBOUNDED FOLLOWING
    ) AS first_service_entrypoint,
    FIRST_VALUE(f.timestamp) OVER (
      PARTITION BY
        f.flow_id
      ORDER BY
        f.`timestamp`
      ROWS BETWEEN
        UNBOUNDED PRECEDING
        AND UNBOUNDED FOLLOWING
    ) AS first_service_timestamp,
    FIRST_VALUE(f.country) OVER (
      PARTITION BY
        f.flow_id
      ORDER BY
        f.`timestamp`
      ROWS BETWEEN
        UNBOUNDED PRECEDING
        AND UNBOUNDED FOLLOWING
    ) AS first_service_country,
    FIRST_VALUE(f.os_name) OVER (
      PARTITION BY
        f.flow_id
      ORDER BY
        f.`timestamp`
      ROWS BETWEEN
        UNBOUNDED PRECEDING
        AND UNBOUNDED FOLLOWING
    ) AS first_service_os
  FROM
    first_services_g s
  INNER JOIN
    fxa_content_auth_oauth AS f
    ON s.first_service_flow = f.flow_id
  WHERE
    f.entrypoint IS NOT NULL
    AND s.first_service_flow IS NOT NULL
    AND DATE(f.`timestamp`) >= '2019-03-01'
)
  -- finally take the entrypoint data and join it back on the other information (os, country etc).
  -- also, add a row number that indicates the order in which the user signed up for their services.
SELECT
  s.user_id,
  s.service,
  s.first_service_flow,
  s.did_register,
  f.first_service_entrypoint AS entrypoint,
  f.first_service_timestamp,
  f.first_service_country,
  f.first_service_os,
  ROW_NUMBER() OVER (PARTITION BY s.user_id ORDER BY first_service_timestamp_last) AS service_number
FROM
  first_services_g s
LEFT JOIN
  flows f
  USING (first_service_flow)
WHERE
  first_service_flow IS NOT NULL
