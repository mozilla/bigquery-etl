WITH base AS (
  SELECT
    user_id,
    country,
    `language`,
    os_name,
    os_version,
    seen_in_tier1_country,
    registered,
    user_service_first_daily_flow_info,
  FROM
    `firefox_accounts_derived.fxa_users_services_daily_v2`
  WHERE
    submission_date = @submission_date
),
bool_fields AS (
  SELECT
    user_id,
    LOGICAL_OR(seen_in_tier1_country) AS seen_in_tier1_country,
    LOGICAL_OR(registered) AS registered,
  FROM
    base
  GROUP BY
    user_id
)
SELECT
  @submission_date AS submission_date,
  * EXCEPT (seen_in_tier1_country, registered, user_service_first_daily_flow_info),
  bool_fields.seen_in_tier1_country,
  bool_fields.registered,
FROM
  base
LEFT JOIN
  bool_fields
USING
  (user_id)
QUALIFY
  ROW_NUMBER() OVER (
    PARTITION BY
      user_id
    ORDER BY
      user_service_first_daily_flow_info.`timestamp` ASC
  ) = 1
