{% if is_init() %}
INSERT INTO
 `{project_id}.{dataset_id}.{table_id}`
{% endif %}
CREATE TEMP FUNCTION synthesize_customer(
  customer ANY TYPE,
  effective_at TIMESTAMP,
  is_deleted BOOL
) AS (
  -- Generate a synthetic customer record from an existing customer record
  -- to simulate how the customer was at the specified `effective_at` timestamp.
  (
    SELECT AS STRUCT
      customer.* REPLACE (
        IF(customer.discount.start <= effective_at, customer.discount, NULL) AS discount,
        is_deleted AS is_deleted,
        (
          SELECT AS STRUCT
            customer.metadata.* REPLACE (
              IF(
                customer.metadata.geoip_date <= effective_at,
                customer.metadata.geoip_date,
                NULL
              ) AS geoip_date
            )
        ) AS metadata
      )
  )
);

WITH original_changelog AS (
  SELECT
    id,
    `timestamp`,
    (
      SELECT AS STRUCT
        customer.* REPLACE (
          STRUCT(
            PARSE_TIMESTAMP(
              '%a %b %d %Y %X GMT%z (Coordinated Universal Time)',
              JSON_VALUE(customer.metadata.geoip_date)
            ) AS geoip_date,
            JSON_VALUE(customer.metadata.paypalAgreementId) AS paypalAgreementId,
            JSON_VALUE(customer.metadata.userid) AS userid,
            TO_HEX(SHA256(JSON_VALUE(customer.metadata.userid))) AS userid_sha256
          ) AS metadata,
          -- Limit address data to just country since the metadata includes FxA user IDs
          -- and this is in a Mozilla-confidential dataset.
          STRUCT(customer.address.country) AS address,
          (
            SELECT AS STRUCT
              customer.shipping.* REPLACE (STRUCT(customer.shipping.address.country) AS address)
          ) AS shipping
        )
    ) AS customer,
    ROW_NUMBER() OVER customer_changes_asc AS customer_change_number
  FROM
    `moz-fx-data-shared-prod`.stripe_external.customers_changelog_v1
  WINDOW
    customer_changes_asc AS (
      PARTITION BY
        customer.id
      ORDER BY
        `timestamp`
    )
),
pre_fivetran_changelog AS (
  -- Construct changelog records for customers deleted before the initial Fivetran Stripe sync,
  -- which were archived on 2022-03-25.
  SELECT
    CAST(NULL AS STRING) AS id,
    TIMESTAMP '2022-03-25 00:02:29' AS `timestamp`,
    STRUCT(
      id,
      STRUCT(address.country) AS address,
      created,
      CAST(NULL AS STRING) AS default_source_id,
      CAST(
        NULL
        AS
          STRUCT<
            id STRING,
            coupon STRUCT<
              id STRING,
              amount_off INTEGER,
              created TIMESTAMP,
              currency STRING,
              duration STRING,
              duration_in_months INTEGER,
              metadata JSON,
              name STRING,
              percent_off FLOAT64,
              redeem_by TIMESTAMP
            >,
            `end` TIMESTAMP,
            invoice_id STRING,
            invoice_item_id STRING,
            promotion_code_id STRING,
            start TIMESTAMP,
            subscription_id STRING
          >
      ) AS discount,
      TRUE AS is_deleted,
      STRUCT(
        CAST(NULL AS TIMESTAMP) AS geoip_date,
        JSON_VALUE(metadata.paypalAgreementId) AS paypalAgreementId,
        CAST(NULL AS STRING) AS userid,
        JSON_VALUE(metadata.userid_sha256) AS userid_sha256
      ) AS metadata,
      CAST(NULL AS STRUCT<address STRUCT<country STRING>>) AS shipping,
      CAST(NULL AS STRING) AS tax_exempt
    ) AS customer,
    1 AS customer_change_number
  FROM
    `moz-fx-data-shared-prod`.stripe_external.pre_fivetran_customers_v2
),
customer_subscription_dates AS (
  SELECT
    subscription.customer_id,
    MIN(subscription.created) AS first_subscription_created,
    MAX(subscription.ended_at) AS last_subscription_ended_at
  FROM
    `moz-fx-data-shared-prod`.stripe_external.subscriptions_changelog_v1
  GROUP BY
    customer_id
),
augmented_original_changelog AS (
  SELECT
    'original' AS type,
    changelog.* REPLACE (
      (
        SELECT AS STRUCT
          changelog.customer.* REPLACE (
            -- Fivetran wasn't always configured to sync the `customer.created` column, so for
            -- customers without `created` values we use their first subscription's `created` value.
            COALESCE(
              changelog.customer.created,
              customer_subscription_dates.first_subscription_created
            ) AS created
          )
      ) AS customer
    )
  FROM
    original_changelog AS changelog
  LEFT JOIN
    customer_subscription_dates
  ON
    changelog.customer.id = customer_subscription_dates.customer_id
  UNION ALL
  SELECT
    'pre_fivetran' AS type,
    *
  FROM
    pre_fivetran_changelog
),
adjusted_original_changelog AS (
  SELECT
    id AS original_id,
    (
      CASE
        -- Backdate the initial timestamp for new active customers if it's within a day of the sync.
        -- Otherwise we'll create a separate synthetic customer creation changelog for them.
        WHEN customer_change_number = 1
          AND customer.is_deleted IS NOT TRUE
          AND TIMESTAMP_DIFF(`timestamp`, customer.created, HOUR) < 24
          THEN STRUCT(customer.created AS `timestamp`, 'adjusted_customer_creation' AS type)
        ELSE STRUCT(`timestamp`, type)
      END
    ).*,
    * EXCEPT (id, `timestamp`, type)
  FROM
    augmented_original_changelog
),
synthetic_customer_creation_changelog AS (
  -- Synthesize customer creation changelogs when the customer was already deleted
  -- or the initial timestamp wasn't within a day of the sync.
  SELECT
    'synthetic_customer_creation' AS type,
    id AS original_id,
    customer.created AS `timestamp`,
    synthesize_customer(customer, effective_at => customer.created, is_deleted => FALSE) AS customer
  FROM
    augmented_original_changelog
  WHERE
    customer_change_number = 1
    AND (customer.is_deleted OR TIMESTAMP_DIFF(`timestamp`, customer.created, HOUR) >= 24)
    AND customer.created IS NOT NULL
),
-- `stripe_external.customers_changelog_v1` began capturing incremental customer changes on 2023-07-10,
-- but the underlying Fivetran `customer` table doesn't preserve historical changes, so the initial
-- changelog records reflected the current state of the customers at the time that ETL first ran.
-- Also, customers deleted before the initial Fivetran Stripe sync were archived as is on 2022-03-25.
-- As a result, we have to synthesize records to more accurately reconstruct history.
questionable_initial_changelog AS (
  SELECT
    *
  FROM
    adjusted_original_changelog
  WHERE
    DATE(`timestamp`) <= '2023-07-10'
    AND customer_change_number = 1
),
synthetic_discount_start_changelog AS (
  SELECT
    'synthetic_discount_start' AS type,
    original_id,
    customer.discount.start AS `timestamp`,
    synthesize_customer(
      customer,
      effective_at => customer.discount.start,
      is_deleted => FALSE
    ) AS customer
  FROM
    questionable_initial_changelog
  WHERE
    customer.discount.start > customer.created
),
synthetic_geolocation_changelog AS (
  SELECT
    'synthetic_geolocation' AS type,
    original_id,
    customer.metadata.geoip_date AS `timestamp`,
    synthesize_customer(
      customer,
      effective_at => customer.metadata.geoip_date,
      is_deleted => FALSE
    ) AS customer
  FROM
    questionable_initial_changelog
  WHERE
    customer.metadata.geoip_date > customer.created
),
synthetic_customer_deletion_changelog AS (
  -- For customers that were already deleted when the changelog started capturing changes we can't know
  -- when they were actually deleted, so we use when their last subscription ended as an approximation.
  SELECT
    'synthetic_customer_deletion' AS type,
    changelog.original_id,
    customer_subscription_dates.last_subscription_ended_at AS `timestamp`,
    synthesize_customer(
      changelog.customer,
      effective_at => customer_subscription_dates.last_subscription_ended_at,
      is_deleted => TRUE
    ) AS customer
  FROM
    questionable_initial_changelog AS changelog
  LEFT JOIN
    customer_subscription_dates
  ON
    changelog.customer.id = customer_subscription_dates.customer_id
  WHERE
    changelog.customer.is_deleted
    AND customer_subscription_dates.last_subscription_ended_at IS NOT NULL
),
synthetic_changelog_union AS (
  SELECT
    *
  FROM
    synthetic_customer_creation_changelog
  UNION ALL
  SELECT
    *
  FROM
    synthetic_discount_start_changelog
  UNION ALL
  SELECT
    *
  FROM
    synthetic_geolocation_changelog
  UNION ALL
  SELECT
    *
  FROM
    synthetic_customer_deletion_changelog
),
changelog_union AS (
  SELECT
    type,
    original_id,
    `timestamp`,
    customer
  FROM
    adjusted_original_changelog
  UNION ALL
  SELECT
    type,
    original_id,
    `timestamp`,
    customer
  FROM
    synthetic_changelog_union
)
SELECT
  CONCAT(customer.id, '-', FORMAT_TIMESTAMP('%FT%H:%M:%E6S', `timestamp`)) AS id,
  `timestamp`,
  type,
  original_id AS stripe_customers_changelog_id,
  customer
FROM
  changelog_union
WHERE
  {% if is_init() %}
    DATE(`timestamp`) < CURRENT_DATE()
  {% else %}
    DATE(`timestamp`) = @date
  {% endif %}
