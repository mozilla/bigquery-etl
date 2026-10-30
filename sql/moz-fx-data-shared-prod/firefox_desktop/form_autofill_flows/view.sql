-- One row per client, day and form autofill interaction ("flow") on Firefox
-- Desktop, for address and credit card forms. Always filter on submission_date.
--
-- The field-level definitions follow the metric-hub data source
-- `form_autofill_forms` (mozilla/metric-hub#1668), so detected_fields,
-- autofilled_fields, modified_fields and net_kept_fields match the Jetstream
-- metrics.
--
-- This is a view and not a table because it has about 300M rows per day. It
-- has no joins, so that a submission_date filter prunes the events_stream
-- partitions.
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_desktop.form_autofill_flows`
AS
WITH autofill_events AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_id,
    legacy_telemetry_client_id,
    profile_group_id,
    sample_id,
    normalized_channel,
    CAST(app_version_major AS INT64) AS app_version_major,
    DATE(SAFE.PARSE_DATETIME('%Y%m%d%H%M%S', client_info.app_build)) AS app_build_date,
    normalized_country_code,
    IF(event_category = 'creditcard', 'credit_card', 'address') AS form_type,
    event_name,
    event_extra,
    -- Autofill tags every event of one form interaction with a shared uuid.
    JSON_VALUE(event_extra, '$.value') AS flow,
    -- popup_shown_* and filled_modified_* name one field in `field_name`.
    JSON_VALUE(event_extra, '$.field_name') AS field_name,
    -- Total autofill events of the client on this day, for the high-volume flag.
    COUNT(*) OVER (PARTITION BY DATE(submission_timestamp), client_id) AS client_daily_event_count
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.events_stream`
  WHERE
    event_category IN ('address', 'creditcard')
    AND event_name IN (
      'detected_address_form',
      'detected_address_form_ext',
      'popup_shown_address_form',
      'filled_address_form',
      'filled_address_form_ext',
      'filled_modified_address_form',
      'submitted_address_form',
      'submitted_address_form_ext',
      'detected_cc_form_v2',
      'popup_shown_cc_form_v2',
      'filled_cc_form_v2',
      'filled_modified_cc_form_v2',
      'submitted_cc_form_v2'
    )
),
-- Each event becomes an array of per-field signals.
event_signals AS (
  SELECT
    * EXCEPT (event_extra, field_name),
    -- Only detected_address_form_ext carries the ML classifier revision.
    IF(
      event_name = 'detected_address_form_ext',
      NULLIF(JSON_VALUE(event_extra, '$.mlversion'), ''),
      NULL
    ) AS ml_version,
    CASE
      -- detected_* and filled_* report every field as an extra key. The address
      -- fields are split across a base and an `_ext` event, so a key absent
      -- from one variant comes back NULL. 'false' means the field is not on
      -- the form.
      WHEN event_name LIKE 'detected_%'
        OR event_name LIKE 'filled_%'
        AND event_name NOT LIKE 'filled_modified_%'
        THEN ARRAY(
            SELECT AS STRUCT
              f.field,
              -- How the field type was found: 'true' = autocomplete attribute,
              -- '0' = regex heuristic, a decimal in (0, 1] = Fathom confidence,
              -- 'ml' = ML classifier. The Glean description says Fathom is an
              -- integer 0-100, but the values sent are decimals such as '0.99'.
              CASE
                WHEN event_name NOT LIKE 'detected_%'
                  THEN NULL
                WHEN f.status = 'ml'
                  THEN 'ml'
                WHEN SAFE_CAST(f.status AS FLOAT64) > 0
                  THEN 'fathom'
                WHEN SAFE_CAST(f.status AS FLOAT64) = 0
                  THEN 'regex'
                WHEN f.status = 'true'
                  THEN 'autocomplete'
                ELSE 'other'
              END AS detection_method,
              event_name LIKE 'filled_%'
              AND f.status = 'filled' AS is_autofilled,
              FALSE AS is_modified,
              FALSE AS is_popup_shown,
              FALSE AS is_split_street_credit
            FROM
              UNNEST(
                [
                  STRUCT('name' AS field, JSON_VALUE(event_extra, '$.name') AS status),
                  STRUCT('given-name', JSON_VALUE(event_extra, '$.given_name')),
                  STRUCT('family-name', JSON_VALUE(event_extra, '$.family_name')),
                  STRUCT('additional-name', JSON_VALUE(event_extra, '$.additional_name')),
                  STRUCT('organization', JSON_VALUE(event_extra, '$.organization')),
                  STRUCT('street-address', JSON_VALUE(event_extra, '$.street_address')),
                  STRUCT('address-line1', JSON_VALUE(event_extra, '$.address_line1')),
                  STRUCT('address-line2', JSON_VALUE(event_extra, '$.address_line2')),
                  STRUCT('address-line3', JSON_VALUE(event_extra, '$.address_line3')),
                  STRUCT('address-level1', JSON_VALUE(event_extra, '$.address_level1')),
                  STRUCT('address-level2', JSON_VALUE(event_extra, '$.address_level2')),
                  STRUCT('postal-code', JSON_VALUE(event_extra, '$.postal_code')),
                  STRUCT('country', JSON_VALUE(event_extra, '$.country')),
                  STRUCT('email', JSON_VALUE(event_extra, '$.email')),
                  STRUCT('tel', JSON_VALUE(event_extra, '$.tel')),
                  STRUCT('cc-name', JSON_VALUE(event_extra, '$.cc_name')),
                  STRUCT('cc-number', JSON_VALUE(event_extra, '$.cc_number')),
                  STRUCT('cc-type', JSON_VALUE(event_extra, '$.cc_type')),
                  STRUCT('cc-exp', JSON_VALUE(event_extra, '$.cc_exp')),
                  STRUCT('cc-exp-month', JSON_VALUE(event_extra, '$.cc_exp_month')),
                  STRUCT('cc-exp-year', JSON_VALUE(event_extra, '$.cc_exp_year'))
                ]
              ) AS f
            WHERE
              f.status IS NOT NULL
              AND f.status != 'false'
          )
      -- popup_shown_* and filled_modified_* use a finer field vocabulary than
      -- the detected/filled extras. Fold each name onto the label the
      -- detected/filled events use (same mapping as metric-hub).
      WHEN event_name LIKE 'popup_shown_%'
        OR event_name LIKE 'filled_modified_%'
        THEN ARRAY(
            SELECT AS STRUCT
              field,
              CAST(NULL AS STRING) AS detection_method,
              FALSE AS is_autofilled,
              event_name LIKE 'filled_modified_%' AS is_modified,
              event_name LIKE 'popup_shown_%' AS is_popup_shown,
              -- The split street address sub-inputs are credited to both street
              -- fields. Autofill writes the street into exactly one of them, so
              -- the credit to the other field is dropped below unless that
              -- field has its own signal.
              field_name IN (
                'address-housenumber',
                'address-extra-housesuffix'
              ) AS is_split_street_credit
            FROM
              UNNEST(
                CASE
                  WHEN field_name IN ('address-housenumber', 'address-extra-housesuffix')
                    THEN ['street-address', 'address-line1']
                  WHEN field_name = 'country-name'
                    THEN ['country']
                  WHEN field_name LIKE 'tel%'
                    THEN ['tel']
                  WHEN field_name IN ('cc-given-name', 'cc-family-name', 'cc-additional-name')
                    THEN ['cc-name']
                  ELSE [field_name]
                END
              ) AS field
            WHERE
              field IS NOT NULL
          )
      ELSE []
    END AS signals
  FROM
    autofill_events
  WHERE
    flow IS NOT NULL
    AND client_id IS NOT NULL
),
per_flow AS (
  SELECT
    submission_date,
    client_id,
    form_type,
    flow,
    ANY_VALUE(legacy_telemetry_client_id) AS legacy_telemetry_client_id,
    ANY_VALUE(profile_group_id) AS profile_group_id,
    ANY_VALUE(sample_id) AS sample_id,
    ANY_VALUE(normalized_channel) AS normalized_channel,
    MAX(app_version_major) AS app_version_major,
    MAX(app_build_date) AS app_build_date,
    ANY_VALUE(normalized_country_code) AS normalized_country_code,
    MAX(ml_version) AS ml_version,
    ANY_VALUE(client_daily_event_count) AS client_daily_event_count,
    LOGICAL_OR(event_name LIKE 'submitted_%') AS is_form_submitted,
    ARRAY_CONCAT_AGG(signals) AS signals
  FROM
    event_signals
  GROUP BY
    submission_date,
    client_id,
    form_type,
    flow
),
-- One entry per (flow, field). A field named by several events in one flow
-- counts once. If the detected_* events of one flow disagree on the method,
-- the inferred methods win over autocomplete, so that a field counts as
-- detected exactly when metric-hub counts it.
per_flow_fields AS (
  SELECT
    * EXCEPT (signals),
    ARRAY(
      SELECT AS STRUCT
        field,
        ARRAY_AGG(
          detection_method IGNORE NULLS
          ORDER BY
            CASE
              detection_method
              WHEN 'ml'
                THEN 1
              WHEN 'fathom'
                THEN 2
              WHEN 'regex'
                THEN 3
              WHEN 'autocomplete'
                THEN 4
              ELSE 5
            END
          LIMIT
            1
        )[SAFE_OFFSET(0)] AS detection_method,
        LOGICAL_OR(is_autofilled) AS is_autofilled,
        LOGICAL_OR(is_modified) AS is_modified,
        LOGICAL_OR(is_popup_shown) AS is_popup_shown
      FROM
        UNNEST(signals)
      GROUP BY
        field
      HAVING
        -- Drop a split street credit to a field that has no other signal.
        LOGICAL_OR(NOT is_split_street_credit)
        OR detection_method IS NOT NULL
        OR is_autofilled
      ORDER BY
        field
    ) AS fields
  FROM
    per_flow
)
SELECT
  * EXCEPT (fields),
  COALESCE((SELECT LOGICAL_OR(is_autofilled) FROM UNNEST(fields)), FALSE) AS has_autofill,
  COALESCE((SELECT LOGICAL_OR(is_popup_shown) FROM UNNEST(fields)), FALSE) AS has_popup,
  COALESCE(
    (SELECT LOGICAL_OR(detection_method IN ('ml', 'fathom', 'regex')) FROM UNNEST(fields)),
    FALSE
  ) AS has_inferred_field,
  COALESCE(
    (SELECT LOGICAL_OR(detection_method = 'autocomplete') FROM UNNEST(fields)),
    FALSE
  ) AS has_autocomplete_field,
  -- Same definitions as metric-hub `form_autofill_forms`: only fields with an
  -- inferred type (regex, Fathom or ML) count.
  (
    SELECT
      COUNTIF(detection_method IN ('ml', 'fathom', 'regex'))
    FROM
      UNNEST(fields)
  ) AS detected_fields,
  (
    SELECT
      COUNTIF(detection_method IN ('ml', 'fathom', 'regex') AND is_autofilled)
    FROM
      UNNEST(fields)
  ) AS autofilled_fields,
  (
    SELECT
      COUNTIF(detection_method IN ('ml', 'fathom', 'regex') AND is_autofilled AND is_modified)
    FROM
      UNNEST(fields)
  ) AS modified_fields,
  (
    SELECT
      COUNTIF(detection_method IN ('ml', 'fathom', 'regex') AND is_autofilled AND NOT is_modified)
    FROM
      UNNEST(fields)
  ) AS net_kept_fields,
  fields
FROM
  per_flow_fields
