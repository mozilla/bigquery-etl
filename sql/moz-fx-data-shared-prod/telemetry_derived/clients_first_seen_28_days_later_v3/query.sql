WITH clients_first_seen_28_days_ago AS (
  SELECT
    client_id,
    sample_id,
    first_seen_date,
    second_seen_date,
    architecture,
    app_build_id,
    app_name,
    locale,
    vendor,
    app_version,
    xpcom_abi,
    document_id,
    distribution_id,
    partner_distribution_version,
    partner_distributor,
    partner_distributor_channel,
    partner_id,
    attribution_campaign,
    attribution_content,
    attribution_dltoken,
    attribution_dlsource,
    attribution_experiment,
    attribution_medium,
    attribution_source,
    attribution_ua,
    attribution_variation,
    engine_data_load_path,
    engine_data_name,
    engine_data_origin,
    engine_data_submission_url,
    apple_model_id,
    city,
    db_version,
    subdivision1,
    isp_name,
    normalized_channel,
    country,
    normalized_os,
    normalized_os_version,
    startup_profile_selection_reason,
    installation_first_seen_admin_user,
    installation_first_seen_default_path,
    installation_first_seen_failure_reason,
    installation_first_seen_from_msi,
    installation_first_seen_install_existed,
    installation_first_seen_installer_type,
    installation_first_seen_other_inst,
    installation_first_seen_other_msix_inst,
    installation_first_seen_profdir_existed,
    installation_first_seen_silent,
    installation_first_seen_version,
    os,
    os_version,
    windows_build_number,
    windows_version,
    metadata,
    profile_group_id
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.clients_first_seen_v3`
  WHERE
    first_seen_date = DATE_SUB(@submission_date, INTERVAL 27 day)
),
clients_first_seen_28_days_ago_with_days_seen AS (
  SELECT
    clients_first_seen_28_days_ago.*,
    cls.days_seen_bits,
    cls.days_visited_1_uri_bits,
    cls.days_interacted_bits,
  FROM
    clients_first_seen_28_days_ago
  LEFT JOIN
    `moz-fx-data-shared-prod.telemetry.clients_last_seen` cls
    ON clients_first_seen_28_days_ago.client_id = cls.client_id
    AND cls.submission_date = @submission_date
)
SELECT
  * REPLACE (
    COALESCE(
      days_seen_bits,
      mozfun.bits28.from_string('0000000000000000000000000000')
    ) AS days_seen_bits,
    COALESCE(
      days_visited_1_uri_bits,
      mozfun.bits28.from_string('0000000000000000000000000000')
    ) AS days_visited_1_uri_bits,
    COALESCE(
      days_interacted_bits,
      mozfun.bits28.from_string('0000000000000000000000000000')
    ) AS days_interacted_bits
  ),
  COALESCE(
    BIT_COUNT(mozfun.bits28.from_string('1111111000000000000000000000') & days_seen_bits) >= 5,
    FALSE
  ) AS activated,
  COALESCE(
    BIT_COUNT(mozfun.bits28.from_string('0111111111111111111111111111') & days_seen_bits) > 0,
    FALSE
  ) AS returned_second_day,
  COALESCE(
    BIT_COUNT(
      mozfun.bits28.from_string(
        '0111111111111111111111111111'
      ) & days_visited_1_uri_bits & days_interacted_bits
    ) > 0,
    FALSE
  ) AS qualified_second_day,
  COALESCE(
    BIT_COUNT(mozfun.bits28.from_string('0000000000000000000001111111') & days_seen_bits) > 0,
    FALSE
  ) AS retained_week4,
  COALESCE(
    BIT_COUNT(
      mozfun.bits28.from_string(
        '0000000000000000000001111111'
      ) & days_visited_1_uri_bits & days_interacted_bits
    ) > 0,
    FALSE
  ) AS qualified_week4,
  @submission_date AS submission_date,
FROM
  clients_first_seen_28_days_ago_with_days_seen
