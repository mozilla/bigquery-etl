WITH _current AS (
  SELECT
    submission_date AS first_seen_date,
    submission_date,
    client_id,
    sample_id,
    normalized_channel,
    n_metrics_ping,
    days_sent_metrics_ping_bits,
    profile_group_id,
    search_with_ads_count_all,
    search_count_all,
    ad_clicks_count_all,
    apple_model_id,
    default_search_engine,
    xpcom_abi,
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
    installation_first_seen_version
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_derived.metrics_clients_daily_v1`
  WHERE
    submission_date = @submission_date
    AND client_id IS NOT NULL
),
_previous AS (
  SELECT
    first_seen_date,
    submission_date,
    client_id,
    sample_id,
    normalized_channel,
    n_metrics_ping,
    days_sent_metrics_ping_bits,
    profile_group_id,
    search_with_ads_count_all,
    search_count_all,
    ad_clicks_count_all,
    apple_model_id,
    default_search_engine,
    xpcom_abi,
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
    installation_first_seen_version
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_derived.metrics_clients_first_seen_v1`
  WHERE
    submission_date < @submission_date
)
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
