SELECT
  email_id,
  basket_token,
  NULLIF(sfdc_id, '') AS sfdc_id,
  NULLIF(fxa_id, '') AS fxa_id,
  CAST(COALESCE(CAST(double_opt_in AS INTEGER), 0) AS BOOLEAN) AS double_opt_in,
  CAST(COALESCE(CAST(has_opted_out_of_email AS INTEGER), 0) AS BOOLEAN) AS has_opted_out_of_email,
  LOWER(COALESCE(NULLIF(email_lang, ''), "unknown")) AS email_lang,
  LOWER(email_format) AS email_format,
  LOWER(COALESCE(NULLIF(mailing_country, ''), "unknown")) AS mailing_country,
  LOWER(COALESCE(NULLIF(cohort, ''), "unknown")) AS cohort,
  mozfun.datetime_util.fxa_parse_date(fxa_created_date) AS fxa_created_date,
  LOWER(COALESCE(NULLIF(fxa_first_service, ''), "unknown")) AS fxa_first_service,
  CAST(COALESCE(CAST(fxa_account_deleted AS INTEGER), 0) AS BOOLEAN) AS fxa_account_deleted,
  CAST(COALESCE(CAST(sub_mozilla_foundation AS INTEGER), 0) AS BOOLEAN) AS sub_mozilla_foundation,
  CAST(COALESCE(CAST(sub_common_voice AS INTEGER), 0) AS BOOLEAN) AS sub_common_voice,
  CAST(COALESCE(CAST(sub_hubs AS INTEGER), 0) AS BOOLEAN) AS sub_hubs,
  CAST(COALESCE(CAST(sub_mixed_reality AS INTEGER), 0) AS BOOLEAN) AS sub_mixed_reality,
  CAST(
    COALESCE(CAST(sub_internet_health_report AS INTEGER), 0) AS BOOLEAN
  ) AS sub_internet_health_report,
  CAST(COALESCE(CAST(sub_miti AS INTEGER), 0) AS BOOLEAN) AS sub_miti,
  CAST(
    COALESCE(CAST(sub_mozilla_fellowship_awardee_alumni AS INTEGER), 0) AS BOOLEAN
  ) AS sub_mozilla_fellowship_awardee_alumni,
  CAST(COALESCE(CAST(sub_mozilla_festival AS INTEGER), 0) AS BOOLEAN) AS sub_mozilla_festival,
  CAST(COALESCE(CAST(sub_mozilla_technology AS INTEGER), 0) AS BOOLEAN) AS sub_mozilla_technology,
  CAST(COALESCE(CAST(sub_mozillians_nda AS INTEGER), 0) AS BOOLEAN) AS sub_mozillians_nda,
  CAST(
    COALESCE(CAST(sub_firefox_accounts_journey AS INTEGER), 0) AS BOOLEAN
  ) AS sub_firefox_accounts_journey,
  CAST(COALESCE(CAST(sub_knowledge_is_power AS INTEGER), 0) AS BOOLEAN) AS sub_knowledge_is_power,
  CAST(
    COALESCE(CAST(sub_take_action_for_the_internet AS INTEGER), 0) AS BOOLEAN
  ) AS sub_take_action_for_the_internet,
  CAST(COALESCE(CAST(sub_test_pilot AS INTEGER), 0) AS BOOLEAN) AS sub_test_pilot,
  CAST(COALESCE(CAST(sub_firefox_news AS INTEGER), 0) AS BOOLEAN) AS sub_firefox_news,
  CAST(COALESCE(CAST(sub_about_mozilla AS INTEGER), 0) AS BOOLEAN) AS sub_about_mozilla,
  CAST(COALESCE(CAST(sub_apps_and_hacks AS INTEGER), 0) AS BOOLEAN) AS sub_apps_and_hacks,
  CAST(COALESCE(CAST(sub_rally AS INTEGER), 0) AS BOOLEAN) AS sub_rally,
  CAST(
    COALESCE(CAST(NULLIF(sub_firefox_sweepstakes, '') AS INTEGER), 0) AS BOOLEAN
  ) AS sub_firefox_sweepstakes,
  LOWER(NULLIF(vpn_waitlist_geo, '')) AS vpn_waitlist_geo,
  SPLIT(NULLIF(vpn_waitlist_platform, ''), ",") AS vpn_waitlist_platform,
  LOWER(NULLIF(relay_waitlist_geo, '')),
  RECIPIENT_ID AS recipient_id,
  mozfun.datetime_util.fxa_parse_date(create_timestamp) AS date_created,
  last_modified_date,
FROM
  `moz-fx-data-marketing-prod.acoustic.contact_export_raw_v1`
WHERE
  last_modified_date = DATE(@submission_date)
