WITH transformed_contact_raw AS (
  SELECT
    email_id,
    CASE
      WHEN sub_common_voice = 1
        THEN 'common-voice'
      WHEN sub_firefox_accounts_journey = 1
        THEN 'firefox-accounts-journey'
      WHEN sub_firefox_news = 1
        THEN 'firefox-news'
      WHEN sub_hubs = 1
        THEN 'hubs'
      WHEN sub_internet_health_report = 1
        THEN 'internet-health-report'
      WHEN sub_knowledge_is_power = 1
        THEN 'knowledge-is-power'
      WHEN sub_miti = 1
        THEN 'miti'
      WHEN sub_mixed_reality = 1
        THEN 'mixed-reality'
      WHEN sub_mozilla_fellowship_awardee_alumni = 1
        THEN 'mozilla-fellowship-awardee-alumni'
      WHEN sub_mozilla_festival = 1
        THEN 'mozilla-festival'
      WHEN sub_mozilla_foundation = 1
        THEN 'mozilla-foundation'
      WHEN sub_mozilla_technology = 1
        THEN 'mozilla-technology'
      WHEN sub_mozillians_nda = 1
        THEN 'mozillians-nda'
      WHEN sub_take_action_for_the_internet = 1
        THEN 'take-action-for-the-internet'
      WHEN sub_test_pilot = 1
        THEN 'test-pilot'
      WHEN sub_about_mozilla = 1
        THEN 'about-mozilla'
      WHEN sub_apps_and_hacks = 1
        THEN 'apps-and-hacks'
      WHEN sub_rally = 1
        THEN 'rally'
      WHEN sub_firefox_sweepstakes = 1
        THEN 'firefox-sweepstakes'
      ELSE NULL
    END AS newsletter_name,
    TRUE AS subscribed
  FROM
    `moz-fx-data-marketing-prod.acoustic.contact_raw_v1`
  WHERE
    -- Ensure at least one subscription is true
    sub_common_voice = 1
    OR sub_firefox_accounts_journey = 1
    OR sub_firefox_news = 1
    OR sub_hubs = 1
    OR sub_internet_health_report = 1
    OR sub_knowledge_is_power = 1
    OR sub_miti = 1
    OR sub_mixed_reality = 1
    OR sub_mozilla_fellowship_awardee_alumni = 1
    OR sub_mozilla_festival = 1
    OR sub_mozilla_foundation = 1
    OR sub_mozilla_technology = 1
    OR sub_mozillians_nda = 1
    OR sub_take_action_for_the_internet = 1
    OR sub_test_pilot = 1
    OR sub_about_mozilla = 1
    OR sub_apps_and_hacks = 1
    OR sub_firefox_sweepstakes = 1
),
final_newsletters AS (
  SELECT
    email_id,
    newsletter_name,
    subscribed
  FROM
    transformed_contact_raw
  WHERE
    newsletter_name IS NOT NULL
  UNION ALL
  SELECT
    email_id,
    name AS newsletter_name,
    TRUE AS subscribed
  FROM
    `moz-fx-data-shared-prod.ctms_braze.ctms_newsletters`
)
SELECT
  email_id AS external_id,
  ARRAY_AGG(DISTINCT newsletter_name) AS newsletters
FROM
  final_newsletters
GROUP BY
  email_id;
