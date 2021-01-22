WITH download_event_count AS (
  SELECT
    date,
    visit_identifier,
    device_category,
    operating_system,
    `language`,
    country,
    source,
    medium,
    campaign,
    ad_content,
    browser,
    COUNTIF(event_action = 'Firefox Download') AS download_events,
  FROM
    `moz-fx-data-marketing-prod.ga_derived.www_site_hits_v1`
  WHERE
    date = @submission_date
    AND hit_type = 'EVENT'
    AND event_category IS NOT NULL
    AND event_label LIKE 'Firefox for Desktop%'
  GROUP BY
    date,
    visit_identifier,
    device_category,
    operating_system,
    `language`,
    country,
    source,
    medium,
    campaign,
    ad_content,
    browser
)
SELECT
  *,
  COUNTIF(download_events > 0) AS downloads,
  COUNTIF(
    download_events > 0
    AND NOT `moz-fx-data-shared-prod.udf.ga_is_mozilla_browser`(browser)
  ) AS non_fx_downloads,
FROM
  download_event_count
GROUP BY
  date,
  device_category,
  operating_system,
  `language`,
  visit_identifier,
  country,
  source,
  medium,
  campaign,
  ad_content,
  browser,
  download_events
