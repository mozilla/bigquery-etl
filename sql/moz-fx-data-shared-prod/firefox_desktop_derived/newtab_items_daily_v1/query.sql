WITH events_dedupe AS (
  SELECT
      *
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_stable.newtab_v1`
  WHERE
    DATE(submission_timestamp) = @submission_date
  QUALIFY ROW_NUMBER() OVER (PARTITION BY document_id ORDER BY submission_timestamp) = 1
),

events_unnested AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    mozfun.norm.browser_version_info(client_info.app_display_version).major_version AS app_version,
    normalized_channel AS channel,
    metrics.string.newtab_locale AS locale,
    normalized_country_code AS country,
    timestamp AS event_timestamp,
    category AS event_category,
    name AS event_name,
    extra AS event_details,
  FROM
    events_dedupe,
    UNNEST(events)
  WHERE category IN ('pocket')
    AND name IN ('impression', 'click', 'save', 'dismiss')
    AND mozfun.norm.browser_version_info(client_info.app_display_version).major_version >= 121
),

flattened_events AS (
  select
      submission_date,
      app_version,
      channel,
      locale,
      country,
      event_category,
      event_name,
      mozfun.map.get_key(event_details, 'corpus_item_id') AS corpus_item_id,
      SAFE_CAST(mozfun.map.get_key(event_details, 'position') AS INT64) AS position,
      SAFE_CAST(mozfun.map.get_key(event_details, 'is_sponsored') AS BOOLEAN) AS is_sponsored,
      SAFE_CAST(mozfun.map.get_key(event_details, 'is_secton_followed') AS BOOLEAN) AS is_secton_followed,
      mozfun.map.get_key(event_details, 'matches_selected_topic') AS matches_selected_topic,
      mozfun.map.get_key(event_details, 'newtab_visit_id') AS newtab_visit_id,
      SAFE_CAST(mozfun.map.get_key(event_details, 'received_rank') AS INT64) AS received_rank,
      mozfun.map.get_key(event_details, 'section') AS section,
      mozfun.map.get_key(event_details, 'section_position') AS section_position,
      mozfun.map.get_key(event_details, 'topic') AS topic,
  from events_unnested
)

select
    submission_date,
    app_version,
    channel,
    locale,
    country,
    case
        when split(locale,'-')[0] = 'de' then 'NEW_TAB_DE_DE'
        when split(locale,'-')[0] = 'es' then 'NEW_TAB_ES_ES'
        when split(locale,'-')[0] = 'fr' then 'NEW_TAB_FR_FR'
        when split(locale,'-')[0] = 'it' then 'NEW_TAB_IT_IT'
        when split(locale,'-')[0] = 'en' and (split(locale,'-')[1] in ('GB', 'IE') or country in ('GB', 'IE')) then 'NEW_TAB_EN_GB'
        when split(locale,'-')[0] = 'en' and (split(locale,'-')[1] = 'IN' or country = 'IN') then 'NEW_TAB_EN_INTL'
        when split(locale,'-')[0] = 'en' and (split(locale,'-')[1] in ('US','CA') or country in ('US','CA')) then 'NEW_TAB_EN_US'
        else 'NEW_TAB_EN_US'
    end as scheduled_surface_id,
    corpus_item_id,
    position,
    is_sponsored,
    is_secton_followed,
    matches_selected_topic,
    received_rank,
    section,
    section_position,
    topic,
    SUM(IF(event_name = 'impression', 1, 0)) AS impression_count,
    SUM(IF(event_name = 'click', 1, 0)) AS click_count,
    SUM(IF(event_name = 'save', 1, 0)) AS save_count,
    SUM(IF(event_name = 'dismiss', 1, 0)) AS dismiss_count,
from flattened_events
where section is not null
group by
    submission_date,
    app_version,
    channel,
    locale,
    country,
    scheduled_surface_id,
    corpus_item_id,
    position,
    is_sponsored,
    is_secton_followed,
    matches_selected_topic,
    received_rank,
    section,
    section_position,
    topic
