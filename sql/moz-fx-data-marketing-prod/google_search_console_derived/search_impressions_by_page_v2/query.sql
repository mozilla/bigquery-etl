WITH searchdata_url_impression_union AS (
  SELECT
    data_date,
    site_url,
    url,
    query,
    is_anonymized_query,
    is_anonymized_discover,
    is_page_experience,
    search_type,
    is_action,
    is_amp_blue_link,
    is_amp_image_result,
    is_amp_story,
    is_amp_top_stories,
    is_edu_q_and_a,
    is_events_details,
    is_events_listing,
    is_job_details,
    is_job_listing,
    is_learning_videos,
    is_math_solvers,
    is_merchant_listings,
    is_organic_shopping,
    is_practice_problems,
    is_product_snippets,
    is_recipe_feature,
    is_recipe_rich_snippet,
    is_review_snippet,
    is_search_appearance_android_app,
    is_special_announcement,
    is_subscribed_content,
    is_tpf_faq,
    is_tpf_howto,
    is_tpf_qa,
    is_translated_result,
    is_video,
    country,
    device,
    impressions,
    clicks,
    sum_position
  FROM
    `moz-fx-data-marketing-prod.searchconsole_addons.searchdata_url_impression`
  UNION ALL
  SELECT
    data_date,
    site_url,
    url,
    query,
    is_anonymized_query,
    is_anonymized_discover,
    is_page_experience,
    search_type,
    is_action,
    is_amp_blue_link,
    is_amp_image_result,
    is_amp_story,
    is_amp_top_stories,
    is_edu_q_and_a,
    is_events_details,
    is_events_listing,
    is_job_details,
    is_job_listing,
    is_learning_videos,
    is_math_solvers,
    is_merchant_listings,
    is_organic_shopping,
    is_practice_problems,
    is_product_snippets,
    is_recipe_feature,
    is_recipe_rich_snippet,
    is_review_snippet,
    is_search_appearance_android_app,
    is_special_announcement,
    is_subscribed_content,
    is_tpf_faq,
    is_tpf_howto,
    is_tpf_qa,
    is_translated_result,
    is_video,
    country,
    device,
    impressions,
    clicks,
    sum_position
  FROM
    `moz-fx-data-marketing-prod.searchconsole_blog.searchdata_url_impression`
  UNION ALL
  SELECT
    data_date,
    site_url,
    url,
    query,
    is_anonymized_query,
    is_anonymized_discover,
    is_page_experience,
    search_type,
    is_action,
    is_amp_blue_link,
    is_amp_image_result,
    is_amp_story,
    is_amp_top_stories,
    is_edu_q_and_a,
    is_events_details,
    is_events_listing,
    is_job_details,
    is_job_listing,
    is_learning_videos,
    is_math_solvers,
    is_merchant_listings,
    is_organic_shopping,
    is_practice_problems,
    is_product_snippets,
    is_recipe_feature,
    is_recipe_rich_snippet,
    is_review_snippet,
    is_search_appearance_android_app,
    is_special_announcement,
    is_subscribed_content,
    is_tpf_faq,
    is_tpf_howto,
    is_tpf_qa,
    is_translated_result,
    is_video,
    country,
    device,
    impressions,
    clicks,
    sum_position
  FROM
    `moz-fx-data-marketing-prod.searchconsole_getpocket.searchdata_url_impression`
  UNION ALL
  SELECT
    data_date,
    site_url,
    url,
    query,
    is_anonymized_query,
    is_anonymized_discover,
    is_page_experience,
    search_type,
    is_action,
    is_amp_blue_link,
    is_amp_image_result,
    is_amp_story,
    is_amp_top_stories,
    is_edu_q_and_a,
    is_events_details,
    is_events_listing,
    is_job_details,
    is_job_listing,
    is_learning_videos,
    is_math_solvers,
    is_merchant_listings,
    is_organic_shopping,
    is_practice_problems,
    is_product_snippets,
    is_recipe_feature,
    is_recipe_rich_snippet,
    is_review_snippet,
    is_search_appearance_android_app,
    is_special_announcement,
    is_subscribed_content,
    is_tpf_faq,
    is_tpf_howto,
    is_tpf_qa,
    is_translated_result,
    is_video,
    country,
    device,
    impressions,
    clicks,
    sum_position
  FROM
    `moz-fx-data-marketing-prod.searchconsole_support.searchdata_url_impression`
  UNION ALL
  SELECT
    data_date,
    site_url,
    url,
    query,
    is_anonymized_query,
    is_anonymized_discover,
    is_page_experience,
    search_type,
    is_action,
    is_amp_blue_link,
    is_amp_image_result,
    is_amp_story,
    is_amp_top_stories,
    is_edu_q_and_a,
    is_events_details,
    is_events_listing,
    is_job_details,
    is_job_listing,
    is_learning_videos,
    is_math_solvers,
    is_merchant_listings,
    is_organic_shopping,
    is_practice_problems,
    is_product_snippets,
    is_recipe_feature,
    is_recipe_rich_snippet,
    is_review_snippet,
    is_search_appearance_android_app,
    is_special_announcement,
    is_subscribed_content,
    is_tpf_faq,
    is_tpf_howto,
    is_tpf_qa,
    is_translated_result,
    is_video,
    country,
    device,
    impressions,
    clicks,
    sum_position
  FROM
    `moz-fx-data-marketing-prod.searchconsole_www.searchdata_url_impression`
)
SELECT
  data_date AS `date`,
  site_url,
  REGEXP_EXTRACT(site_url, r'^(?:https?://|sc-domain:)([^/]+)') AS site_domain_name,
  url AS page_url,
  REGEXP_EXTRACT(url, r'^https?://([^/]+)') AS page_domain_name,
  REGEXP_EXTRACT(url, r'^https?://(?:[^/]+)([^\?#]*)') AS page_path,
  REGEXP_EXTRACT(url, r'^https?://(?:[^/]+)/*([^/\?#]*)') AS page_path_segment_1,
  query,
  (is_anonymized_query OR is_anonymized_discover) AS is_anonymized,
  is_page_experience AS has_good_page_experience,
  INITCAP(REPLACE(search_type, '_', ' ')) AS search_type,
  CASE
    WHEN is_action
      THEN 'Action'
    WHEN is_amp_blue_link
      THEN 'AMP non-rich result'
    WHEN is_amp_image_result
      THEN 'AMP on image result'
    WHEN is_amp_story
      THEN 'AMP story'
    WHEN is_amp_top_stories
      THEN 'AMP top stories'
    WHEN is_edu_q_and_a
      THEN 'Education Q&A'
    WHEN is_events_details
      THEN 'Event details'
    WHEN is_events_listing
      THEN 'Event listing'
    WHEN is_job_details
      THEN 'Job details'
    WHEN is_job_listing
      THEN 'Job listing'
    WHEN is_learning_videos
      THEN 'Learning videos'
    WHEN is_math_solvers
      THEN 'Math solvers'
    WHEN is_merchant_listings
      THEN 'Merchant listings'
    WHEN is_organic_shopping
      THEN 'Shopping'
    WHEN is_practice_problems
      THEN 'Practice problems'
    WHEN is_product_snippets
      THEN 'Product snippets'
    WHEN is_recipe_feature
      THEN 'Recipe feature'
    WHEN is_recipe_rich_snippet
      THEN 'Recipe rich snippet'
    WHEN is_review_snippet
      THEN 'Review snippet'
    WHEN is_search_appearance_android_app
      THEN 'Android app'
    WHEN is_special_announcement
      THEN 'Special announcement'
    WHEN is_subscribed_content
      THEN 'Subscribed content'
    WHEN is_tpf_faq
      THEN 'FAQ rich result'
    WHEN is_tpf_howto
      THEN 'How-to rich result'
    WHEN is_tpf_qa
      THEN 'Q&A rich result'
    WHEN is_translated_result
      THEN 'Translated result'
    WHEN is_video
      THEN 'Video'
    ELSE 'Normal result'
  END AS search_appearance,
  UPPER(country) AS country_code,
  INITCAP(device) AS device_type,
  impressions,
  clicks,
  ((sum_position / impressions) + 1) AS average_position
FROM
  searchdata_url_impression_union
WHERE
  data_date = @date
