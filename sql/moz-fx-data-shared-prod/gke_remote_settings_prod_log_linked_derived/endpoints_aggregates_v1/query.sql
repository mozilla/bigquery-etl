WITH attachments_urls AS (
  SELECT
    'attachments' AS source,
    http_request.request_url AS url,
    http_request.response_size AS size,
    *
  FROM
    `moz-fx-remote-settings-prod.remote_settings_prod_default_log_linked._AllLogs`
  WHERE
    http_request.request_url LIKE 'https://firefox-settings-attachments.cdn.mozilla.net%' -- exclude bad URLs
    AND http_request.status = 200
),
api_urls AS (
  SELECT
    'api' AS source,
    http_request.request_url AS url,
    http_request.response_size AS size,
    *
  FROM
    `moz-fx-remote-settings-prod.gke_remote_settings_prod_log_linked._AllLogs`
  WHERE
    http_request.request_url LIKE 'https://firefox.settings.services.mozilla.com%'  -- exclude writers, etc.
    AND http_request.status = 200
),
urls AS (
  SELECT
    *
  FROM
    attachments_urls
  UNION ALL
  SELECT
    *
  FROM
    api_urls
),
urls_with_endpoint AS (
  SELECT
    CASE
      WHEN source = 'attachments'
        AND url LIKE '%/bundles/%'
        THEN 'bundles'
      WHEN source = 'attachments'
        THEN 'attachments'
      WHEN url LIKE '%/v1/'
        THEN 'root'
      WHEN url LIKE '%/records%'
        THEN 'records'
      WHEN url LIKE '%/changeset%'
        AND url LIKE '%collection=%'
        THEN 'filtered-monitor'
      WHEN url LIKE '%/changeset%'
        THEN 'changeset'
      WHEN url LIKE '%/collections/%'
        THEN 'collection'
      ELSE 'other'
    END AS endpoint,
    *
  FROM
    urls
  WHERE
    EXTRACT(DATE FROM timestamp) = @submission_date
),
urls_with_types_bid_cid AS (
  SELECT
    CASE
      WHEN endpoint = 'bundles'
        THEN REGEXP_EXTRACT(url, r'/bundles/([^\\.]+)')
      WHEN endpoint = 'attachments'
        THEN REGEXP_EXTRACT(url, r'https://[^/]+/([^/]+)/[^/]+/[^/]+')
      WHEN endpoint = 'filtered-monitor'
        THEN REGEXP_EXTRACT(url, r'bucket=([^&]+)')
      ELSE REGEXP_EXTRACT(url, r'/buckets/([^/]+)/')
    END AS bid,
    CASE
      WHEN endpoint = 'bundles'
        THEN REGEXP_EXTRACT(url, r'/bundles/([^\\.]+)')
      WHEN endpoint = 'attachments'
        THEN REGEXP_EXTRACT(url, r'https://[^/]+/[^/]+/([^/]+)/[^/]+')
      WHEN endpoint = 'filtered-monitor'
        THEN REGEXP_EXTRACT(url, r'collection=([^&]+)')
      ELSE REGEXP_EXTRACT(url, r'/collections/([^/?]+)')
    END AS cid,
    *
  FROM
    urls_with_endpoint
),
total_size_hits_by_cid AS (
  SELECT
    MAX(timestamp) AS timestamp,
    source,
    endpoint,
    REPLACE(
      REPLACE(REPLACE(bid, "-staging", ""), "staging", "blocklists"),
      "-workspace",
      ""
    ) AS bid,
    COALESCE(SPLIT(cid, '--')[SAFE_OFFSET(1)], cid) AS cid,
    SUM(size) AS size,
    COUNT(*) AS hits
  FROM
    urls_with_types_bid_cid
  GROUP BY
    source,
    endpoint,
    bid,
    cid
)
SELECT
  timestamp,
  source,
  endpoint,
  bid AS bucket_id,
  cid AS collection_id,
  hits,
  size
FROM
  total_size_hits_by_cid
WHERE
  hits > 2  -- ignore noise (not real clients)
