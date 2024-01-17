CREATE OR REPLACE VIEW
  `moz-fx-data-marketing-prod.fetch.fetch_deduped`
AS
WITH metrics_ingestion_date_per_ad AS (
  SELECT
    _table_suffix AS ingestionDate,
    MAX(_table_suffix) OVER (PARTITION BY date, Adname) AS IngestionDateMax,
    *,
  FROM
    `moz-fx-data-marketing-prod.fetch.metric_*`
),
latest_ingested_metrics AS (
  SELECT
    *,
  FROM
    metrics_ingestion_date_per_ad
  WHERE
    ingestionDate = IngestionDateMax
)
SELECT
  Created,
  AdName AS AdNameTrafficking,
  TrackingLink,
  Product,
  Campaign,
  Vendor,
  Country,
  OperatingSystem,
  Device,
  Channel,
  CreativeType,
  Targeting,
  Creative,
  CreativeSize,
  CreativeConcept,
  CreativeLanguage,
  TrafficType,
  Tracking,
  Goal,
  MediaType,
  Placement,
  SocialString,
  metric.*,
FROM
  `moz-fx-data-marketing-prod.fetch.latest_trafficking` AS trafficking
LEFT JOIN
  -- Join metric / spend data to campaign information data
  latest_ingested_metrics AS metric
  USING (AdName)
