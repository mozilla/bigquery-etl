SELECT
  *,
  MAX(DATE(_PARTITIONTIME)) OVER () AS _LATEST_DATE,
  DATE(_PARTITIONTIME) AS _DATA_DATE
FROM
  `moz-fx-data-marketing-prod.google_play_store.p_Reviews_v1`
