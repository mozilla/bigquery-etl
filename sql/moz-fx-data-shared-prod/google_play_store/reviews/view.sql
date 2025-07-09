CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.google_play_store.reviews`
AS
SELECT
  *,
  MAX(DATE(_PARTITIONTIME)) OVER () AS _LATEST_DATE,
  DATE(_PARTITIONTIME) AS _DATA_DATE
FROM
  `444337733603.google_play_store.p_Reviews_v1`
