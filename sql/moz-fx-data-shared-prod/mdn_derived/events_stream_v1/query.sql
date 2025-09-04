CREATE OR REPLACE VIEW `moz-fx-data-shared-prod.mdn.events_stream_v1` AS
  (SELECT *
   FROM `moz-fx-data-shared-prod.mdn_fred.events_stream`
   UNION ALL SELECT *
   FROM `moz-fx-data-shared-prod.mdn_yari.events_stream`)
