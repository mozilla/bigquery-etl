CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mozphab.events`
AS SELECT
  * REPLACE(
    mozfun.norm.metadata(metadata) AS metadata)
FROM
  `moz-fx-data-shared-prod.mozphab_stable.events_v1`
