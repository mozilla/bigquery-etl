-- Return the most likely version over many windows of the estimated major
-- version
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod`.org_mozilla_fenix.geckoview_version
AS
WITH agg AS (
  SELECT
    array_agg(t ORDER BY geckoview_major_version DESC, n_pings DESC LIMIT 1)[offset(0)] AS top_row
  FROM
    `moz-fx-data-shared-prod`.org_mozilla_fenix_derived.geckoview_version_v1 t
  GROUP BY
    build_hour
)
SELECT
  top_row.*
FROM
  agg
