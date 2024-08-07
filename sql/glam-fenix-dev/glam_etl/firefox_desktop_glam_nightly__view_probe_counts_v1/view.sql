-- view for firefox_desktop_glam_nightly__view_probe_counts_v1;
CREATE OR REPLACE VIEW
  `glam-fenix-dev.glam_etl.firefox_desktop_glam_nightly__view_probe_counts_v1`
AS
WITH all_counts AS (
  SELECT
    *
  FROM
    `glam-fenix-dev.glam_etl.firefox_desktop_glam_nightly__scalar_probe_counts_v1`
  UNION ALL
  SELECT
    *
  FROM
    `glam-fenix-dev.glam_etl.firefox_desktop_glam_nightly__histogram_probe_counts_v1`
)
SELECT
  *
FROM
  all_counts
