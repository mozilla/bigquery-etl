CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.releases`
AS
SELECT
  *,
  mozfun.norm.extract_version(version, "major") AS major_version,
  mozfun.norm.extract_version(version, "minor") AS minor_version,
  mozfun.norm.extract_version(version, "patch") AS patch_version,
  mozfun.norm.extract_version(version, "beta") AS beta_version,
FROM
  `moz-fx-data-shared-prod.telemetry_derived.releases_v1`
