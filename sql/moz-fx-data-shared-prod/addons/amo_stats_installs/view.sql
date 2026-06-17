-- Note: In the future this view will also include fenix_stats_installs
-- Will be done via: WEBEXT-2609
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.addons.amo_stats_installs`
AS
SELECT
  *,
  -- app value follows the same convention as addons.amo_stats_dau
  "Desktop" AS app,
FROM
  `moz-fx-data-shared-prod.addons.firefox_desktop_stats_installs`
