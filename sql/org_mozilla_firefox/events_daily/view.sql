CREATE OR REPLACE VIEW
  org_mozilla_firefox.events_daily
AS
SELECT
  *
FROM
  org_mozilla_firefox_derived.events_daily_v1
