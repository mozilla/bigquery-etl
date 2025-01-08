-- This should match https://github.com/mozilla/gcp-ingestion/blob/fb7e9ed9e891e3e2320d85e05d7c1a1aedb57780/ingestion-beam/src/main/java/com/mozilla/telemetry/transforms/NormalizeAttributes.java#L34
CREATE OR REPLACE FUNCTION norm.app_channel(channel_name STRING)
RETURNS STRING AS (
  CASE
    WHEN channel_name IN ("release", "esr", "beta", "aurora", "nightly")
      THEN channel_name
    -- The cck suffix was used for various deployments before Firefox Quantum;
    -- cck refers to the "Client Customization Wizard", see
    -- https://mike.kaply.com/2012/04/13/customizing-firefox-extensions-and-the-cck-wizard/
    WHEN STARTS_WITH(channel_name, "nightly-cck-")
      THEN "nightly"
    WHEN STARTS_WITH(channel_name, "beta")
      THEN "beta"
    ELSE "Other"
  END
);

-- Tests
SELECT
  assert.equals(norm.app_channel("release"), "release"),
  assert.equals(norm.app_channel("Release"), "Other"),
  assert.equals(norm.app_channel("nightly"), "nightly"),
  assert.equals(norm.app_channel("nightly-cck-1"), "nightly"),
  assert.equals(norm.app_channel("beta1"), "beta"),
  assert.equals(norm.app_channel(""), "Other"),
