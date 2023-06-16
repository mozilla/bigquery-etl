CREATE OR REPLACE FUNCTION norm.firefox_android_package_name_to_channel(package_name STRING)
RETURNS STRING AS (
  CASE
    package_name
    WHEN "org.mozilla.firefox"
      THEN "release"
    WHEN "org.mozilla.firefox_beta"
      THEN "beta"
    WHEN "org.mozilla.fenix"
      THEN "nightly"
    ELSE NULL
  END
);

SELECT
  assert.equals(norm.firefox_android_package_name_to_channel("org.mozilla.firefox"), "release"),
  assert.equals(norm.firefox_android_package_name_to_channel("org.mozilla.firefox_beta"), "beta"),
  assert.equals(norm.firefox_android_package_name_to_channel("org.mozilla.fenix"), "nightly"),
  assert.null(norm.firefox_android_package_name_to_channel("org.mozilla.other")),
