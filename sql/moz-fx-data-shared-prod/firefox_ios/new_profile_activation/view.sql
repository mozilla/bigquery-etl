CREATE OR REPLACE VIEW
  firefox_ios.new_profile_activation
AS
SELECT
  *
FROM
  firefox_ios_derived.new_profile_activation_v1
