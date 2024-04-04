CREATE OR REPLACE FUNCTION norm.funnel_derived_ga_metrics(
  device_category STRING,
  browser STRING,
  operating_system STRING
)
RETURNS STRING AS (
  CASE
    WHEN device_category != 'desktop'
      THEN 'mobile'
    WHEN browser IN ('Mozilla', 'Firefox')
      THEN 'existing user'
	-- note, the sessions and downloads
	-- should always be zero due to
	-- the measures being *_non_fx_*
	-- but useful if we change the structure
    WHEN browser NOT IN ('Mozilla', 'Firefox')
      AND operating_system = 'Windows'
      THEN 'mozorg windows funnel'
    WHEN browser NOT IN ('Mozilla', 'Firefox')
      AND operating_system = 'Macintosh'
      THEN 'mozorg mac funnel'
    ELSE 'mozorg other'
  END
);
