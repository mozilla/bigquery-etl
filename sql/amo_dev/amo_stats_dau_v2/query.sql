/*

Reduced stats table for dev and stage versions of the AMO service.

*/
--
SELECT
  *
FROM
  amo_prod.amo_stats_dau_v2
WHERE
  submission_date = @submission_date
  AND (
    addon_id IN (
      '{db55bb9b-0d9f-407f-9b65-da9dd29c8d32}', -- :willdurand
      '{7e7eda8f-2e5d-4f43-86a9-07c6139e7a08}', -- :mat
      'close-tabs-by-pattern@virgule.net',      -- :mat
      '{46607a7b-1b2a-40ce-9afe-91cda52c46a6}', -- theme owned by :scolville
      '{0ec56aba-6955-43fb-a5cf-ed3f3ab66e7e}', -- theme owned by :caitmuenster
      '@contain-facebook',
      '@testpilot-containers',
      'FirefoxColor@mozilla.com',
      'private-relay@firefox.com',
      'notes@mozilla.com',
      '1b2383b324c8520974ee097e46301d5ca4e076de387c02886f1c6b1503671586@pokeinthe.io' -- Laboratory
    )
    OR addon_id LIKE 'langpack-%@firefox.mozilla.org'
  )
