/*

Reduced stats table for dev and stage versions of the AMO service.

*/
--
SELECT
  *
FROM
  amo_prod.amo_stats_dau_v1
WHERE
  submission_date = @submission_date
  AND addon_id IN (
    '{db55bb9b-0d9f-407f-9b65-da9dd29c8d32}', -- :willdurand
    '{7e7eda8f-2e5d-4f43-86a9-07c6139e7a08}', -- :mat
    'close-tabs-by-pattern@virgule.net'       -- :mat
  )
