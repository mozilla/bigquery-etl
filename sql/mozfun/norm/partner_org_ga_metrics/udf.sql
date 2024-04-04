CREATE OR REPLACE FUNCTION norm.partner_org_ga_metrics()
RETURNS STRING AS (
  (SELECT 'non-distribution' AS partner_org)
)
