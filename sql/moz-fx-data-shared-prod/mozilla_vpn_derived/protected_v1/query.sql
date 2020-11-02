-- WARNING: on mobile this is undercounted and may not be measurable
WITH base AS (
  SELECT
    TO_HEX(SHA256(jsonPayload.fields.fxa_uid)) AS fxa_uid,
    MIN(`timestamp`) AS first_protected,
  FROM
    `moz-fx-guardian-prod-bfc7`.log_storage.stdout
  WHERE
    jsonPayload.fields.isprotected
    AND DATE(`timestamp`) = @date
  GROUP BY
    fxa_uid
  UNION ALL
  SELECT
    *
  FROM
    protected_v1
)
SELECT
  fxa_uid,
  MIN(first_protected) AS first_protected,
FROM
  base
GROUP BY
  (fxa_uid)
