-- WARNING: on mobile this is undercounted and may not be measurable
WITH base AS (
  SELECT
    TO_HEX(SHA256(jsonPayload.fields.fxa_uid)) AS fxa_uid,
    MIN(`timestamp`) AS first_protected,
  FROM
    `moz-fx-guardian-prod-bfc7`.log_storage.stdout
  WHERE
    jsonPayload.fields.isprotected
    {% if not is_init() %}
      AND DATE(`timestamp`) = @date
    {% endif %}
  GROUP BY
    fxa_uid
  UNION ALL
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.mozilla_vpn_derived.protected_v1`
)
SELECT
  fxa_uid,
  MIN(first_protected) AS first_protected,
FROM
  base
GROUP BY
  (fxa_uid)
