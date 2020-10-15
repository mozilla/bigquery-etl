-- WARNING: on mobile this is undercounted and may not be measurable
WITH _current AS (
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
)
SELECT
  fxa_uid,
  IF(
    _previous.first_protected IS NULL
    OR _previous.first_protected > _current.first_protected,
    _current,
    previous
  ).first_protected,
FROM
  protected_v1 AS _previous
FULL JOIN
  _current
USING
  (fxa_uid)
