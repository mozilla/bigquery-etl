SELECT
  user_id,
  is_active,
  created_at,
  ended_at,
  updated_at,
  type,
  provider,
  IF(
    provider = "APPLE",
    mozfun.iap.scrub_apple_receipt(mozfun.iap.parse_apple_receipt(provider_receipt_json)),
    NULL
  ) AS apple_receipt,
FROM
  mozilla_vpn_external.subscriptions_v1
