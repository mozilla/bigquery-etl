SELECT
  id,
  _fivetran_synced,
  amount_off,
  created,
  currency,
  duration,
  duration_in_months,
  is_deleted,
  max_redemptions,
  metadata,
  name,
  percent_off,
  redeem_by,
  times_redeemed,
  valid,
FROM
  `dev-fivetran`.stripe_nonprod.coupon
