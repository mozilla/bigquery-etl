SELECT
  *
FROM
  `moz-fx-data-shared-prod`.mozilla_vpn_derived.channel_group_proportions_live
WHERE
  IF(@date IS NULL, subscription_start_date < CURRENT_DATE - 7, subscription_start_date = @date)
