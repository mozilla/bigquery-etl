SELECT
  users_attribution.user_id,
  users.created_at AS user_created_at,
  users.fxa_uid,
  users_attribution.* EXCEPT (user_id)
FROM
  `moz-fx-data-shared-prod.mozilla_vpn_derived.users_attribution_v1` AS users_attribution
LEFT JOIN
  `moz-fx-data-shared-prod.mozilla_vpn_derived.users_v1` AS users
  ON users_attribution.user_id = users.id
