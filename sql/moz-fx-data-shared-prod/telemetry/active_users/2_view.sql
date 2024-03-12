CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.active_users_view`
AS
SELECT
    submission_date,
    app_name,
    days_active_user_bits,
    udf.pos_of_trailing_set_bit(days_active_user_bits) = 1 AS is_dau,
    udf.pos_of_trailing_set_bit(days_active_user_bits) <= 7 AS is_wau,
    udf.pos_of_trailing_set_bit(days_active_user_bits) <= 28 AS is_mau,
    True AS is_desktop,
    False AS is_mobile
FROM
`moz-fx-data-shared-prod.telemetry.active_users_mobile_v1`
--  TODO: UNION ALL Desktop.
