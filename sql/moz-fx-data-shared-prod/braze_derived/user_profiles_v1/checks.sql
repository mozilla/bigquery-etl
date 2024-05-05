-- raw sql checks
ASSERT(SELECT COUNT(*) FROM `moz-fx-data-shared-prod.braze_derived.users_v1`) = (
  SELECT
    COUNT(*)
  FROM
    `moz-fx-data-shared-prod.braze_derived.user_profiles_v1`
);

-- macro checks

#fail
{{ not_null(["external_id", "email", "email_subscribe", "has_fxa", "create_timestamp", "update_timestamp"]) }}

#fail
{{ min_row_count(85000000) }}

#fail
{{ is_unique(["external_id", "email", "fxa_id_sha256"]) }}
