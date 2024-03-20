CREATE OR REPLACE TABLE
    `accounts_backend_derived.users_services_first_seen_v1`
PARTITION BY
    first_seen_date
CLUSTER BY
    user_id_sha256
AS
SELECT
    user_id as user_id_sha256,
    submission_date,
    service,
    did_register,
    first_service_country
FROM
    firefox_accounts_derived.fxa_users_services_first_seen_v2
WHERE
    submission_date = '2023-12-31'