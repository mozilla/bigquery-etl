#warn
WITH
current_partition_QDAU AS (
    SELECT SUM(qdau),
            FROM `{{ project_id }}.{{ dataset_id }}.{{ table_name }}`
            WHERE submission_date = @submission_date
),

upstream_QDAU_base AS (
        SELECT
        client_id,
        SUM(payload.processes.parent.scalars.browser_engagement_total_uri_count_normal_and_private_mode) AS scalar_parent_browser_engagement_total_uri_count_normal_and_private_mode_sum,
        SUM(payload.processes.parent.scalars.browser_engagement_total_uri_count) AS scalar_parent_browser_engagement_total_uri_count_sum,
        SUM(COALESCE(
            payload.processes.parent.scalars.browser_engagement_active_ticks,
            payload.simple_measurements.active_ticks
            )) AS active_ticks
        FROM
            `moz-fx-data-shared-prod.telemetry_live.main_v5`
        WHERE
        DATE(submission_timestamp) = @submission_date
        AND normalized_app_name = 'Firefox'
        AND document_id IS NOT NULL
        GROUP BY
        client_id
    ),
    overactive AS (
        SELECT
        client_id
        FROM
        upstream_QDAU_base
        GROUP BY
        client_id
        HAVING
        COUNT(*) > 150000
    ),
    client_summary AS (
        SELECT
        client_id,
        SUM(COALESCE(
            scalar_parent_browser_engagement_total_uri_count_normal_and_private_mode_sum,
            scalar_parent_browser_engagement_total_uri_count_sum
        )) AS total_uri_count,
        SUM(active_ticks / (3600 / 5)) AS active_hours_sum,
        FROM
        upstream_QDAU_base
        LEFT JOIN
        overactive
        USING
        (client_id)
        WHERE
        overactive.client_id IS NULL
        GROUP BY
        client_id
    ),
    last_seen AS (
        SELECT
        client_id,
        days_since_seen,
        FROM `moz-fx-data-shared-prod.telemetry.clients_last_seen`
        WHERE
        submission_date = @submission_date
    ),
    upstream_QDAU AS (
    SELECT
        COUNTIF(
        active_hours_sum > 0 AND total_uri_count > 0 AND days_since_seen = 0
        )
    FROM
        client_summary
    LEFT JOIN last_seen
        USING(client_id)
    )
    SELECT
        IF(
            (SELECT * FROM upstream_QDAU) <> (SELECT * FROM current_partition_QDAU),
            ERROR("QDAU mismatch between aggregates table and live table"),
            NULL
        );

#warn
WITH
dau_sum AS (
    SELECT SUM(dau),
            FROM `{{ project_id }}.{{ dataset_id }}.{{ table_name }}`
            WHERE submission_date = @submission_date
),

distinct_client_count_base AS (
              SELECT client_id
              FROM `moz-fx-data-shared-prod.telemetry_live.main_v5`
              WHERE
                DATE(submission_timestamp) = @submission_date
                AND normalized_app_name = 'Firefox'
                AND document_id IS NOT NULL
            ),
            overactive AS (
              SELECT client_id
              FROM distinct_client_count_base
              GROUP BY client_id
              HAVING COUNT(*) > 150000
            ),
            distinct_client_count AS (
            SELECT COUNT(DISTINCT client_id),
            FROM distinct_client_count_base
            FULL OUTER JOIN overactive USING(client_id)
            WHERE overactive.client_id IS NULL
            )
        SELECT
            IF(
                (SELECT * FROM dau_sum) <> (SELECT * FROM distinct_client_count),
                ERROR("DAU mismatch between aggregates table and live table"),
                NULL
            );
