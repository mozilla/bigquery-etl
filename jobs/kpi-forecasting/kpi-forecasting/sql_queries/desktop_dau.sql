WITH
    cd_dau AS (
        SELECT
            submission_date,
            100 * COUNTIF(scalar_parent_browser_engagement_total_uri_count_sum > 0 AND active_hours_sum > 0) AS uri_at_dau_cd
        FROM telemetry.clients_daily
        WHERE submission_date >= DATE(2017,1,1)
        AND sample_id = 0
        GROUP BY 1
        ORDER BY 1
    )
    ,

    main_clients AS (
        SELECT
         DATE(submission_timestamp) AS submission_date,
         client_id,
         COALESCE(SUM(payload.processes.parent.scalars.browser_engagement_total_uri_count_normal_and_private_mode), 0) AS normal_private,
         COALESCE(SUM(payload.processes.parent.scalars.browser_engagement_total_uri_count), 0) AS normal,
         COALESCE(SUM(payload.processes.parent.scalars.browser_engagement_active_ticks), 0) AS active_ticks,
        FROM telemetry.main
        WHERE DATE(submission_timestamp) >= DATE(2020,12,1)
        AND sample_id = 0
        GROUP BY 1, 2
    )
    ,

    main_dau AS (
        SELECT
            submission_date,
            100 * COUNTIF((normal > 0 OR normal_private > 0) and active_ticks > 0) AS uri_dau_either_at,
        FROM main_clients
        GROUP BY 1
    )

SELECT
    *
FROM cd_dau
LEFT JOIN main_dau
USING(submission_date)