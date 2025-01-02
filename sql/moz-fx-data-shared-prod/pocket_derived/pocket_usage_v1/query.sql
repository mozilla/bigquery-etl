with pocket_activity as (
    select
        DATE(submission_timestamp) AS events_submission_date,
        normalized_channel as events_channel,
        event_name,
        count(distinct client_info.client_id) as event_client_count,
    FROM
        moz-fx-data-shared-prod.fenix.events_unnested AS events
    WHERE
        DATE(submission_timestamp) = @submission_date
        AND event_category = 'pocket'
        AND metadata.isp.name != 'BrowserStack' -- Removal of known bots. It aims to filter out the data coming from the bots.
        AND ping_info.reason != 'startup' -- Removal of dirty pings
    group by 1,2,3
),

channel_users as (
    select
        DATE(submission_timestamp) AS submission_date,
        normalized_channel AS channel,
        COUNT(DISTINCT client_info.client_id) AS total_client_count,
    FROM
        fenix.baseline
    WHERE
        DATE(submission_timestamp) = @submission_date
        AND metadata.isp.name != 'BrowserStack' -- Removal of known bots. It aims to filter out the data coming from the bots.
        AND ping_info.reason != 'dirty_startup' -- Removal of dirty pings
    GROUP BY 1, 2
)

SELECT
    submission_date,
    channel,
    event_name,
    event_client_count,
    total_client_count,
    (event_client_count / total_client_count) * 100 AS incidence_percentage
FROM
    channel_users as a
LEFT JOIN pocket_activity as b on submission_date = events_submission_date AND channel = events_channel
order by 1,2,3
