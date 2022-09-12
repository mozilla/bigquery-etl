with query_logs as (
    SELECT
        TIMESTAMP_TRUNC(timestamp, DATE )AS merino_date,
        jsonPayload.fields.*
    FROM `suggest-searches-prod-a30f.logs.stdout`
    WHERE
        jsonPayload.type = "web.suggest.request"
        and jsonPayload.fields.session_id is not null
), terminal_queries as (
    select 
        array_agg(ql order by sequence_no desc limit 1)[offset(0)].*
    from query_logs ql
    group by ql.session_id
)
select
    merino_date,
    query as search_term,
    count(*) search_sessions
from terminal_queries
group by 1,2
-- Level 2 aggregation. See: https://wiki.mozilla.org/Data_Publishing
having count(*) > 5000
