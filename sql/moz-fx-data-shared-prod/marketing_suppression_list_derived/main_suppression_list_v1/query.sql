with
-- CTMS `has_opted_out_of_email` emails
ctms_suppressions as (
  select 1
),

-- Acoustic Suppression List
acoustic_suppressions AS (
  select 1
),

-- braze unsubscribes
braze_unsubscribes AS (
  select 1
),

-- braze hard bounces
braze_hard_bounces AS (
  select 1
),

suppression_list AS (
  SELECT
    email,
    external_id,
    source,
    reason
)
