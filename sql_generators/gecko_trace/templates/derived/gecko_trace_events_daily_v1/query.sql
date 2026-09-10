WITH {% include '_shared/event_ctes.sql' %}
SELECT
  @submission_date AS submission_date,
  event_hash AS event_signature,
  COUNT(*) AS hit_count
FROM
  span_event_hashes
GROUP BY
  event_signature
