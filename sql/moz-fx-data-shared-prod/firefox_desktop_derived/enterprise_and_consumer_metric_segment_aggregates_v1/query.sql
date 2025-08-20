WITH client_baseline AS (
  SELECT
    client_id,
    sample_id,
    normalized_channel,
    distribution_id,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.baseline_active_users`
  WHERE
    submission_date = @submission_date
    -- TODO: should the base include clients that sent us a baseline ping or were marked as "active"?
    AND is_daily_user
),
client_metrics AS (
  SELECT
    client_info.client_id,
    sample_id,
    normalized_channel,
    MAX_BY(
      metrics.metrics.boolean.policies_is_enterprise,
      submission_timestamp
    ) AS current_policies_is_enterprise,
    MAX_BY(metrics.metrics.quantity.policies_count, submission_timestamp) AS current_policies_count,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.metrics`
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    ALL
),
-- TODO: understand what is the purpose of this
last_known AS (
  SELECT
    client_id,
    sample_id,
    normalized_channel,
		-- MAX_BY(classification_policies_count, submission_date) AS classification_policies_count,
		-- MAX_BY(classification_policies_is_enterprise, submission_date) AS classification_policies_is_enterprise,
		-- MAX_BY(classification_distribution_id, submission_date) AS classification_distribution_id
  -- TODO: if each partition contains a full list of clients could we not just grab the most recent partition instead of the entire table here?
    classification_policies_count,
    classification_policies_is_enterprise,
    classification_distribution_id,
	-- FROM <new_table>
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_derived.enterprise_and_consumer_metric_segment_aggregates_v1`
  WHERE
    submission_date = @submission_date
	-- GROUP BY ALL
),
joined AS (
  SELECT
    @submission_date AS submission_date,
    client_id,
    sample_id,
    normalized_channel,
    client_metrics.current_policies_count,
    client_metrics.current_policies_is_enterprise,
    client_baseline.distribution_id,
    COALESCE(
      client_metrics.current_policies_count,
      last_known.classification_policies_count
    ) AS classification_policies_count,
    COALESCE(
      client_metrics.current_policies_is_enterprise,
      last_known.classification_policies_is_enterprise
    ) AS classification_policies_is_enterprise,
    COALESCE(
      client_baseline.distribution_id,
      last_known.classification_distribution_id
    ) AS classification_distribution_id
  FROM
    client_baseline
  LEFT JOIN
    client_metrics
    USING (client_id, sample_id, normalized_channel)
-- TODO: shouldn't this be a full outer join?
  LEFT JOIN
    last_known
    USING (client_id, sample_id, normalized_channel)
)
SELECT
  *,
  -- TODO: is this classification expected to change for clients? Should it be flexible or should the value be stored in the table?
  CASE
    WHEN normalized_channel = "release"
      AND ((classification_policies_count > 1) OR (classification_policies_is_enterprise = TRUE))
      THEN "enterprise_release"
    WHEN normalized_channel = "release"
      AND (
        (classification_distribution_id IS NOT NULL)
        OR (classification_policies_count = 0)
        OR (classification_policies_is_enterprise = FALSE)
      )
      THEN "consumer_release"
    WHEN normalized_channel = "esr"
      AND ((classification_policies_count > 1) AND (classification_distribution_id IS NULL))
      THEN "enterprise_esr"
    WHEN normalized_channel = "esr"
      AND (
        (classification_distribution_id IS NOT NULL)
        OR (classification_policies_count = 0)
        OR (classification_policies_is_enterprise = FALSE)
      )
      THEN "consumer_esr"
    WHEN normalized_channel = "release"
      THEN "unknown_release"
    WHEN normalized_channel = "esr"
      THEN "unknown_esr"
    ELSE ERROR("This should not happen.") -- TODO: what should not happen?
  END AS enterprise_classification,
FROM
  joined
