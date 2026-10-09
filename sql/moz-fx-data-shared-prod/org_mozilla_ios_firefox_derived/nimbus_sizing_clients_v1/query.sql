-- Reads the dedicated nimbus-targeting-context ping, not the sparse context
-- object carried on the general metrics ping.
--
-- The ping covers the whole population and carries sample_id, the standard
-- uniform 0-99 population hash. The table this replaces had no sample column
-- at all, which is why it hand-rolled ABS(MOD(FARM_FINGERPRINT(client_id),
-- 100)) -- and why the x10 scale-up was wrong there: it corrected for that
-- hash while the source was already a small, non-uniform slice of clients.
-- Here `sample_id < 10` is a true 10% of the population, matching desktop,
-- so the x10 applied downstream is correct.
--
-- The context object is kept verbatim rather than flattened into typed
-- columns: Experimenter owns the JEXL-to-SQL mapping and emits
-- JSON_VALUE(context, ...) lookups, so adding a targeting attribute is an
-- Experimenter-only change with no pool schema migration here.
--
-- normalized_country_code is carried because Experimenter's `region` mapping
-- falls back to it; the context only records region on some builds.
SELECT
  client_info.client_id AS client_id,
  DATE(submission_timestamp) AS submission_date,
  normalized_channel,
  normalized_country_code,
  metrics.object.nimbus_system_recorded_nimbus_context AS context
FROM
  `moz-fx-data-shared-prod.firefox_ios.nimbus_targeting_context`
WHERE
  DATE(submission_timestamp)
  BETWEEN DATE_SUB(@submission_date, INTERVAL 6 DAY)
  AND @submission_date
  AND sample_id < 10
  AND client_info.client_id IS NOT NULL
  AND metrics.object.nimbus_system_recorded_nimbus_context IS NOT NULL
QUALIFY
  ROW_NUMBER() OVER (PARTITION BY client_info.client_id ORDER BY submission_timestamp DESC) = 1
