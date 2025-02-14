--code for first run
MERGE INTO `moz-fx-data-shared-prod.telemetry_derived.fx_accounts_linked_clients_v2`
USING ( 
with all_new_combos AS (
  SELECT DISTINCT 
  metrics.uuid.client_association_legacy_client_id AS legacy_telemetry_client_id,
  metrics.string.client_association_uid AS account_id,
  date(submission_timestamp)
  FROM `moz-fx-data-shared-prod.firefox_desktop.fx_accounts`
  WHERE metrics.uuid.client_association_legacy_client_id is not null 
  and metrics.string.client_association_uid is not null
  {% if is_init() %}
  and date(submission_timestamp) <= @submission_date
  {% else %}
  and date(submission_timestamp) = @submission_date
  {% endif %}
) ,

linked_clients_level_1 AS (
  SELECT DISTINCT 
  a.legacy_telemetry_client_id AS client_id,
  b.legacy_telemetry_client_id AS linked_client_id,
  @submission_date AS linkage_first_seen_date,
  @submission_date AS linkage_last_seen_date
  FROM all_new_combos a
  LEFT JOIN 
  all_new_combos b
  ON a.account_id = b.account_id
  AND a.legacy_telemetry_client_id <> b.legacy_telemetry_client_id
  AND a.legacy_telemetry_client_id < b.legacy_telemetry_client_id

),

linked_clients_level_2 AS (

)

--code for all other runs









USING  (

) AS s 
ON T.client_id = S.client_id
AND T.linked_client_id = S.linked_client_id

WHEN NOT MATCHED BY TARGET 
THEN INSERT 
(


)
WHEN MATCHED
THEN UPDATE SET 
T.? = S.?,
T.? = S.?,
T.? = S.?,
T.? = S.?
