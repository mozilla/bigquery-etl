CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.jira_service_desk.issue`
AS
WITH requests AS (
  -- Join requests related data
  SELECT
    * EXCEPT (id, description),
    id AS request_id,
    description AS request_description
  FROM
    (
      SELECT
        * EXCEPT (_fivetran_synced)
      FROM
        `moz-fx-data-shared-prod.jira_service_desk_syndicate.request`
    ) AS request
  LEFT JOIN
    (
      SELECT
        * EXCEPT (_fivetran_synced, service_desk_id)
      FROM
        `moz-fx-data-shared-prod.jira_service_desk_syndicate.request_type`
    ) AS request_type
    ON request.request_type_id = request_type.id
),
fields AS (
  -- Join fields related data
  SELECT
    * EXCEPT (id, field_id, description),
    id AS field_id,
    description AS field_description
  FROM
    (
      SELECT
        * EXCEPT (_fivetran_synced, _fivetran_deleted, name),
        name AS field_name
      FROM
        `moz-fx-data-shared-prod.jira_service_desk_syndicate.field`
    ) AS field
  LEFT JOIN
    (
      SELECT
        * EXCEPT (_fivetran_synced)
      FROM
        `moz-fx-data-shared-prod.jira_service_desk_syndicate.issue_field_history`
    ) AS field_history
    ON field_history.field_id = field.id
  LEFT JOIN
    (
      SELECT
        * EXCEPT (_fivetran_synced, id, name),
        id AS field_option_id,
        name AS field_option_name
      FROM
        `moz-fx-data-shared-prod.jira_service_desk_syndicate.field_option`
    ) AS field_option
    ON field_history.value = SAFE_CAST(field_option.field_option_id AS STRING)
)
-- Join requests and fields data with issues
SELECT
  * EXCEPT (issue_id, parent_id)
FROM
  `moz-fx-data-shared-prod.jira_service_desk_syndicate.issue` AS issue
LEFT JOIN
  requests
  ON issue.id = requests.issue_id
LEFT JOIN
  fields
  ON issue.id = fields.issue_id
