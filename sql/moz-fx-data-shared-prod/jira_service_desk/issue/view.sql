CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.jira_service_desk.issue`
AS
WITH requests AS (
  -- Join requests related data
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.jira_service_desk_syndicate.request` AS request
  LEFT JOIN
    `moz-fx-data-shared-prod.jira_service_desk_syndicate.request_type` AS request_type
    ON request.request_type_id = request_type.id
),
fields AS (
  -- Join fields related data
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.jira_service_desk_syndicate.field` AS field
  LEFT JOIN
    `moz-fx-data-shared-prod.jira_service_desk_syndicate.issue_field_history` AS field_history
    ON field_history.field_id = field.id
  LEFT JOIN
    `moz-fx-data-shared-prod.jira_service_desk_syndicate.field_option` AS field_option
    ON field.id = "customfield_" || field_option.id
)
-- Join requests and fields data with issues
SELECT
  *
FROM
  `moz-fx-data-shared-prod.jira_service_desk_syndicate.issue` AS issue
LEFT JOIN
  requests
  ON issue.id = requests.issue_id
LEFT JOIN
  fields
  ON issue.id = fields.issue_id
