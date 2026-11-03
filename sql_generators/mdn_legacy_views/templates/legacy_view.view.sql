-- Generated via ./bqetl generate mdn_legacy_views
CREATE OR REPLACE VIEW
  `{{ project_id }}.mdn.{{ view_name }}`
AS
WITH fred AS (
  {{ fred_query }}
)
SELECT
  {{ fred_select }}
FROM
  fred
UNION ALL
SELECT
  {{ yari_select }}
FROM
  `{{ project_id }}.mdn_yari.{{ yari_table }}`
