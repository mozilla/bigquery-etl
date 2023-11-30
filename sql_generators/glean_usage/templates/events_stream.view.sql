{{ header }}

CREATE OR REPLACE VIEW
  `{{ project_id }}.{{ events_stream_view }}`
AS
SELECT
  *
FROM
  `{{ project_id }}.{{ events_stream_table }}`
