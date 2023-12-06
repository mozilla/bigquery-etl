{{ header }}

CREATE OR REPLACE VIEW
  `{{ project_id }}.{{ events_stream_view }}`
AS
SELECT
  * REPLACE(
    `mozfun.json.from_map`(ping_info.experiments) AS experiments,
    `mozfun.json.from_map`(event.extra) AS event_extra
  )
FROM
  `{{ project_id }}.{{ events_stream_table }}`
