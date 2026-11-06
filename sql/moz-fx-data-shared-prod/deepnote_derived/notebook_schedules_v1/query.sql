-- Expand a five-field cron expression into its weekly fire times (local day of week,
-- 0 = Sunday, and minute of day). Day-of-month and month fields are not expanded;
-- is_weekly is false for crons that restrict them (e.g. monthly schedules).
CREATE TEMP FUNCTION expand_cron(cron STRING)
RETURNS STRUCT<is_weekly BOOL, fire_times ARRAY<STRUCT<day_of_week INT64, minute_of_day INT64>>>
LANGUAGE js AS r"""
  const NAMES = {sun: 0, mon: 1, tue: 2, wed: 3, thu: 4, fri: 5, sat: 6};
  function parse(field, min, max) {
    const values = new Set();
    for (const part of field.toLowerCase().split(",")) {
      const [range, stepText] = part.split("/");
      const step = stepText ? parseInt(stepText, 10) : 1;
      let lo = min, hi = max;
      if (range !== "*") {
        const [a, b] = range.split("-").map(v => (v in NAMES ? NAMES[v] : parseInt(v, 10)));
        lo = a;
        hi = b === undefined ? (stepText ? max : a) : b;
      }
      for (let v = lo; v <= hi; v += step) values.add(v === 7 && max === 7 ? 0 : v);
    }
    return [...values];
  }
  const fields = cron.trim().split(/\s+/);
  if (fields.length !== 5) return null;
  const [minute, hour, dom, month, dow] = fields;
  const fireTimes = [];
  for (const d of parse(dow, 0, 7))
    for (const h of parse(hour, 0, 23))
      for (const m of parse(minute, 0, 59))
        fireTimes.push({day_of_week: d, minute_of_day: h * 60 + m});
  return {is_weekly: dom === "*" && month === "*", fire_times: fireTimes};
""";

WITH latest AS (
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.deepnote_derived.workspace_snapshot_v1`
  WHERE
    snapshot_date = (
      SELECT
        MAX(snapshot_date)
      FROM
        `moz-fx-data-shared-prod.deepnote_derived.workspace_snapshot_v1`
    )
),
schedules AS (
  SELECT
    latest.snapshot_date,
    latest.project_id,
    latest.project_name,
    notebook.notebook_id,
    notebook.name AS notebook_name,
    notebook.schedule.cron,
    notebook.schedule.timezone,
    notebook.schedule.next_run_at,
    IFNULL(notebook.schedule.is_paused, FALSE) AS is_paused,
    notebook.schedule.pause_reason,
    expand_cron(notebook.schedule.cron) AS expanded,
  FROM
    latest
  CROSS JOIN
    UNNEST(notebooks) AS notebook
  WHERE
    notebook.schedule.cron IS NOT NULL
)
SELECT
  snapshot_date,
  project_id,
  project_name,
  notebook_id,
  notebook_name,
  cron,
  timezone,
  next_run_at,
  is_paused,
  pause_reason,
  expanded.is_weekly,
  expanded.fire_times,
  ARRAY(
    SELECT AS STRUCT
      FORMAT_DATE('%A', DATE_ADD(DATE '2026-01-04', INTERVAL day_of_week DAY)) AS day_of_week,
      COUNT(*) AS runs,
    FROM
      UNNEST(expanded.fire_times)
    GROUP BY
      day_of_week
  ) AS runs_per_weekday,
FROM
  schedules
