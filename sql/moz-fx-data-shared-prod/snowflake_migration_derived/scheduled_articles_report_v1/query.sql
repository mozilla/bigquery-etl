WITH approved_ml AS (
  SELECT
    scheduled_corpus_item_scheduled_at,
    scheduled_surface_id,
    COUNT(*) AS total
  FROM
    `moz-fx-data-shared-prod.snowflake_migration_derived.corpus_item_schedules_updated_v1`
  WHERE
    (scheduled_corpus_status = 'added' OR scheduled_corpus_status = 'rescheduled')
    AND curator_created_by = 'ML'
    AND is_syndicated = FALSE
    AND is_collection = FALSE
    {% if is_init() %}
      AND DATE(happened_at) >= '2024-09-19'
    {% else %}
      AND DATE(happened_at) = @submission_date
    {% endif %}
  GROUP BY
    1,
    2
  ORDER BY
    1
),
dates_surfaces AS (
  SELECT DISTINCT
    scheduled_corpus_item_scheduled_at AS scheduled_date,
    scheduled_surface_id
  FROM
    approved_ml
  ORDER BY
    scheduled_corpus_item_scheduled_at ASC
),
removed_ml AS (
  SELECT
    scheduled_corpus_item_scheduled_at,
    scheduled_surface_id,
    COUNT(*) AS total
  FROM
    `moz-fx-data-shared-prod.snowflake_migration_derived.corpus_item_schedules_updated_v1`
  WHERE
    scheduled_corpus_status = 'removed'
    AND curator_created_by = 'ML'
    AND is_syndicated = FALSE
    AND is_collection = FALSE
    {% if is_init() %}
      AND DATE(happened_at) >= '2024-09-19'
    {% else %}
      AND DATE(happened_at) = @submission_date
    {% endif %}
    AND APPROVED_CORPUS_ITEM_EXTERNAL_ID NOT IN (
      SELECT
        APPROVED_CORPUS_ITEM_EXTERNAL_ID
      FROM
        `moz-fx-data-shared-prod.snowflake_migration_derived.corpus_items_updated_v1`
      WHERE
        reviewed_corpus_update_status = 'removed'
        {% if is_init() %}
          AND DATE(happened_at) >= '2024-09-19'
        {% else %}
          AND DATE(happened_at) = @submission_date
        {% endif %}
    )
  GROUP BY
    1,
    2
  ORDER BY
    1
),
rejected_ml AS (
  SELECT
    s.scheduled_corpus_item_scheduled_at,
    s.scheduled_surface_id,
    COUNT(*) AS total
  FROM
    `moz-fx-data-shared-prod.snowflake_migration_derived.corpus_item_schedules_updated_v1` s
  JOIN
    `moz-fx-data-shared-prod.snowflake_migration_derived.corpus_items_updated_v1` u
    ON s.APPROVED_CORPUS_ITEM_EXTERNAL_ID = u.APPROVED_CORPUS_ITEM_EXTERNAL_ID
    AND u.reviewed_corpus_update_status = 'removed'
    {% if is_init() %}
      AND DATE(u.happened_at) >= '2024-09-19'
    {% else %}
      AND DATE(u.happened_at) = @submission_date
    {% endif %}
  WHERE
    s.scheduled_corpus_status = 'removed'
    AND s.curator_created_by = 'ML'
    AND s.is_syndicated = FALSE
    AND s.is_collection = FALSE
    {% if is_init() %}
      AND DATE(happened_at) >= '2024-09-19'
    {% else %}
      AND DATE(happened_at) = @submission_date
    {% endif %}
  GROUP BY
    1,
    2
  ORDER BY
    1
),
manual_within_tool_ml AS (
  SELECT
    scheduled_corpus_item_scheduled_at,
    scheduled_surface_id,
    COUNT(*) AS total
  FROM
    `moz-fx-data-shared-prod.snowflake_migration_derived.corpus_item_schedules_updated_v1`
  WHERE
    scheduled_corpus_status = 'added'
    AND curator_created_by != 'ML'
    AND is_syndicated = FALSE
    AND is_collection = FALSE
    AND (
      scheduled_action_ui_page IN ('CORPUS', 'PROSPECTING')
      AND corpus_item_loaded_from != 'MANUAL'
    )
    AND (
      DATE(scheduled_corpus_item_scheduled_at) != DATE(scheduled_corpus_item_created_at)
      AND is_time_sensitive = FALSE
    )
    {% if is_init() %}
      AND DATE(happened_at) >= '2024-09-19'
    {% else %}
      AND DATE(happened_at) = @submission_date
    {% endif %}
  GROUP BY
    1,
    2
  ORDER BY
    1
),
manual_outside_tool_ml AS (
  SELECT
    scheduled_corpus_item_scheduled_at,
    scheduled_surface_id,
    COUNT(*) AS total
  FROM
    `moz-fx-data-shared-prod.snowflake_migration_derived.corpus_item_schedules_updated_v1`
  WHERE
    scheduled_corpus_status = 'added'
    AND curator_created_by != 'ML'
    AND is_syndicated = FALSE
    AND is_collection = FALSE
    AND (
      scheduled_action_ui_page = 'SCHEDULE'
      OR (
        scheduled_action_ui_page IN ('CORPUS', 'PROSPECTING')
        AND corpus_item_loaded_from = 'MANUAL'
      )
    )
    AND (
      DATE(scheduled_corpus_item_scheduled_at) != DATE(scheduled_corpus_item_created_at)
      AND is_time_sensitive = FALSE
    )
    {% if is_init() %}
      AND DATE(happened_at) >= '2024-09-19'
    {% else %}
      AND DATE(happened_at) = @submission_date
    {% endif %}
  GROUP BY
    1,
    2
  ORDER BY
    1
),
combined AS (
  SELECT
    d.scheduled_date,
    d.scheduled_surface_id,
    IFNULL(a.total, 0) AS approved_total,
    IFNULL(rej.total, 0) AS rejected_total,
    IFNULL(rem.total, 0) AS removed_total,
    IFNULL(mwt.total, 0) AS manual_within_tool_total,
    IFNULL(mot.total, 0) AS manual_outside_tool_total,
  FROM
    dates_surfaces d
  LEFT JOIN
    approved_ml a
    ON d.scheduled_date = a.scheduled_corpus_item_scheduled_at
    AND d.scheduled_surface_id = a.scheduled_surface_id
  LEFT JOIN
    rejected_ml rej
    ON d.scheduled_date = rej.scheduled_corpus_item_scheduled_at
    AND d.scheduled_surface_id = rej.scheduled_surface_id
  LEFT JOIN
    removed_ml rem
    ON d.scheduled_date = rem.scheduled_corpus_item_scheduled_at
    AND d.scheduled_surface_id = rem.scheduled_surface_id
  LEFT JOIN
    manual_within_tool_ml mwt
    ON d.scheduled_date = mwt.scheduled_corpus_item_scheduled_at
    AND d.scheduled_surface_id = mwt.scheduled_surface_id
  LEFT JOIN
    manual_outside_tool_ml mot
    ON d.scheduled_date = mot.scheduled_corpus_item_scheduled_at
    AND d.scheduled_surface_id = mot.scheduled_surface_id
  ORDER BY
    d.scheduled_date ASC
)
SELECT
  scheduled_date,
  scheduled_surface_id,
  approved_total,
  removed_total,
  rejected_total,
  manual_within_tool_total,
  manual_outside_tool_total,
  rejected_total / NULLIF(rejected_total + approved_total, 0) AS rejection_rate,
  removed_total / NULLIF(removed_total + approved_total, 0) AS removal_rate,
  manual_outside_tool_total / NULLIF(
    manual_outside_tool_total + approved_total,
    0
  ) AS manual_outside_rate,
  (rejected_total + removed_total + manual_outside_tool_total) / NULLIF(
    approved_total + removed_total + rejected_total + manual_within_tool_total + manual_outside_tool_total,
    0
  ) AS schedule_failure_rate
FROM
  combined
ORDER BY
  1
