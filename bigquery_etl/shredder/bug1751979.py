#!/usr/bin/env python3

"""Constants for use in sanitizing main_v4 for bug 1751979."""

BUG_1751979_MAIN_V4_UDFS = r"""
CREATE TEMP FUNCTION sanitize_search_counts(input ANY TYPE) AS (
  (
    WITH base AS (
      SELECT
        key,
        value,
        REGEXP_EXTRACT(key, "([^.]+[.]in-content[:.][^:]+:).*") AS prefix,
        REGEXP_EXTRACT(key, "[^.]+[.]in-content[:.][^:]+:(.*)") AS code,
      FROM
        UNNEST(input)
    )
    SELECT
      ARRAY_AGG(
        STRUCT(
          IF(
            prefix IS NULL
            OR code IN (
              "none",
              "other",
              "hz",
              "h_",
              "MOZ2",
              "MOZ4",
              "MOZ5",
              "MOZA",
              "MOZB",
              "MOZD",
              "MOZE",
              "MOZI",
              "MOZM",
              "MOZO",
              "MOZT",
              "MOZW",
              "MOZSL01",
              "MOZSL02",
              "MOZSL03",
              "firefox-a",
              "firefox-b",
              "firefox-b-1",
              "firefox-b-ab",
              "firefox-b-1-ab",
              "firefox-b-d",
              "firefox-b-1-d",
              "firefox-b-e",
              "firefox-b-1-e",
              "firefox-b-m",
              "firefox-b-1-m",
              "firefox-b-o",
              "firefox-b-1-o",
              "firefox-b-lm",
              "firefox-b-1-lm",
              "firefox-b-lg",
              "firefox-b-huawei-h1611",
              "firefox-b-is-oem1",
              "firefox-b-oem1",
              "firefox-b-oem2",
              "firefox-b-tinno",
              "firefox-b-pn-wt",
              "firefox-b-pn-wt-us",
              "ubuntu",
              "ffab",
              "ffcm",
              "ffhp",
              "ffip",
              "ffit",
              "ffnt",
              "ffocus",
              "ffos",
              "ffsb",
              "fpas",
              "fpsa",
              "ftas",
              "ftsa",
              "newext",
              "monline_dg",
              "monline_3_dg",
              "monline_4_dg",
              "monline_7_dg"
            ),
            key,
            CONCAT(prefix, "other.scrubbed")
          ) AS key,
          value
        )
      )
    FROM
      base
  )
);

CREATE TEMP FUNCTION sanitize_scalar(input ANY TYPE) AS (
  (
    WITH base AS (
      SELECT
        key,
        value,
        REGEXP_EXTRACT(key, "([^:]+:[^:]+:).*") AS prefix,
        REGEXP_EXTRACT(key, "[^:]+:[^:]+:(.*)") AS code,
      FROM
        UNNEST(input)
    )
    SELECT
      ARRAY_AGG(
        STRUCT(
          IF(
            prefix IS NULL
            OR code IN (
              "none",
              "other",
              "hz",
              "h_",
              "MOZ2",
              "MOZ4",
              "MOZ5",
              "MOZA",
              "MOZB",
              "MOZD",
              "MOZE",
              "MOZI",
              "MOZM",
              "MOZO",
              "MOZT",
              "MOZW",
              "MOZSL01",
              "MOZSL02",
              "MOZSL03",
              "firefox-a",
              "firefox-b",
              "firefox-b-1",
              "firefox-b-ab",
              "firefox-b-1-ab",
              "firefox-b-d",
              "firefox-b-1-d",
              "firefox-b-e",
              "firefox-b-1-e",
              "firefox-b-m",
              "firefox-b-1-m",
              "firefox-b-o",
              "firefox-b-1-o",
              "firefox-b-lm",
              "firefox-b-1-lm",
              "firefox-b-lg",
              "firefox-b-huawei-h1611",
              "firefox-b-is-oem1",
              "firefox-b-oem1",
              "firefox-b-oem2",
              "firefox-b-tinno",
              "firefox-b-pn-wt",
              "firefox-b-pn-wt-us",
              "ubuntu",
              "ffab",
              "ffcm",
              "ffhp",
              "ffip",
              "ffit",
              "ffnt",
              "ffocus",
              "ffos",
              "ffsb",
              "fpas",
              "fpsa",
              "ftas",
              "ftsa",
              "newext",
              "monline_dg",
              "monline_3_dg",
              "monline_4_dg",
              "monline_7_dg"
            ),
            key,
            CONCAT(prefix, "other.scrubbed")
          ) AS key,
          value
        )
      )
    FROM
      base
  )
);
"""

BUG_1751979_MAIN_V4_REPLACE_CLAUSE = r"""
REPLACE (
(
  SELECT AS STRUCT
    payload.* REPLACE (
      (
        SELECT AS STRUCT
          payload.keyed_histograms.* REPLACE (
            sanitize_search_counts(payload.keyed_histograms.search_counts) AS search_counts
          )
      ) AS keyed_histograms,
      (
        SELECT AS STRUCT
          payload.processes.* REPLACE (
            (
              SELECT AS STRUCT
                payload.processes.parent.* REPLACE (
                  (
                    SELECT AS STRUCT
                      payload.processes.parent.keyed_scalars.* REPLACE (
  sanitize_scalar(
    payload.processes.parent.keyed_scalars.browser_search_content_urlbar
  ) AS browser_search_content_urlbar,
  sanitize_scalar(
    payload.processes.parent.keyed_scalars.browser_search_content_urlbar_handoff
  ) AS browser_search_content_urlbar_handoff,
  sanitize_scalar(
    payload.processes.parent.keyed_scalars.browser_search_content_urlbar_searchmode
  ) AS browser_search_content_urlbar_searchmode,
  sanitize_scalar(
    payload.processes.parent.keyed_scalars.browser_search_content_searchbar
  ) AS browser_search_content_searchbar,
  sanitize_scalar(
    payload.processes.parent.keyed_scalars.browser_search_content_about_home
  ) AS browser_search_content_about_home,
  sanitize_scalar(
    payload.processes.parent.keyed_scalars.browser_search_content_about_newtab
  ) AS browser_search_content_about_newtab,
  sanitize_scalar(
    payload.processes.parent.keyed_scalars.browser_search_content_contextmenu
  ) AS browser_search_content_contextmenu,
  sanitize_scalar(
    payload.processes.parent.keyed_scalars.browser_search_content_webextension
  ) AS browser_search_content_webextension,
  sanitize_scalar(
    payload.processes.parent.keyed_scalars.browser_search_content_system
  ) AS browser_search_content_system,
  sanitize_scalar(
    payload.processes.parent.keyed_scalars.browser_search_content_tabhistory
  ) AS browser_search_content_tabhistory,
  sanitize_scalar(
    payload.processes.parent.keyed_scalars.browser_search_content_reload
  ) AS browser_search_content_reload,
  sanitize_scalar(
    payload.processes.parent.keyed_scalars.browser_search_content_unknown
  ) AS browser_search_content_unknown
                      )
                  ) AS keyed_scalars
                )
            ) AS parent
          )
      ) AS processes
    )
) AS payload)
"""

BUG_1751979_MAIN_SUMMARY_V4_UDFS = r"""
CREATE TEMP FUNCTION sanitize_search_counts_ms(
  input ARRAY<STRUCT<engine STRING, source STRING, count INT64>>
) AS (
  ARRAY(
    WITH base AS (
      SELECT
        -- This reverses the separation logic in main_summary where
        -- engine and source are parsed from the search_counts key
        CONCAT(engine, '.', source) AS key,
        `count`
      FROM
        UNNEST(input)
    ),
    parsed AS (
      SELECT
        *,
        REGEXP_EXTRACT(key, "([^.]+[.]in-content[:.][^:]+:).*") AS prefix,
        REGEXP_EXTRACT(key, "[^.]+[.]in-content[:.][^:]+:(.*)") AS code,
      FROM
        base
    ),
    scrubbed AS (
      SELECT
        * REPLACE (
          IF(
            prefix IS NULL
            OR code IN (
              "none",
              "other",
              "hz",
              "h_",
              "MOZ2",
              "MOZ4",
              "MOZ5",
              "MOZA",
              "MOZB",
              "MOZD",
              "MOZE",
              "MOZI",
              "MOZM",
              "MOZO",
              "MOZT",
              "MOZW",
              "MOZSL01",
              "MOZSL02",
              "MOZSL03",
              "firefox-a",
              "firefox-b",
              "firefox-b-1",
              "firefox-b-ab",
              "firefox-b-1-ab",
              "firefox-b-d",
              "firefox-b-1-d",
              "firefox-b-e",
              "firefox-b-1-e",
              "firefox-b-m",
              "firefox-b-1-m",
              "firefox-b-o",
              "firefox-b-1-o",
              "firefox-b-lm",
              "firefox-b-1-lm",
              "firefox-b-lg",
              "firefox-b-huawei-h1611",
              "firefox-b-is-oem1",
              "firefox-b-oem1",
              "firefox-b-oem2",
              "firefox-b-tinno",
              "firefox-b-pn-wt",
              "firefox-b-pn-wt-us",
              "ubuntu",
              "ffab",
              "ffcm",
              "ffhp",
              "ffip",
              "ffit",
              "ffnt",
              "ffocus",
              "ffos",
              "ffsb",
              "fpas",
              "fpsa",
              "ftas",
              "ftsa",
              "newext",
              "monline_dg",
              "monline_3_dg",
              "monline_4_dg",
              "monline_7_dg"
            ),
            key,
            CONCAT(prefix, "other.scrubbed")
          ) AS key
        )
      FROM
        parsed
    )
    -- The SUBSTR and UNNEST logic here is copied from the prod main_summary query
    -- see https://github.com/mozilla/bigquery-etl/blob/222c4266/sql
    --     /moz-fx-data-shared-prod/telemetry_derived/main_summary_v4/part1.sql#L266-L273
    SELECT AS STRUCT
      SUBSTR(_key, 0, pos - 2) AS engine,
      SUBSTR(_key, pos) AS source,
      -- We sum by engine and source just in case we have several non-conforming codes that
      -- all get the same scrubbed value; we want to avoid having duplicate keys.
      SUM(`count`) AS `count`
    FROM
      parsed,
      UNNEST([REPLACE(key, 'in-content.', 'in-content:')]) AS _key,
      UNNEST([LENGTH(REGEXP_EXTRACT(_key, '.+?[.].'))]) AS pos
    GROUP BY
      engine,
      source
  )
);
"""

BUG_1751979_MAIN_SUMMARY_V4_REPLACE_CLAUSE = r"""
  REPLACE (sanitize_search_counts_ms(search_counts) AS search_counts)
"""
