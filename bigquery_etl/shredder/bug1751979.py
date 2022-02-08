#!/usr/bin/env python3

"""Constants for use in sanitizing main_v4 for bug 1751979."""

BUG_1751979_MAIN_V4_UDFS = """
CREATE TEMP FUNCTION sanitize_search_counts(input ANY TYPE) AS (
  (
    WITH base AS (
      SELECT
        key,
        value,
        REGEXP_EXTRACT(key, "([^.]+\\.in-content[:.][^:]+:).*") AS prefix,
        REGEXP_EXTRACT(key, "[^.]+\\.in-content[:.][^:]+:(.*)") AS code,
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

BUG_1751979_MAIN_V4_REPLACE_CLAUSE = """
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
) AS payload
"""
