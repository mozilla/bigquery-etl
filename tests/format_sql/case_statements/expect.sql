SELECT
  -- Multi-line WHEN conditions
  CASE
    WHEN bool_true > 0
      AND bool_false = 0
      THEN "always"
    WHEN bool_true = 0
      AND bool_false > 0
      THEN "never"
    ELSE "sometimes"
  END AS case_1,
  -- Multi-line WHEN condition, multi-line THEN value
  CASE
    WHEN legacy_app_name LIKE "Firefox"
      AND normalized_os LIKE "%"
      THEN STRUCT(
          "firefox_desktop" AS app_name,
          "Firefox" AS product,
          "Firefox for Desktop" AS canonical_app_name,
          "Firefox for Desktop" AS canonical_name
        )
  END AS case_2,
  -- CASE...END followed by comma
  IF(
    object = "bookmarks-panel",
    CASE
      WHEN string_value = "app-menu"
        THEN ("app-menu", value)
      WHEN string_value LIKE "home-panel%"
        THEN ("home-panel", value)
    END,
    NULL
  ) AS case_3,
  -- CASE...END followed by field access operator
  CASE
    WHEN flag
      THEN STRUCT(1 AS case_4a, 2 AS case_4b)
    ELSE STRUCT(3 AS case_4a, 4 AS case_4b)
  END.*,
  -- CASE <expression> syntax
  CASE
    status
    WHEN 1
      THEN "active"
    WHEN 2
      THEN "expired"
    ELSE "unknown"
  END AS case_5,
