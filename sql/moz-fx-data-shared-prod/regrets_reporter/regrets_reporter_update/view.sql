CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.regrets_reporter.regrets_reporter_update`
AS
SELECT
  * REPLACE (
    (
      SELECT AS STRUCT
        metadata.* REPLACE (
          (
            SELECT AS STRUCT
              metadata.header.*,
              SAFE.PARSE_TIMESTAMP(
                '%a, %d %b %Y %T %Z',
              -- Even though it's not part of the spec, many clients append
              -- '+00:00' to their Date headers, so we strip that suffix.
                REPLACE(metadata.header.`date`, 'GMT+00:00', 'GMT')
              ) AS parsed_date,
              ARRAY(
                SELECT
                  TRIM(t)
                FROM
                  UNNEST(SPLIT(metadata.header.x_source_tags, ',')) t
              ) AS parsed_x_source_tags
          ) AS header,
          -- Limit the geo info we present to the country level;
          -- https://bugzilla.mozilla.org/show_bug.cgi?id=1654078#c45
          (SELECT AS STRUCT metadata.geo.country) AS geo
        )
    ) AS metadata
  )
FROM
  `moz-fx-data-shared-prod.regrets_reporter_stable.regrets_reporter_update_v1`
