-- UDF that returns TRUE when a Zendesk ticket's aggregated tag set contains any of the
-- automation / autosolve / experiment-macro tags maintained by the Customer Experience team.
-- Mirrors the `automation_class` CTE that aggregates `moz-fx-data-shared-prod.zendesk_syndicate.ticket_tag`
-- per ticket_id with `MAX(CASE WHEN tag IN (...) THEN 1 ELSE 0 END)`.
-- Returns:
--   TRUE  when at least one automation tag is present in the input array.
--   FALSE when tags exist but none are in the automation set (including the empty array).
--   NULL  when the input array itself is NULL (input data missing) — distinct from an
--         empty tag set, which evaluates to FALSE.
-- Note from the Customer Experience team:
-- The tag list captures Self-Service Automation (SSA) flows, Appbot autosolve, loginless
-- autosolve, and the SSA experiment macro / star variants. Tickets matching any of these
-- were resolved or rated through automation rather than agent handling.
CREATE OR REPLACE FUNCTION customer_experience.is_automated(tags ARRAY<STRING>)
RETURNS BOOL AS (
  IF(
    tags IS NULL,
    NULL,
    (
      SELECT
        IFNULL(
          LOGICAL_OR(
            tag IN (
              'ssa-sign-in-failure-automation',
              'ssa-connection-issues-automation',
              'ssa-sync-data-automation',
              'appbot-autosolve',
              'ssa-experiment-2fa-automation',
              'ssa-experiment-pwrdreset-automation',
              'ssa-experiment-emailverify-automation',
              'ssa-experiment-2fa-macro',
              'ssa-experiment-pwrdreset-macro',
              'ssa-experiment-emailverify-macro',
              'ssa-experiment-connectionissues-macro',
              'ssa-experiment-changeemail-macro',
              'ssa-experiment-sign-in-failure-macro',
              'ssa-experiment-sync-data-macro',
              'ssa-experiment-4-star',
              'ssa-experiment-5-star',
              'loginless-autosolve'
            )
          ),
          FALSE
        )
      FROM
        UNNEST(tags) AS tag
    )
  )
);

-- Tests
SELECT
  -- TRUE when any single automation tag is present
  assert.true(customer_experience.is_automated(['ssa-sign-in-failure-automation'])),
  assert.true(customer_experience.is_automated(['appbot-autosolve'])),
  assert.true(customer_experience.is_automated(['loginless-autosolve'])),
  assert.true(customer_experience.is_automated(['ssa-experiment-2fa-macro'])),
  assert.true(customer_experience.is_automated(['ssa-experiment-4-star'])),
  assert.true(customer_experience.is_automated(['ssa-experiment-5-star'])),
  -- TRUE when an automation tag is mixed with unrelated tags
  assert.true(customer_experience.is_automated(['bot', 'english', 'appbot-autosolve'])),
  assert.true(
    customer_experience.is_automated(
      ['ssa-sync-data-automation', 'ssa-experiment-changeemail-macro']
    )
  ),
  -- FALSE when no automation tag is present
  assert.false(customer_experience.is_automated(['bot', 'english'])),
  assert.false(customer_experience.is_automated(['spanish'])),
  assert.false(customer_experience.is_automated(['automation', 'autosolve'])),
  -- Empty array → FALSE (no automation tags is a known answer, not missing data)
  assert.false(customer_experience.is_automated([])),
  -- NULL elements inside the array (LOGICAL_OR skips NULL comparisons; IFNULL pins to FALSE)
  assert.false(customer_experience.is_automated([CAST(NULL AS STRING)])),
  assert.true(customer_experience.is_automated(['appbot-autosolve', CAST(NULL AS STRING)])),
  -- NULL input → NULL (input data missing)
  assert.null(customer_experience.is_automated(CAST(NULL AS ARRAY<STRING>)))
