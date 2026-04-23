CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.github_external.github_prs`
AS
SELECT
  repo_name,
  pr_number,
  title,
  body,
  author,
  html_url,
  created_at,
  updated_at,
  merged_at,
  base_branch,
  head_branch,
  additions,
  deletions,
  changed_files,
  commits,
  comments,
  review_comments,
  labels,
  requested_reviewers
FROM
  `moz-fx-data-shared-prod.github_external.bqetl_prs_v1`
UNION ALL
SELECT
  repo_name,
  pr_number,
  title,
  body,
  author,
  html_url,
  created_at,
  updated_at,
  merged_at,
  base_branch,
  head_branch,
  additions,
  deletions,
  changed_files,
  commits,
  comments,
  review_comments,
  labels,
  requested_reviewers
FROM
  `moz-fx-data-shared-prod.github_external.private_bqetl_prs_v1`
-- Once PR-9091 is approved, merged and tables backfilled, comment out section below to add looker repos to the view
-- Uncommenting required before backfilling the pr_embeddings table
--UNION ALL
--SELECT
--  repo_name,
--  pr_number,
--  title,
--  body,
--  author,
--  html_url,
--  created_at,
--  updated_at,
--  merged_at,
--  base_branch,
--  head_branch,
--  additions,
--  deletions,
--  changed_files,
--  commits,
--  comments,
--  review_comments,
--  labels,
--  requested_reviewers
--FROM
--  `moz-fx-data-shared-prod.github_external.lookml_generator_prs_v1`
--UNION ALL
--SELECT
--  repo_name,
--  pr_number,
--  title,
--  body,
--  author,
--  html_url,
--  created_at,
--  updated_at,
--  merged_at,
--  base_branch,
--  head_branch,
--  additions,
--  deletions,
--  changed_files,
--  commits,
--  comments,
--  review_comments,
--  labels,
--  requested_reviewers
--FROM
--  `moz-fx-data-shared-prod.github_external.looker_spoke_default_prs_v1`
--UNION ALL
--SELECT
--  repo_name,
--  pr_number,
--  title,
--  body,
--  author,
--  html_url,
--  created_at,
--  updated_at,
--  merged_at,
--  base_branch,
--  head_branch,
--  additions,
--  deletions,
--  changed_files,
--  commits,
--  comments,
--  review_comments,
--  labels,
--  requested_reviewers
--FROM
--  `moz-fx-data-shared-prod.github_external.looker_spoke_private_prs_v1`
