# Claude Code Review - Testing Plan

## Prerequisites

### 1. Fork the Repository
```bash
# Fork via GitHub CLI
gh repo fork mozilla/bigquery-etl --clone=false

# Or fork via GitHub UI:
# Go to https://github.com/mozilla/bigquery-etl
# Click "Fork" button
```

### 2. Add ANTHROPIC_API_KEY Secret to Your Fork
1. Go to `https://github.com/YOUR_USERNAME/bigquery-etl/settings/secrets/actions`
2. Click "New repository secret"
3. Name: `ANTHROPIC_API_KEY`
4. Value: Your Anthropic API key from https://console.anthropic.com/
5. Click "Add secret"

### 3. Clone Your Fork
```bash
git clone https://github.com/YOUR_USERNAME/bigquery-etl.git
cd bigquery-etl

# Add upstream remote
git remote add upstream https://github.com/mozilla/bigquery-etl.git
```

### 4. Create Test Branch with Workflow
```bash
# Ensure you have the latest from upstream
git fetch upstream main
git checkout -b test-claude-review

# The workflow file should already exist at:
# .github/workflows/claude-review.yml

# Push to your fork
git add .github/workflows/claude-review.yml
git commit -m "Add Claude code review workflow"
git push origin test-claude-review
```

## Test Scenarios

### Test 1: Draft PR (Should NOT Trigger Review)

**Expected**: Claude should NOT review draft PRs

```bash
# Create a test SQL file
mkdir -p sql/test_project/test_dataset/test_table_v1
cat > sql/test_project/test_dataset/test_table_v1/query.sql << 'EOF'
SELECT
  *
FROM
  `project.dataset.table`
WHERE
  DATE(submission_timestamp) = @submission_date
EOF

git add sql/test_project/test_dataset/test_table_v1/query.sql
git commit -m "Test: Add sample query with SELECT *"
git push origin test-claude-review

# Create DRAFT PR
gh pr create \
  --draft \
  --title "TEST: Draft PR - Claude should NOT review" \
  --body "This is a draft PR. Claude should NOT trigger automatically."
```

**Validation**:
- [ ] Go to Actions tab in your fork
- [ ] Verify "Claude Code Review" workflow does NOT run
- [ ] Check workflow logs show job was skipped due to draft condition

### Test 2: Ready for Review Trigger

**Expected**: Claude SHOULD review when marked ready

```bash
# Get the PR number from previous test
PR_NUMBER=$(gh pr list --state open --json number --jq '.[0].number')

# Mark PR as ready for review
gh pr ready $PR_NUMBER
```

**Validation**:
- [ ] Go to Actions tab and verify workflow starts
- [ ] Check that Claude posts review comments
- [ ] Verify Claude flags the `SELECT *` issue
- [ ] Verify Claude notes missing metadata.yaml and schema.yaml
- [ ] Check that comments are properly formatted with inline annotations

### Test 3: New Non-Draft PR (Should Trigger)

**Expected**: Claude SHOULD review new non-draft PRs

```bash
# Create new branch
git checkout -b test-claude-review-2

# Create a better example with metadata
mkdir -p sql/test_project/test_dataset/test_table_v2

cat > sql/test_project/test_dataset/test_table_v2/query.sql << 'EOF'
-- Test query for Claude review
SELECT
  user_id,
  event_name,
  submission_timestamp
FROM
  `{{ project_id }}.{{ dataset_id }}.events`
WHERE
  DATE(submission_timestamp) = @submission_date
  AND user_id IS NOT NULL
EOF

cat > sql/test_project/test_dataset/test_table_v2/metadata.yaml << 'EOF'
friendly_name: Test Events Table
description: Test table for Claude review validation
owners:
  - test@mozilla.com
labels:
  application: test
  incremental: true
scheduling:
  dag_name: bqetl_test
EOF

cat > sql/test_project/test_dataset/test_table_v2/schema.yaml << 'EOF'
fields:
- name: user_id
  type: STRING
  mode: NULLABLE
  description: User identifier
- name: event_name
  type: STRING
  mode: REQUIRED
  description: Name of the event
- name: submission_timestamp
  type: TIMESTAMP
  mode: REQUIRED
  description: When the event was submitted
EOF

git add sql/test_project/test_dataset/test_table_v2/
git commit -m "Add well-structured test table with metadata"
git push origin test-claude-review-2

# Create NON-DRAFT PR
gh pr create \
  --title "TEST: Well-structured query - Claude should review" \
  --body "This PR includes query, metadata, and schema. Claude should provide positive feedback."
```

**Validation**:
- [ ] Workflow triggers automatically
- [ ] Claude provides comprehensive review
- [ ] Claude acknowledges proper metadata and schema
- [ ] Claude may still suggest improvements (formatting, tests, etc.)

### Test 4: @claude Comment Trigger

**Expected**: Claude SHOULD respond to @claude mentions in comments

```bash
# Get PR number
PR_NUMBER=$(gh pr list --state open --json number --jq '.[0].number')

# Add comment mentioning @claude
gh pr comment $PR_NUMBER --body "@claude Can you check if this query is optimized for partitioned tables?"
```

**Validation**:
- [ ] Workflow triggers on comment
- [ ] Claude responds to the specific question
- [ ] Claude analyzes partition filter usage
- [ ] Response is contextually relevant to the question

### Test 5: Code Review Comment Trigger

**Expected**: Claude SHOULD respond to @claude in code review comments

```bash
# This requires manual testing via GitHub UI:
# 1. Go to Files Changed tab in the PR
# 2. Click on a line in query.sql
# 3. Add review comment: "@claude Is this the right data type for user_id?"
# 4. Submit review
```

**Validation**:
- [ ] Workflow triggers on review comment
- [ ] Claude responds to specific line question
- [ ] Response addresses the data type concern

### Test 6: Synchronize Trigger (New Commit)

**Expected**: Claude SHOULD re-review when new commits added to ready PR

```bash
# Make a change to existing PR
git checkout test-claude-review-2

# Add a problematic query
cat > sql/test_project/test_dataset/test_table_v2/query.sql << 'EOF'
-- Potential SQL injection risk
SELECT
  *
FROM
  `{{ project_id }}.{{ dataset_id }}.events`
WHERE
  user_id = '{{ user_input }}'
EOF

git add sql/test_project/test_dataset/test_table_v2/query.sql
git commit -m "Add problematic query with potential SQL injection"
git push origin test-claude-review-2
```

**Validation**:
- [ ] Workflow triggers on new commit
- [ ] Claude updates review with new findings
- [ ] Claude flags SQL injection risk
- [ ] Claude flags `SELECT *` issue
- [ ] Comments update in-place (sticky comment behavior)

### Test 7: Fork PR Security (Should NOT Trigger)

**Expected**: Claude should NOT run on PRs from external forks (security)

```bash
# This requires another GitHub account or asking a colleague:
# 1. Have someone fork YOUR fork
# 2. They make changes and open PR to YOUR fork
# 3. Workflow should NOT trigger (to protect ANTHROPIC_API_KEY)
```

**Validation**:
- [ ] Workflow does NOT run on fork PRs
- [ ] Job skipped due to repository check
- [ ] Logs show: "github.event.pull_request.head.repo.full_name != github.repository"

## Test 8: Complex BigQuery SQL

**Expected**: Claude provides SQL-specific feedback

```bash
git checkout -b test-claude-review-complex

mkdir -p sql/test_project/test_dataset/complex_query_v1

cat > sql/test_project/test_dataset/complex_query_v1/query.sql << 'EOF'
-- Complex query for testing
WITH user_events AS (
  SELECT
    user_id,
    event_timestamp,
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_timestamp DESC) as rn
  FROM
    `{{ project_id }}.analytics.events`
),
filtered_events AS (
  SELECT
    user_id,
    event_timestamp
  FROM
    user_events
  WHERE
    rn = 1
)
SELECT
  fe.user_id,
  fe.event_timestamp,
  up.country,
  up.device_type
FROM
  filtered_events fe
LEFT JOIN
  `{{ project_id }}.analytics.user_profiles` up
ON
  fe.user_id = up.user_id
EOF

cat > sql/test_project/test_dataset/complex_query_v1/metadata.yaml << 'EOF'
friendly_name: Complex User Events
description: Latest event per user with profile data
owners:
  - data-team@mozilla.com
EOF

git add sql/test_project/test_dataset/complex_query_v1/
git commit -m "Add complex query with window functions and joins"
git push origin test-claude-review-complex

gh pr create \
  --title "TEST: Complex SQL query review" \
  --body "Testing Claude's ability to review complex BigQuery SQL with CTEs, window functions, and joins."
```

**Validation**:
- [ ] Claude reviews query structure
- [ ] Claude may suggest partition filters for large tables
- [ ] Claude checks if window function is appropriate
- [ ] Claude flags missing schema.yaml
- [ ] Claude may suggest query optimizations

## Verification Checklist

After running all tests, verify:

### Functionality
- [ ] Draft PRs are skipped
- [ ] Non-draft PRs trigger review
- [ ] `ready_for_review` event triggers review
- [ ] `@claude` mentions in comments trigger
- [ ] `@claude` in code review comments trigger
- [ ] New commits trigger re-review
- [ ] Fork PRs are blocked (security)

### Review Quality
- [ ] Claude identifies SQL anti-patterns (SELECT *)
- [ ] Claude checks for metadata completeness
- [ ] Claude validates schema presence
- [ ] Claude suggests BigQuery-specific optimizations
- [ ] Claude flags security issues
- [ ] Claude provides actionable feedback

### GitHub Integration
- [ ] Inline comments appear on specific lines
- [ ] Summary comment appears on PR
- [ ] Comments update in place (sticky)
- [ ] Progress tracking works
- [ ] Multiple reviews don't create duplicate comments

### Performance
- [ ] Review completes in reasonable time (< 5 minutes)
- [ ] API costs are acceptable
- [ ] No workflow failures or timeouts

## Cleanup After Testing

```bash
# Close all test PRs
gh pr list --state open --json number --jq '.[].number' | xargs -I {} gh pr close {}

# Delete test branches locally
git checkout main
git branch -D test-claude-review test-claude-review-2 test-claude-review-complex

# Delete remote test branches
git push origin --delete test-claude-review test-claude-review-2 test-claude-review-complex

# Delete test SQL files
git checkout main
rm -rf sql/test_project/
git commit -am "Clean up test files"
git push origin main
```

## Submitting to Upstream

Once testing is complete and successful:

```bash
# Create clean branch from upstream main
git fetch upstream main
git checkout -b add-claude-review upstream/main

# Copy only the workflow file
git checkout YOUR_TEST_BRANCH -- .github/workflows/claude-review.yml

# Create documentation
git checkout YOUR_TEST_BRANCH -- .github/CLAUDE_REVIEW_TEST_PLAN.md

git add .github/workflows/claude-review.yml .github/CLAUDE_REVIEW_TEST_PLAN.md
git commit -m "Add Claude code review GitHub Action

This PR adds automated code review using Anthropic's Claude via GitHub Actions.

Features:
- Reviews PRs when marked ready for review (skips drafts)
- Responds to @claude mentions in PR comments
- Focuses on BigQuery ETL best practices
- Provides inline code comments and summary feedback

Testing completed in fork: [link to your fork]
Test results documented in CLAUDE_REVIEW_TEST_PLAN.md

Requires ANTHROPIC_API_KEY secret to be added by repo admin."

git push origin add-claude-review

# Create PR to upstream
gh pr create \
  --repo mozilla/bigquery-etl \
  --title "Add Claude automated code review workflow" \
  --body "$(cat <<EOF
## Summary
This PR adds automated code review using Claude via GitHub Actions.

## Features
- ✅ Reviews PRs when marked ready for review
- ✅ Skips draft PRs
- ✅ Responds to @claude mentions in comments
- ✅ BigQuery and SQL-specific review focus
- ✅ Security: blocks fork PRs to protect secrets

## Testing
Comprehensive testing completed in fork: https://github.com/YOUR_USERNAME/bigquery-etl

See test results in .github/CLAUDE_REVIEW_TEST_PLAN.md

## Required Action
Repo admin needs to add ANTHROPIC_API_KEY secret:
1. Settings → Secrets and variables → Actions
2. New repository secret: ANTHROPIC_API_KEY
3. Value: [Anthropic API key]

## Documentation
- Workflow: .github/workflows/claude-review.yml
- Test plan: .github/CLAUDE_REVIEW_TEST_PLAN.md

Addresses: [link to relevant issue if any]
EOF
)"
```

## Notes

- **Cost Monitoring**: Each PR review consumes API tokens. Monitor usage at https://console.anthropic.com/
- **Iteration**: First reviews may need prompt refinement based on actual PR patterns
- **False Positives**: Track and adjust prompt if Claude flags valid patterns
- **Team Feedback**: Collect feedback from team on review usefulness

## Troubleshooting

### Workflow doesn't trigger
- Check if PR is draft
- Verify secret name is exactly `ANTHROPIC_API_KEY`
- Check workflow file syntax with `yamllint .github/workflows/claude-review.yml`

### Claude doesn't respond to @claude
- Ensure trigger_phrase matches (case-sensitive)
- Check if comment is on a PR (not standalone issue)
- Verify permissions include `issues: write`

### Review quality issues
- Refine prompt in workflow file
- Adjust `--max-turns` if review incomplete
- Add specific examples to prompt

### API errors
- Check API key is valid
- Verify sufficient API credits
- Check rate limits in Anthropic console
