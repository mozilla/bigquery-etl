Checklist for reviewer:

- [ ] Commits should reference a bug or github issue, if relevant (if a bug is
  referenced, the pull request should include the bug number in the title)
- [ ] Scan the PR and verify that no changes (particularly to
  `.circleci/config.yml`) will cause environment variables (particularly
  credentials) to be exposed in test logs
- [ ] Ensure the container image will be using permissions granted to
  [telemetry-airflow](https://github.com/mozilla/telemetry-airflow/)
  responsibly. If this is an update to a submodule, take the time to review the
  explicit diff of the submodule.