# Checklist for reviewer:

- [ ] Commits should reference a bug or github issue, if relevant (if a bug is referenced, the pull request should include the bug number in the title).
- [ ] If the PR comes from a fork, carefully review the changed code (especially workflow files, scripts, and code) before approving the `external-fork` environment deployment to run integration CI tests.
- [ ] If adding a new field to a query, ensure that the schema and dependent downstream schemas have been updated.
- [ ] When adding a new derived dataset, ensure that data is not available already (fully or partially) and recommend extending an existing dataset in favor of creating new ones. Data can be available in the [bigquery-etl repository](https://github.com/mozilla/bigquery-etl), [looker-hub](https://github.com/mozilla/looker-hub) or in [looker-spoke-default](https://github.com/mozilla/looker-spoke-default/tree/e1315853507fc1ac6e78d252d53dc8df5f5f322b).

For modifications to schemas in restricted namespaces (see [`CODEOWNERS`](https://github.com/mozilla/bigquery-etl/blob/main/CODEOWNERS)):
- [ ] Follow the [change control procedure](https://docs.google.com/document/d/1TTJi4ht7NuzX6BPG_KTr6omaZg70cEpxe9xlpfnHj9k/edit#heading=h.ttegrcfy18ck)
