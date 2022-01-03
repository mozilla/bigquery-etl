Checklist for reviewer:

- [ ] Commits should reference a bug or github issue, if relevant (if a bug is referenced, the pull request should include the bug number in the title)
- [ ] If the PR comes from a fork, trigger integration CI tests by running the [Push to upstream workflow](https://github.com/mozilla/bigquery-etl/actions/workflows/push-to-upstream.yml) and provide the `<username>:<branch>` of the fork as parameter. The parameter will also show up
in the logs of the `manual-trigger-required-for-fork` CI task together with more detailed instructions.
- [ ] If adding a new field to a query, ensure that the schema and dependent downstream schemas have been updated

For modifications to schemas in restricted namespaces (see [`CODEOWNERS`](/CODEOWNERS)):
- [ ] Follow the [change control procedure](https://docs.google.com/document/d/1TTJi4ht7NuzX6BPG_KTr6omaZg70cEpxe9xlpfnHj9k/edit#heading=h.ttegrcfy18ck)
