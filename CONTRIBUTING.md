# Contributing to bigquery-etl

Thank you for your interest in contributing to bigquery-etl! Although the code in this repository is licensed under the MPL, working on this repository effectively requires access to Mozilla's BigQuery data infrastructure which is reserved for Mozilla employees and designated contributors. For more information, see the sections on [gaining access] and [BigQuery Access Request] on [docs.telemetry.mozilla.org].

More information on working with this repository can be found in the README.md file (at the root of this repository) and in the [repository documentation].

If you are updating schemas associated with certain restricted-access datasets (specified in [`CODEOWNERS`](/CODEOWNERS)), a CODEOWNER (usually SRE) will automatically be assigned to review the PR. Please follow additional [change control procedures] for PRs referencing these schemas. The CODEOWNER will be responsible for merging the PR once it has been approved.

[gaining access]: https://docs.telemetry.mozilla.org/concepts/gaining_access.html
[BigQuery Access Request]: https://docs.telemetry.mozilla.org/cookbooks/bigquery/access.html#bigquery-access-request
[docs.telemetry.mozilla.org]: https://docs.telemetry.mozilla.org
[repository documentation]: https://mozilla.github.io/bigquery-etl/
[change control procedures]: https://docs.google.com/document/d/1TTJi4ht7NuzX6BPG_KTr6omaZg70cEpxe9xlpfnHj9k/edit#heading=h.ttegrcfy18ck
