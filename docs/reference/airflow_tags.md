# Airflow Tags

## Why
Airflow tags enable DAGs to be filtered in the web ui view to reduce the number of DAGs shown to just those that you are interested in.

Additionally, their objective is to provide a little bit more information such as their impact to make it easier to understand the DAG and impact of failures when doing Airflow triage.

More information and the discussions can be found the the original Airflow Tags Proposal (can be found within data org `proposals/` folder).

## Valid tags

### impact/tier tag

We borrow the [tiering system](https://wiki.mozilla.org/Sheriffing/Job_Visibility_Policy#Overview_of_the_Job_Visibility_Tiers) used by our integration and testing sheriffs. This is to maintain a level of consistency across different systems to ensure common language and understanding across teams. Valid tier tags include:

- **impact/tier_0**: Foundational pipelines that a significant portion of downstream data processing depends on (e.g. copy dedupe, Glean usage). A Tier 0 failure takes priority over all other work — all hands on deck, must fix ASAP. A bug ticket must be created immediately and the issue worked until it is resolved or explicitly handed off to someone who has accepted ownership.
- **impact/tier_1**: Business-critical pipelines: those supporting key daily reporting, important product functionality, or time-sensitive processes such as data sent to external systems or marketing ad platforms. A bug ticket must be created and the issue is expected to be resolved within hours, no later than the end of the same business day.
- **impact/tier_2**: Important pipelines supporting established, actively used workflows that are not time-sensitive, such as external data feeds (app store reviews, Reddit, etc.), where a short delay is acceptable but an extended outage would affect planned work. A bug ticket must be created and the issue is expected to be resolved within 2–3 business days.
- **impact/tier_3**: No impact on other processes and is not used to generate any metrics used by business users or to make any decisions. A bug ticket should be created and it’s up to the job owner to fix this issue in whatever time frame they deem to be reasonable.

### triage/ tag

This tag is meant to provide guidance to a triage engineer on how to respond to a specific DAG failure when the job owner does not want the standard process to be followed.

- **triage/record_only**: Failures should _only_ be recorded and the job owner informed without taking any active steps to fix the failure.

- **triage/no_triage**: No triage should be performed on this job. Should only be used in a limited number of cases, like this is still WIP, where no production processes are affected.

- **triage/confidential** - Failures should be recorded by the triage engineer as normal, and bug should be marked **Confidential**.
