# Airflow tags

## Why
Airflow tags enable DAGs to be filtered in the web ui view to reduce the number of DAGs shown to just those that you are interested in.

Additionally, their objective is to provide a little bit more information such as their impact to make it easier to understand the DAG and impact of failures when doing Airflow triage.

More information and the discussions can be found the the original [Airflow tags proposal](https://docs.google.com/document/d/1LqOCmadsC6kPTusqyczHha-mdG_DVsNWK4-a1-NHI_8).

---

## Valid tags

### impact/tier tag

We follow the same tiers as the ones defined for automation/treeherder. This is to maintain a level of consistency across different systems to ensure common language and understanding across teams. Valid tier tags include:

- **impact/tier_1**: Highest priority/impact/critical DAG. A job with this tag implies that many downstream processes are impacted and affects Mozilla’s (many users across different teams and departments) ability to make decisions. A bug ticket must be created and the issue needs to be resolved as soon as possible.
- **impact/tier_2**:  Job of increased importance and impact, however, not critical and only limited impact on other processes. One team or group of people is affected and the pipeline does not generate any business critical metrics. A bug ticket must be created and should be addressed within a few working days.
- **impact/tier_3**: No impact on other processes and is not used to generate any metrics used by business users or to make any decisions. A bug ticket should be created and it’s up to the job owner to fix this issue in whatever time frame they deem to be reasonable.
