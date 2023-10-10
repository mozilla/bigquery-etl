# Funnel Generation

This generator generates queries for funnels that are defined in config files.

> This is currently a work in progress and being tested by data science. Don't use for production funnels just yet, things might change in the future.

## Background

Funnels are used to visualize and understand user behaviours throughout customer journeys. Funnels consist of a sequence of steps that each represent a certain event that happened.
Each step uses data from specific datasets. In some cases we have IDs (for example `flow_id`s) that can be used to connect events across all of the funnel steps. For example, these IDs can ensure that a client is only considered as part of a step if the client was also seen in earlier steps.

Since we don't always have these identifiers, in some cases steps of a funnel might be "independent". Each step might aggregate values based on specific (potentially different) datasets without making sure, that for example, a client is part of earlier funnel steps as well.

It is also sometimes desired to look at specific segments of a funnel. For example, having funnels that show how clients of a specific operating system behave.

All of this is encoded in this SQL generator.

## Defining Funnels

Funnels are defined in `.toml` files in [`sql_generators/funnels/configs/`](https://github.com/mozilla/bigquery-etl/tree/main/sql_generators/funnels/configs/). The syntax is similar to [defining metrics in metric-hub](https://docs.telemetry.mozilla.org/concepts/metric_hub.html).

The file name of the config file needs to be unique and will be the name of the result table in BigQuery.
The `destination_dataset` is by default set to `telemetry_derived`, but can be overwritten in the config.

### `[funnels]` Section

It is possible to define multiple funnels in a config file. Each funnel requires a unique identifier and a set of `steps` it consists of. 
Funnels can also optinally be segmented on specific dimensions, like operating system or country.

```toml
[funnels]

# subscription_funnel is the unique identifier for this funnel
[funnels.subscription_funnel]

friendly_name = "Successful Subscription Funnel"  # optional
description = "Funnel from signup to starting a subscription"  # optional
steps = ["signup", "verify", "start_subscription"]  # referenced steps; defined below
dimensions = ["os"]  # this funnel is segmented on operation system; dimensions are defined below

# a second funnel
[funnels.subscription_error_funnel]

friendly_name = "Subscription Error Funnel"
description = "Funnel from Signup to running into an error"
steps = ["signup", "verify", "error_subscription"]  # step can be 'reused' and referenced across multiple funnels
```

### `[steps]` Section

Each step requires a unique identifier. Steps depend on data sources and define how a specific field should be filtered and aggregated.

```toml
[steps]

[steps.signup]
friendly_name = "Sign up"  # optional
description = "Sign up for VPN"  # optional
data_source = "events"  # referenced data source; defined below
filter_expression = """
    event_name = 'authentication_inapp_step' AND
    `mozfun.map.get_key`(event_extra, 'state') = 'StateVerifyingSessionEmailCode'
"""  # optional; SQL WHERE expression
join_previous_step_on = "client_info.client_id"  # optional
select_expression = "client_info.client_id"  # the field to be aggregated
aggregation = "count distinct"  # aggregation
```

* `data_source` referenced a data source slug. Data sources defined in metric-hub can also be referenced, however `platform` needs to be specified in the config.
* `filter_expression` is an optional SQL snippet that gets applied to the `data_source`
* `join_previous_step_on`: Steps in a funnel might depend on previous steps in the same funnel. For example, we might want to ensure that only clients that have been seen in the first step of the funnel get considered in the second step of the funnel. `join_previous_step_on` can be used to specify fields that should propagate through all the steps of a funnel. This field is optional. If steps in a funnel are "independent" from each other then this field does not need to be set.
* `select_expression` specifies the field that should be aggregated in the funnel step
* `aggregation` specifies the type of aggregation that should be applied to the field from the `select_expression`. There are a few pre-defined aggregations which can be referenced, like `min`, `max`, `count`, `count distinct` or `mean`. It is also possible to have custom SQL aggregations which need to be string templates aggregating a `{column}` value, e.g. `ANY_VALUE({column})`.

### `[data_sources]` Section

Data sources specify a table or a SQL query data should be queried from for a `step` or `dimension`.

```toml
[data_sources]

[data_sources.main]
# FROM expression - often just a fully-qualified table name. Sometimes a subquery.
from_expression = "mozdata.telemetry.main"

[data_sources.events]
from_expression = """
    (SELECT * FROM mozdata.mozilla_vpn.events_unnested
    WHERE client_info.app_channel = 'production' AND client_info.os = 'iOS')
"""
submission_date_column = "DATE(submission_timestamp)"
client_id_column = "client_info.client_id"
```

* `submission_date_column`: by default set to `submission_date`. This field might need to be set to the field that specifies a date
* `client_id_column`: by default set to `client_id`. This field is only used when using dimensions to segment the data. The `dimension` `client_id_column` will be joined with the `client_id_column` of the step. This field can be set to any unique identifier.

### `[dimensions]` Section

Dimensions define a field or dimension on which the client population should be segmented.

```toml
[dimensions]

[dimensions.os]
data_source = "events"
select_expression = "normalized_os"
friendly_name = "Operating System"  # optional
description = "Normalized Operating System"  # optional
client_id_column = "client_info.client_id"
```

### Example Config

```toml
destination_dataset = "mozilla_vpn_derived"
platform = "mozilla_vpn"
owners = ["example@mozilla.org"]  # optional; users getting notification if funnel run fails
version = "1"  # optional; default is set to 1

[funnels]

[funnels.subscription_funnel]

friendly_name = "Start Subscription Funnel"
description = "Funnel from Signup to starting a subscription"
steps = ["signup", "verify", "start_subscription"]
dimensions = ["os"]

[funnels.subscription_error_funnel]
friendly_name = "Subscription Error Funnel"
description = "Funnel from Signup to running into an error"
steps = ["signup", "verify", "error_subscription"]


[steps]

[steps.signup]
friendly_name = "Sign up"
description = "Sign up for VPN"
data_source = "events"
filter_expression = """
    event_name = 'authentication_inapp_step' AND
    `mozfun.map.get_key`(event_extra, 'state') = 'StateVerifyingSessionEmailCode'
"""
join_previous_step_on = "client_info.client_id"
select_expression = "client_info.client_id"
aggregation = "count distinct"

[steps.verify]
friendly_name = "Verify"
description = "Verify email"
data_source = "events"
select_expression = "client_info.client_id"
where_expression = """
    event_name = 'authentication_inapp_step' AND
    `mozfun.map.get_key`(event_extra, 'state') = 'StateVerifyingSessionEmailCode'
"""
aggregation = "count distinct"
join_previous_step_on = "client_info.client_id"

[steps.start_subscription]
friendly_name = "Start Subscription"
description = "Start VPN subscription"
data_source = "events"
select_expression = "client_info.client_id"
where_expression = "event_name = 'iap_subscription_started'"
aggregation = "count distinct"
join_previous_step_on = "client_info.client_id"

[steps.error_subscription]
friendly_name = "Subscription Error"
description = "subscription error"
data_source = "events"
select_expression = "client_info.client_id"
where_expression = "event_name = 'error_alert_shown'"
aggregation = "count"
join_previous_step_on = "client_info.client_id"


[data_sources]

[data_sources.events]
from_expression = """
    (SELECT * FROM mozdata.mozilla_vpn.events_unnested
    WHERE client_info.app_channel = 'production' AND client_info.os = 'iOS')
"""
submission_date_column = "DATE(submission_timestamp)"
client_id_column = "client_info.client_id"


[dimensions]

[dimensions.os]
data_source = "events"
select_expression = "normalized_os"
friendly_name = "Operating System"
description = "Normalized Operating System"
client_id_column = "client_info.client_id"
```

## Generating Funnels

The generated funnel queries do not need to be checked in to `main`. They'll get generated and executed automatically.

To generate the query (for example for debugging) run: `./bqetl generate funnels`. The generated funnel query will be in `sql/moz-fx-data-shared` in the folder specified as the `destination_dataset` (`telemetry_derived` by default).

To run the generated query run: `./bqetl query run <destination_dataset>.<name_of_config_with_underscores> --project_id=moz-fx-data-shared-prod --dataset_id=<destination_dataset> --destination_table=<destination_table>`

## Results

Generated funnel queries are scheduled to run on a daily basis. Each funnel query writes the results for all the funnels defined in the corresponding config into a single result table with the following schema:

| Column Name       | Type             | Description                                                              |
| `submission_date` | Date             | Aggregated funnel results for this date                                  |
| `funnel`          | String           | Funnel identifier specifying funnel results are for                      |
| `<segment>`       | String           | Segment value; there are as many segment columns as segments             |
| `<step>`          | Number           | Aggregated value for this step; `NULL` if step is not part of the funnel |

`<segment>` and `<step>` are replaced by the unique segment and step identifiers.
