# event_analysis

These functions are specific for use with the `events_daily` and `event_types` tables.
By themselves, these two tables are nearly impossible to use since the event history
is compressed; however, these stored procedures should make the data accessible.

The `events_daily` table is created as a result of two steps:
1. Map each event to a single UTF8 char which will represent it
2. Group each client-day and store a string that records, using the
    compressed format, that clients' event history for that day.
    The characters are ordered by the timestamp which they appeared
    that day.

The best way to access this data is to create a view to do the heavy lifting.
For example, to see which clients completed a certain action, you can create a view using these functions that
knows what that action's representation is (using the compressed mapping from 1.) and
create a regex string that checks for the presence of that event. The view makes this
transparent, and allows users to simply query a boolean field representing the presence
of that event on that day.


## create_events_view (Stored Procedure)

Create a view that queries the `events_daily` table. This view currently supports both funnels and event counts.
Funnels are created as a struct, with each step in the funnel as a boolean column in the struct, indicating whether the user completed that step on that day.
Event counts are simply integers.


### Usage

```sql
create_events_view(
    view_name STRING,
    project STRING,
    dataset STRING,
    funnels ARRAY<STRUCT<
        funnel_name STRING,
        funnel ARRAY<STRUCT<
            step_name STRING,
            events ARRAY<STRUCT<
                category STRING,
                event_name STRING>>>>>>,
    counts ARRAY<STRUCT<
        count_name STRING,
        events ARRAY<STRUCT<
            category STRING,
            event_name STRING>>>>
  )
```

- `view_name`: The name of the view that will be created. This view
    will be in the shared-prod project, in the analysis bucket,
    and so will be queryable at:
        ```sql
        `moz-fx-data-shared-prod`.analysis.{view_name}
        ```

- `project`: The project where the `dataset` is located.

- `dataset`: The dataset that must contain both the `events_daily` and
    `event_types` tables.

- `funnels`: An array of funnels that will be created. Each funnel has
    two parts:
    1. `funnel_name`: The name of the funnel is what the column representing
        the funnel will be named in the view. For example, with the value
        `"onboarding"`, the view can be selected as follows:
        ```sql
        SELECT onboarding
        FROM `moz-fx-data-shared-prod`.analysis.{view_name}
        ```
    2. `funnel`: The ordered series of steps that make up a funnel.
        Each step also has:
        1. `step_name`: Used to name the column
            within the funnel and represents whether the user completed
            that step on that day. For example, within `onboarding` a user may
            have `completed_first_card` as a step; this can be queried at
            ```sql
            SELECT onboarding.completed_first_step
            FROM `moz-fx-data-shared-prod`.analysis.{view_name}
            ```
        2. `events`: The set of events which indicate the user completed
            that step of the funnel. Most of the time this is a single event.
            Each event has a `category` and `event_name`.

- `counts`: An array of counts. Each count has two parts, similar to funnel steps:
    1. `count_name`: Used to name the column representing the event count. E.g.
        `"clicked_settings_count"` would be queried at
        ```sql
        SELECT clicked_settings_count
        FROM `moz-fx-data-shared-prod`.analysis.{view_name}
        ```
    2. `events`: The set of events you want to count. Each event has
        a `category` and `event_name`.


#### Recommended Pattern
Because the view definitions themselves are not informative about the contents of the events fields,
it is best to put your query immediately after the procedure invocation, rather than invoking the procedure and running a separate query.

[This STMO query](https://sql.telemetry.mozilla.org/queries/75243/source) is an example of doing so.
This allows viewers of the query to easily interpret what the funnel and count columns represent.


#### Structure of the Resulting View

The view will be created at 
```
`moz-fx-data-shared-prod`.analysis.{event_name}.
```

The view will have a schema roughly matching the following:
```
root
 |-- submission_date: date
 |-- client_id: string
 |-- {funnel_1_name}: record
 |  |-- {funnel_step_1_name} boolean
 |  |-- {funnel_step_2_name} boolean
 ...
 |-- {funnel_N_name}: record
 |  |-- {funnel_step_M_name}: boolean
 |-- {count_1_name}: integer
 ...
 |-- {count_N_name}: integer
 ...dimensions...
```


##### Funnels
Each funnel will be a `STRUCT` with nested columns representing completion of each step
The types of those columns are boolean, and represent whether the user completed that
step on that day.

```sql
STRUCT(
    completed_step_1 BOOLEAN,
    completed_step_2 BOOLEAN,
    ...
) AS funnel_name
```

With one row per-user per-day, you can use `COUNTIF(funnel_name.completed_step_N)` to query
these fields. See below for an example.


##### Event Counts
Each event count is simply an `INT64` representing the number of times the user completed
those events on that day. If there are multiple events represented within one count,
the values are summed. For example, if you wanted to know the number of times a user
opened or closed the app, you could create a single event count with those two
events.

```sql
event_count_name INT64
```

### Examples
The following creates a few fields:
- `collection_flow` is a funnel for those that started creating
    a collection within Fenix, and then finished, either by adding
    those tabs to an existing collection or saving it as a new
    collection.
- `collection_flow_saved` represents users who started the collection
    flow then saved it as a new collection.
- `number_of_collections_created` is the number of collections created
- `number_of_collections_deleted` is the number of collections deleted

```sql
CALL mozfun.event_analysis.create_events_view(
  'fenix_collection_funnels',
  'moz-fx-data-shared-prod',
  'org_mozilla_firefox',

  -- Funnels
  [
    STRUCT(
      "collection_flow" AS funnel_name,
      [STRUCT(
        "started_collection_creation" AS step_name,
        [STRUCT('collections' AS category, 'tab_select_opened' AS event_name)] AS events),
      STRUCT(
        "completed_collection_creation" AS step_name,
        [STRUCT('collections' AS category, 'saved' AS event_name),
        STRUCT('collections' AS category, 'tabs_added' AS event_name)] AS events)
    ] AS funnel),

    STRUCT(
      "collection_flow_saved" AS funnel_name,
      [STRUCT(
        "started_collection_creation" AS step_name,
        [STRUCT('collections' AS category, 'tab_select_opened' AS event_name)] AS events),
      STRUCT(
        "saved_collection" AS step_name,
        [STRUCT('collections' AS category, 'saved' AS event_name)] AS events)
    ] AS funnel)
  ],

  -- Event Counts
  [
    STRUCT(
      "number_of_collections_created" AS count_name,
      [STRUCT('collections' AS category, 'saved' AS event_name)] AS events
    ),
    STRUCT(
      "number_of_collections_deleted" AS count_name,
      [STRUCT('collections' AS category, 'removed' AS event_name)] AS events
    )
  ]
);
```

From there, you can query a few things. For example, the fraction 
of users who completed each step of the collection flow over time:
```sql
SELECT
    submission_date,
    COUNTIF(collection_flow.started_collection_creation) / COUNT(*) AS started_collection_creation,
    COUNTIF(collection_flow.completed_collection_creation) / COUNT(*) AS completed_collection_creation,
FROM
    `moz-fx-data-shared-prod`.analysis.fenix_collection_funnels
WHERE
    submission_date >= DATE_SUB(current_date, INTERVAL 28 DAY)
GROUP BY
    submission_date
```

Or you can see the number of collections created and deleted:
```sql
SELECT
    submission_date,
    SUM(number_of_collections_created) AS number_of_collections_created,
    SUM(number_of_collections_deleted) AS number_of_collections_deleted,
FROM
    `moz-fx-data-shared-prod`.analysis.fenix_collection_funnels
WHERE
    submission_date >= DATE_SUB(current_date, INTERVAL 28 DAY)
GROUP BY
    submission_date
```


## extract_event_counts (UDF)

Extract the events and their counts from an events string.
This function explicitly ignores event properties, and retrieves just the counts of the top-level events.


### Usage
```
extract_event_counts(
    events STRING
)
```

`events` - A comma-separated events string,
where each event is represented as a string
of unicode chars.

### Example
See [this dashboard](https://sql.telemetry.mozilla.org/dashboard/fenix-events)
for example usage.


## extract_event_counts_with_properties (UDF)

Extract events with event properties and their associated counts.
Also extracts raw events and their counts. This allows for querying with and without properties in the same dashboard.


### Usage
```
extract_event_counts_with_properties(
    events STRING
)
```

`events` - A comma-separated events string,
where each event is represented as a string
of unicode chars.

### Example
See [this query](https://sql.telemetry.mozilla.org/queries/75082)
for example usage.

### Caveats
This function extracts both counts for events with each property,
and for all events without their properties.

This allows us to include both total counts for an event (with any
property value), and events that don't have properties.
