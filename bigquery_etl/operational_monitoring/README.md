Operational Monitoring
===

Operational Monitoring is a general term that refers to monitoring the health of software.

For the purpose of this project, there will be a couple of specific monitoring use cases that are supported:
1. Monitoring build over build. This is typically for Nightly where one build may contain changes that a previous build doesn't and we want to see if those changes affected certain metrics.
2. Monitoring by submission date over time. This is helpful for a rollout in Release for example, where we want to make sure there are no performance or stability regressions over time as a new build rolls out.

Although the ETL for both cases shares a common set of templates, the data is a stored/represented slightly differently.
* For case 1, submission date is used as the partition but previous submission dates are only there for backup. The most recent submission date is the only one of interest as it will include all relevant builds to be graphed.
* For case 2, submission date is also used as the partition, but they are not backups. The previous submission dates will include dates that need to be graphed.

#### Project Definition Files
A project is defined using in a JSON file in combination with 2 other JSON files. Examples of each file can be found below:

##### dimensions.json
```
{
  "cores_count": {
    "source": ["mozdata.telemetry.main_nightly"],
    "sql": "environment.system.cpu.cores"
  },
  "os": {
    "source": ["mozdata.telemetry.main_nightly", "mozdata.telemetry.crash"],
    "sql": "normalized_os"
  }
  ...
}
```

##### probes.json
```
{
  "CONTENT_SHUTDOWN_CRASHES": {
    "source": "mozdata.telemetry.crash",
    "category": "stability",
    "type": "scalar",
    "sql": "IF(REGEXP_CONTAINS(payload.process_type, 'content') AND REGEXP_CONTAINS(payload.metadata.ipc_channel_error, 'ShutDownKill'), 1, 0)"
  },
  "CONTENT_PROCESS_COUNT": {
    "source": "mozdata.telemetry.main_nightly",
    "category": "performance",
    "type": "histogram",
    "sql": "payload.histograms.content_process_count"
  },
 ...
}
```

##### <project_name>.json

```
{
  "name": "Fission Release Rollout",
  "slug": "bug-1732206-rollout-fission-release-rollout-release-94-95",
  "channel": "release",
  "boolean_pref": "environment.settings.fission_enabled",
  "xaxis": "submission_date",
  "start_date": "2021-11-09",
  "analysis": [{
  	"source": "mozdata.telemetry.main_1pct",
  	"dimensions": ["cores_count", "os"],
  	"probes": [
  		"CONTENT_PROCESS_COUNT",
    ]
	}, {
    "source": "mozdata.telemetry.crash",
    "dimensions": ["cores_count", "os"],
    "probes": [
      "CONTENT_SHUTDOWN_CRASHES",
    ]
  }]
}
```

Below is the description of each field in the project definition and its allowable values:
* `name`: Name that will be used as the generated Looker dashboard title
* `slug`: The slug associated with the rollout that is being monitored
* `channel`: `release`, `beta`, or `nightly`. The channel the rollout is running in
* `boolean_pref`: A sql snippet that results in a boolean representing whether a user is included in the rollout or not (note: if this is not included, the slug is used to check instead)
* `xaxis`: `submission_date` or `build_id`. specifies the type of monitoring desired as described above.
* `start_date`: `yyyy-mm-dd`, specifies the oldest submission date or build id to be processed (where build id is cast to date). If not set, defaults to the previous 30 days.
* `analysis`: An array of objects with the fields `source`, `dimensions`, and `probes` where each object represents data that will be pulled out from a different sourc.
    * `source`: The BigQuery table name to be queried
    * `dimensions`: The dimensions of interest, referenced from `dimensions.json`
    * `probes`: The probes of interest, referenced from `probes.json`






