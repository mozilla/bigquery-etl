This UDF transforms the `ping_info.experiments` field from Glean pings into the format for `experiments` used by Legacy Telemetry pings. In particular, it drops the exta information that Glean pings collect.

If you need to combine Glean data with Legacy Telemetry data, then you can use this UDF to transform a Glean experiments field into the structure of a Legacy Telemetry one.
