#warn
{{ is_unique(["dte", "os", "location", "device_type"], "dte = DATE_SUB(@date, INTERVAL 4 DAY)") }}
