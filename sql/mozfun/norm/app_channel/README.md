Normalize app channel name, returning "Other" for unrecognized values.

Based on the logic used in ingestion:
https://github.com/mozilla/gcp-ingestion/blob/fb7e9ed9e891e3e2320d85e05d7c1a1aedb57780/ingestion-beam/src/main/java/com/mozilla/telemetry/transforms/NormalizeAttributes.java#L34
