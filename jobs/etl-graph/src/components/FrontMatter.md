<script>
    // look at the result of the manifest to determine when the file last changed
    async function getLastModified() {
        let resp = await fetch("data/manifest.json")
        return resp.headers.get("last-modified")
    }
</script>

{#await getLastModified() then last_modified}

_Last updated {last_modified}_

{/await}

This network represents the relationships between tables in BigQuery. It was
created by scraping the BigQuery `TABLES` and `JOBS_BY_PROJECT` tables in the
`INFORMATION_SCHEMA` dataset.

The source can be found at [docker-etl/jobs/etl-graph][repo]. See
[NOTES.md][notes] for an overview of development. This visualizaton is powered
by [vis-network][vis-network].

[repo]: https://github.com/mozilla/docker-etl/tree/main/jobs/etl-graph
[notes]: https://github.com/mozilla/docker-etl/blob/main/jobs/etl-graph/README.md
[vis-network]: https://visjs.github.io/vis-network/docs/network/