<script>
  import { onMount } from "svelte";
  import { DataSet } from "vis-data/peer";
  import Network from "./Network.svelte";

  let data;

  // view-source:https://visjs.github.io/vis-network/examples/network/exampleApplications/loadingBar.html
  function getDatasetId(name) {
    return name.split(".")[0];
  }

  onMount(async () => {
    let edges = await fetch("data/edges.json").then((resp) => resp.json());
    let nodes = new Set();
    for (let i = 0; i < edges.length; i++) {
      nodes.add(edges[i].destination);
      nodes.add(edges[i].referenced);
    }
    // datasets are nodes too now, but assigned to a different group
    let datasets = new Set();
    nodes.forEach((name) => {
      datasets.add(getDatasetId(name));
    });

    // check for intersection, which will break network visualization
    let intersect = new Set([...nodes].filter((n) => datasets.has(n)));
    if (intersect.size > 0) {
      console.log("intersection between nodes and datasets found");
      console.log(intersect);
    }

    let nodeMap = new Map([...nodes, ...datasets].map((el, idx) => [el, idx]));

    data = {
      nodes: new DataSet(
        [...nodes]
          .map((el) => ({
            id: nodeMap.get(el),
            // we show the name without the project prefix to reduce clutter
            label: el.split(":")[1],
            group: 0,
            title: el,
          }))
          .concat(
            [...datasets].map((el) => ({
              id: nodeMap.get(el),
              label: el,
              group: 1,
              title: el,
            }))
          )
      ),
      edges: new DataSet(
        edges
          .map((el) => ({
            from: nodeMap.get(el.referenced),
            to: nodeMap.get(el.destination),
          }))
          // .slice(0, 100)
          .concat(
            [...nodes].map((el) => ({
              from: nodeMap.get(getDatasetId(el)),
              to: nodeMap.get(el),
            }))
          )
      ),
    };
  });
</script>

<style type="text/css">
  #container {
    width: 800px;
    margin: 0 auto;
  }
</style>

<div id="container">
  <h1>BigQuery ETL Query Network</h1>
  <i>Created 2020-06-18.</i>
  <p>
    This network represents the relationships between tables in BigQuery. Each
    blue node represents a table, while each orange node represents a dataset.
    The network was created by scraping the BigQuery TABLES and JOBS_BY_PROJECT
    tables in the INFORMATION_SCHEMA dataset. Views are resolved using bq with
    --dry_run.
  </p>
  <p>
    Scroll and drag to navigate the network. Selecting a node by double clicking
    will show summary information about the table or dataset.
  </p>
  <p>
    The source can be found at
    <a
      href="https://github.com/acmiyaguchi/etl-graph">acmiyaguchi/etl-graph</a>.
    See
    <a
      href="https://github.com/acmiyaguchi/etl-graph/blob/main/README.md">NOTES.md</a>
    for a detailed overview of development. This visualizaton is powered by
    <a href="https://visjs.github.io/vis-network/docs/network/">vis-network</a>.
  </p>

  {#if data}
    <Network {data} />
  {/if}
</div>
