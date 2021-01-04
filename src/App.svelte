<script>
  import { onMount } from "svelte";
  import { DataSet } from "vis-data/peer";
  import FrontMatter from "./FrontMatter.md";
  import Network from "./Network.svelte";
  import Summary from "./Summary.svelte";

  let data;
  let network = null;
  let selectedNode = null;

  // view-source:https://visjs.github.io/vis-network/examples/network/exampleApplications/loadingBar.html
  function getDatasetId(name) {
    return name.split(".")[0];
  }

  function getNode(data, title) {
    return data.nodes.get({
      filter: (item) => item.title == title,
    })[0];
  }

  function transform(edges) {
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
    return {
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
  }

  onMount(async () => {
    let edges = await fetch("data/edges.json").then((resp) => resp.json());
    data = transform(edges);
    selectedNode = getNode(
      data,
      "moz-fx-data-shared-prod:telemetry_stable.main_v4"
    );
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

  <FrontMatter />

  {#if data}
    <Network {data} bind:network bind:selectedNode />
    <Summary {data} {network} root={selectedNode} />
  {/if}
</div>
