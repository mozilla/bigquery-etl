<script>
  import { onMount } from "svelte";
  import { transform, getNode } from "./utils.js";
  import FrontMatter from "./FrontMatter.md";
  import Network from "./Network.svelte";
  import Summary from "./Summary.svelte";
  import { redraw } from "./store.js";

  let edges;
  let includeDatasetNodes = true;
  let data;

  let network = null;
  let selectedNode = null;
  // set redraw anytime this variable changes
  $: includeDatasetNodes ? redraw.set(true) : redraw.set(true);

  // The changes only when we fetch the data or modify some parameters
  $: data = edges ? transform(edges, includeDatasetNodes) : null;
  // initialize the selection
  $: selectedNode =
    !selectedNode && data
      ? getNode(data, "moz-fx-data-shared-prod:telemetry_stable.main_v4")
      : selectedNode;

  onMount(async () => {
    edges = await fetch("data/edges.json").then((resp) => resp.json());
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

  <label><input type="checkbox" bind:checked={includeDatasetNodes} />include
    dataset</label>

  {#if data && selectedNode}
    <Network {data} bind:network bind:selectedNode />
    <Summary {data} {network} bind:root={selectedNode} />
  {/if}
</div>
