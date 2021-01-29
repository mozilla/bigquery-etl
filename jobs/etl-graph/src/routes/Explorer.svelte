<script>
  import { onMount } from "svelte";
  import { transform, getNode } from "../utils.js";
  import Network from "../components/Network.svelte";
  import Summary from "../components/Summary.svelte";
  import { redraw } from "../store.js";
  import SearchBox from "../components/SearchBox.svelte";

  let edges;
  let nodes;
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
    nodes = await fetch("data/nodes.json").then((resp) => resp.json());
  });
</script>

{#if nodes && edges && data && selectedNode}
  <h2>Search Box</h2>
  <SearchBox {data} bind:root={selectedNode} />

  <h2>Network</h2>
  <p>
    Scroll and drag to navigate the network. Each blue node represents a table,
    while each yellow node represents a dataset. Selecting a node by clicking
    will show summary information about the table or dataset. Double click a
    node to center the network. By default, only neighboring nodes will be shown
    in the sub-network.
  </p>
  <label><input type="checkbox" bind:checked={includeDatasetNodes} />include
    dataset</label>
  <Network {data} bind:network bind:selectedNode />

  <h2>Summary</h2>
  <Summary {data} {network} bind:root={selectedNode} {nodes} />
{/if}
