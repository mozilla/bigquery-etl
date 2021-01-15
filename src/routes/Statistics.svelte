<script>
    import { onMount } from "svelte";

    let edges;

    onMount(async () => {
        edges = await fetch("data/edges.json").then((resp) => resp.json());
    });

    function nodeLength(edges) {
        let set = new Set();
        for (let i = 0; i < edges.length; i++) {
            set.add(edges[i].reference);
            set.add(edges[i].destination);
        }
        return set.size;
    }
</script>

<h2>Network Statistics</h2>
<p>This page contains statistics about the network.</p>

{#if edges}There are {nodeLength(edges)} nodes and {edges.length} edges.{/if}
