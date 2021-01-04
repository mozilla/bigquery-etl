<script>
    export let data;
    export let network;
    export let root;
    $: output = network && root ? transform(root) : null;

    function transform(root) {
        let parents = data.nodes.get(
            network.getConnectedNodes(root.id, "from")
        );
        let children = data.nodes.get(network.getConnectedNodes(root.id, "to"));
        console.log(root);
        console.log(parents);
        // pretty output
        return {
            root: root.title,
            links: {
                references: parents.map((node) => node.title),
                destinations: children.map((node) => node.title),
            },
        };
    }
</script>

<div id="summary">
    {#if root}
        <h3>Summary of {root.label}</h3>
        <pre>{JSON.stringify(output, null, 2)}</pre>
    {/if}
</div>
