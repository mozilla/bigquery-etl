<script>
    import { redraw } from "../store.js";

    export let data;
    export let network;
    export let root;

    $: output = network && root ? transform(root) : null;

    function transform(root) {
        let parents = data.nodes.get(
            network.getConnectedNodes(root.id, "from")
        );
        let children = data.nodes.get(network.getConnectedNodes(root.id, "to"));
        return {
            root: root.title,
            links: {
                references: parents,
                destinations: children,
            },
        };
    }
</script>

<div id="summary">
    {#if output}
        <h3>{root.title}</h3>
        <h4>References</h4>
        <ul>
            {#each output.links.references as node}
                <li>
                    <a
                        href="javascript:void(0)"
                        on:click={() => {
                            root = node;
                            redraw.set(true);
                        }}>{node.title}</a>
                </li>
            {/each}
        </ul>
        <h4>Destinations</h4>
        <ul>
            {#each output.links.destinations as node}
                <li>
                    <a
                        href="javascript:void(0)"
                        on:click={() => {
                            root = node;
                            redraw.set(true);
                        }}>{node.title}</a>
                </li>
            {/each}
        </ul>
    {/if}
</div>
