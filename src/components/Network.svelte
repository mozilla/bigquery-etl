<script>
    import { Network } from "vis-network/peer";
    import { redraw } from "../store.js";

    export let data;
    export let network;
    export let selectedNode;

    let container;
    let progress = 0;

    $: data && container && $redraw && transform(data, selectedNode);

    let options = {
        nodes: {
            shape: "dot",
            size: 16,
        },
        physics: {
            barnesHut: { gravitationalConstant: -30000 },
            stabilization: { iterations: 1000, updateInterval: 100 },
        },
        layout: {
            improvedLayout: false,
        },
        interaction: {
            tooltipDelay: 200,
            hideEdgesOnDrag: true,
        },
        edges: {
            arrows: { to: true },
        },
    };

    function transform(data, center) {
        progress = 0;
        network = new Network(container, data, options);
        let firstOrder = network
            .getConnectedNodes(center.id)
            .concat([center.id]);
        let set = new Set(firstOrder);
        network.clustering.cluster({
            joinCondition: (options) => {
                return !set.has(options.id);
            },
        });

        network.on("stabilizationProgress", (params) => {
            progress = Math.round((params.iterations / params.total) * 100);
        });
        network.on("stabilizationIterationsDone", () => {
            progress = 100;
        });
        network.on("selectNode", (obj) => {
            let node = data.nodes.get(obj.nodes)[0];
            if (!node) {
                return;
            }
            // select a node, but don't redraw
            selectedNode = node;
        });
        network.on("doubleClick", (obj) => {
            let node = data.nodes.get(obj.nodes)[0];
            if (!node) {
                return;
            }
            selectedNode = node;
            redraw.set(true);
        });
        redraw.set(false);
    }
</script>

<style>
    .network {
        height: 600px;
        margin: 0 auto;
        border: 1px solid lightgray;
    }
</style>

<div class="network" bind:this={container} />
{#if progress < 100}
    <p>Loading {progress}%</p>
{/if}
