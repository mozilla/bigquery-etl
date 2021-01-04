<script>
    import { onMount } from "svelte";
    import { Network } from "vis-network/peer";
    import Summary from "./Summary.svelte";

    export let data;
    export let initNodeTitle;
    export let selectedNode = getNode(initNodeTitle);

    let container;
    let network;
    let progress = 0;

    let options = {
        nodes: {
            shape: "dot",
            size: 16,
        },
        physics: {
            forceAtlas2Based: {
                gravitationalConstant: -26,
                centralGravity: 0.005,
                springLength: 230,
                springConstant: 0.18,
            },
            maxVelocity: 146,
            solver: "forceAtlas2Based",
            timestep: 0.35,
            stabilization: {
                enabled: true,
                iterations: 200,
                updateInterval: 10,
            },
        },
        layout: {
            improvedLayout: false,
        },
        interaction: {
            tooltipDelay: 200,
            hideEdgesOnDrag: true,
        },
        edges: {
            smooth: true,
            arrows: { to: true },
        },
    };

    function getNode(title) {
        return data.nodes.get({
            filter: (item) => item.title == title,
        })[0];
    }

    onMount(async () => {
        // create a network
        network = new Network(container, data, options);
        network.on("stabilizationProgress", (params) => {
            progress = Math.round((params.iterations / params.total) * 100);
        });
        network.once("stabilizationIterationsDone", () => {
            progress = 100;
        });
        network.on("selectNode", (obj) => {
            selectedNode = data.nodes.get(obj.nodes)[0];
        });
    });
</script>

<style>
    .network {
        width: 800px;
        height: 600px;
        margin: 0 auto;
        border: 1px solid lightgray;
    }
</style>

<div class="network" bind:this={container} />
{#if progress < 100}
    <p>Loading {progress}%</p>
{/if}

{#if network && selectedNode}
    <Summary {network} {data} root={selectedNode} />
{/if}
