<script>
    import { onMount } from "svelte";
    import { Network } from "vis-network/peer";
    import Summary from "./Summary.svelte";

    export let nodeMap;
    export let data;

    let container;
    let network;
    let selectedNode;

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

    onMount(async () => {
        // create a network
        network = new Network(container, data, options);
        network.on("stabilizationProgress", function (params) {
            document.getElementById("progress").innerHTML =
                Math.round((params.iterations / params.total) * 100) + "%";
        });

        network.on("selectNode", (obj) => {
            selectedNode = data.nodes.get(obj.nodes)[0];
        });
        network.once("stabilizationIterationsDone", function () {
            document.getElementById("progress").innerHTML = "100%";
            setTimeout(function () {
                document.getElementById("status").style.display = "none";
                // set a default summary
                selectedNode = data.nodes.get(
                    nodeMap.get(
                        "moz-fx-data-shared-prod:telemetry_stable.main_v4"
                    )
                );
            }, 500);
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
<p id="status">Loading <span id="progress">0%</span></p>

{#if selectedNode}
    <Summary {network} {data} root={selectedNode} />
{/if}
