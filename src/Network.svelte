<script>
    import { onMount } from "svelte";
    import { Network } from "vis-network/peer";

    export let data;

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
        let container = document.getElementById("mynetwork");
        let network = new Network(container, data, options);
        network.on("stabilizationProgress", function (params) {
            document.getElementById("progress").innerHTML =
                Math.round((params.iterations / params.total) * 100) + "%";
        });

        function setSummary(root) {
            if (root.label == null) {
                console.log("did not set summary");
                return;
            }
            let div = document.getElementById("summary");
            // clear the summary
            div.innerHTML = "";
            // dump stringified json into the summary
            let parents = data.nodes.get(
                network.getConnectedNodes(root.id, "from")
            );
            let children = data.nodes.get(
                network.getConnectedNodes(root.id, "to")
            );
            console.log(root);
            console.log(parents);
            // pretty output
            let output = {
                root: root.title,
                links: {
                    references: parents.map((node) => node.title),
                    destinations: children.map((node) => node.title),
                },
            };

            div.innerHTML =
                `<h3>Summary of ${root.label} </h3><pre>` +
                JSON.stringify(output, null, 2) +
                "</pre>";
        }

        network.on("selectNode", (obj) => {
            let root = data.nodes.get(obj.nodes)[0];
            setSummary(root);
        });
        network.once("stabilizationIterationsDone", function () {
            document.getElementById("progress").innerHTML = "100%";
            setTimeout(function () {
                document.getElementById("status").style.display = "none";
                // set a default summary
                setSummary(
                    data.nodes.get(
                        nodeMap.get(
                            "moz-fx-data-shared-prod:telemetry_stable.main_v4"
                        )
                    )
                );
            }, 500);
        });
    });
</script>

<style>
    #mynetwork {
        width: 800px;
        height: 600px;
        margin: 0 auto;
        border: 1px solid lightgray;
    }
</style>

<div id="mynetwork" />
<p id="status">Loading <span id="progress">0%</span></p>
<div id="summary" />
