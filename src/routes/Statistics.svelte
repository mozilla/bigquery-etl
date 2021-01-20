<script>
    import { onMount } from "svelte";
    import { zip, countBy } from "lodash";

    let stats;
    let degreeElement;

    function transform(degree) {
        // returns frequency of degree values
        let data = zip(...Object.entries(countBy(degree)));
        return {
            x: data[0].map((x) => parseInt(x)),
            y: data[1],
        };
    }

    function plot(element, data) {
        if (!element || !data) {
            return;
        }
        Plotly.plot(
            element,
            ["in_degree", "out_degree", "degree"].map((name) => ({
                ...transform(stats[name]),
                type: "scatter",
                name: name,
            })),
            {
                title: "Degree Distribution",
                xaxis: { type: "log", title: "degree" },
                yaxis: { type: "log", title: "frequency" },
            },
            { responsive: true }
        );
    }

    $: plot(degreeElement, stats);

    onMount(async () => {
        stats = await fetch("data/network_stats.json").then((resp) =>
            resp.json()
        );
    });
</script>

<h2>Network Statistics</h2>
<p>
    This page contains statistics about the network. These are generated with
    <a href="https://networkx.org/">NetworkX</a>
    on the query network. Nodes are tables and edges are references within
    queries.
</p>

{#if stats}
    <table class="table table-sm">
        <thead>
            <tr>
                <th>statistic</th>
                <th>value</th>
            </tr>
        </thead>
        <tbody>
            {#each ['number_of_nodes', 'number_of_edges', 'avg_in_degree', 'avg_out_degree', 'avg_degree', 'number_strongly_connected_components', 'number_weakly_connected_components'] as key}
                <tr>
                    <td>{key}</td>
                    <td>{stats[key]}</td>
                </tr>
            {/each}
        </tbody>
    </table>

    <div bind:this={degreeElement} />
{/if}
