<script>
    import { redraw } from "../store.js";
    import CodeMirror from "codemirror/lib/codemirror.js";
    import Tabulator from "tabulator-tables";
    import "codemirror/mode/sql/sql.js";

    export let data;
    export let network;
    export let root;
    // node summary information
    export let nodes;

    let tableElement;
    let editorElement;
    let editor;
    let table;

    $: output = network && root ? transform(root) : null;
    $: nodeInfo =
        root && nodes
            ? nodes.find((row) => row.destination_table == root.title)
            : null;

    $: editorElement &&
        !editor &&
        (editor = CodeMirror(editorElement, {
            value: "",
            lineNumbers: true,
            mode: "text/x-sql",
        }));
    $: editor && nodeInfo && editor.setValue(nodeInfo.query);
    $: tableElement &&
        nodeInfo &&
        (table = new Tabulator(tableElement, {
            data: nodeInfo.most_recent_jobs,
            layout: "fitDataFill",
            initialSort: [{ column: "creation_time", dir: "desc" }],
            autoColumns: true,
        }));
    $: table && !nodeInfo && table.destroy();

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
        {#if nodeInfo}
            <h4>Query</h4>
            <div
                bind:this={editorElement}
                style={nodeInfo ? '' : 'display:none'} />
            <h4>Job History (last 10 runs)</h4>
            <p>
                Run the following command in the command line to see more
                information:
                <code>bq show -j {root.title.split(':')[0]}:$job_id</code>
            </p>
            <div bind:this={tableElement} />
        {:else}
            <p>No associated query information found.</p>
        {/if}
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
