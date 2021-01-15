<script>
    import Fuse from "fuse.js";
    import { FormGroup, Label, Input, Table } from "sveltestrap";
    import { redraw } from "../store.js";

    export let data;
    export let root;

    let fuse;
    let term;
    let results;
    let timeout;

    $: data &&
        (fuse = new Fuse(data.nodes.get(), {
            keys: ["label"],
            includeScore: true,
        }));

    function search(term) {
        clearTimeout(timeout);
        timeout = setTimeout(() => {
            console.log("search");
            results = fuse.search(term).slice(0, 10);
        }, 750);
    }
</script>

<FormGroup>
    <Label>Search for a dataset or table.</Label>
    <Input
        type="text"
        placeholder="moz-fx-data-shared-prod.telemetry.main_v4"
        bind:value={term}
        on:input={() => search(term)} />
</FormGroup>

<div>
    {#if results}
        <Table bordered class="table-sm">
            <thead>
                <tr>
                    <th>title</th>
                    <th>score</th>
                </tr>
            </thead>
            <tbody>
                {#each results as row}
                    <tr>
                        <td>
                            <a
                                href="javascript:void(0)"
                                on:click={() => {
                                    root = row.item;
                                    redraw.set(true);
                                }}>{row.item.title}</a>
                        </td>
                        <td>{row.score.toFixed(2)}</td>
                    </tr>
                {/each}
            </tbody>
        </Table>
    {/if}
</div>
