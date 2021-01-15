<script>
    import Fuse from "fuse.js";
    import { redraw } from "./store.js";

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

<input bind:value={term} on:input={() => search(term)} />

<div>
    {#if results}
        <table>
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
                        <td>{row.score}</td>
                    </tr>
                {/each}
            </tbody>
        </table>
    {/if}
</div>
