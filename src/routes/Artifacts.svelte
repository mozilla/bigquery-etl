<script>
    import { onMount } from "svelte";

    let data;

    onMount(async () => {
        data = await fetch("data/manifest.json").then((resp) => resp.json());
    });
</script>

<h2>Artifacts</h2>

<p>Download the generated intermediate files.</p>

{#if data}
    <ul>
        {#each data as item}
            <li>
                <a href={item.path}>{item.path}
                    -
                    {(item.size_bytes / 1000).toFixed(1)}
                    kb</a>
            </li>
        {/each}
    </ul>
{/if}
