<script>
    import Explorer from "./routes/Explorer.svelte";
    import FrontMatter from "./components/FrontMatter.md";
</script>

<style type="text/css">
    main {
        width: 900px;
        margin: 0 auto;
    }
</style>

<main>
    <h1>BigQuery ETL Query Network</h1>
    <FrontMatter />
    <Explorer />
</main>
