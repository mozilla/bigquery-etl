<script>
    import Routes from "./routes/Routes.svelte";
    import Nav from "./components/Nav.svelte";
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
    <Nav />
    <Routes />
</main>
