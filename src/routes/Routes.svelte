<script>
    import page from "page";
    import Explorer from "./Explorer.svelte";
    import Statistics from "./Statistics.svelte";
    import Artifacts from "./Artifacts.svelte";
    import { pagePathname } from "../store.js";

    let component;

    function setComponent(c) {
        return ({ pathname: pathname }) => {
            component = c;
            // set the store so we can show the active page in the navbar
            pagePathname.set(pathname);
        };
    }

    page("/", setComponent(Explorer));
    page("/statistics", setComponent(Statistics));
    page("/artifacts", setComponent(Artifacts));
    page({ hashbang: false });
</script>

<svelte:component this={component} />
