<script>
    import page from "page";
    import Explorer from "./Explorer.svelte";
    import Statistics from "./Statistics.svelte";
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
    page({ hashbang: true });
</script>

<svelte:component this={component} />
