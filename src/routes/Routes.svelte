<script>
    import page from "page";
    import Explorer from "./Explorer.svelte";
    import { pagePathname } from "../store.js";

    let component;

    function setComponent(c) {
        return ({ pathname: pathname }) => {
            component = c;
            // set the store so we can show the active page in the navbar
            console.log(pathname);
            pagePathname.set(pathname);
        };
    }

    page("/", setComponent(Explorer));
    page({ hashbang: true });
</script>

<svelte:component this={component} />
