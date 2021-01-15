<script>
    import { Nav, NavItem, NavLink } from "sveltestrap";
    import { pagePathname } from "../store.js";

    let items = [{ href: "", name: "explorer" }];
</script>

<Nav class="ml-auto" tabs>
    {#each items as item}
        <NavItem>
            <NavLink active={item.href == $pagePathname} href={item.href}>
                {item.name}
            </NavLink>
        </NavItem>
    {/each}
</Nav>
