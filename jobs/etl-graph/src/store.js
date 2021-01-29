import { writable } from "svelte/store";

export const redraw = writable(true);
export const pagePathname = writable("");
