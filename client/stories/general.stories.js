import { withKnobs, select, number } from "@storybook/addon-knobs";
import SomeComponent from "../src/components/SomeComponent.svelte";

export default {
  title: "Component Template",
  decorators: [withKnobs],
};

export const Basic = () => ({
  Component: SomeComponent,
  props: {
    value: number("value", 100),
    string: select("string", ["opt 1", "opt 2", "opt 3", "opt 4"]),
  },
});
