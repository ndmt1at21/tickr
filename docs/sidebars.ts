import type { SidebarsConfig } from "@docusaurus/plugin-content-docs";

const sidebars: SidebarsConfig = {
  docs: [
    "intro",
    "getting-started",
    {
      type: "category",
      label: "API reference",
      collapsed: false,
      items: ["producer", "consumer", "handlers"],
    },
    "configuration",
    "observability",
  ],
};

export default sidebars;
