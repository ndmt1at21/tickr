import { themes as prismThemes } from "prism-react-renderer";
import type { Config } from "@docusaurus/types";
import type * as Preset from "@docusaurus/preset-classic";

const config: Config = {
  title: "tickr",
  tagline:
    "Reliable async messaging for Go microservices — transactional outbox without an external broker.",
  favicon: "img/logo.png",

  url: "https://ndmt1at21.github.io",
  baseUrl: "/tickr/",

  organizationName: "ndmt1at21",
  projectName: "tickr",

  onBrokenLinks: "warn",
  markdown: {
    hooks: {
      onBrokenMarkdownLinks: "warn",
    },
  },

  i18n: {
    defaultLocale: "en",
    locales: ["en"],
  },

  presets: [
    [
      "classic",
      {
        docs: {
          routeBasePath: "/",
          sidebarPath: "./sidebars.ts",
          editUrl: "https://github.com/ndmt1at21/tickr/edit/main/docs/",
        },
        blog: false,
        theme: {
          customCss: "./src/css/custom.css",
        },
      } satisfies Preset.Options,
    ],
  ],

  themeConfig: {
    colorMode: {
      defaultMode: "dark",
      respectPrefersColorScheme: true,
    },
    navbar: {
      title: "tickr",
      logo: {
        alt: "tickr",
        src: "img/logo.png",
      },
      items: [
        {
          type: "docSidebar",
          sidebarId: "docs",
          position: "left",
          label: "Docs",
        },
        {
          href: "https://pkg.go.dev/github.com/ndmt1at21/tickr",
          label: "pkg.go.dev",
          position: "right",
        },
        {
          href: "https://github.com/ndmt1at21/tickr",
          label: "GitHub",
          position: "right",
        },
      ],
    },
    footer: {
      style: "dark",
      links: [
        {
          title: "Docs",
          items: [
            { label: "Getting started", to: "/getting-started" },
            { label: "Producer", to: "/producer" },
            { label: "Consumer", to: "/consumer" },
            { label: "Configuration", to: "/configuration" },
            { label: "Observability", to: "/observability" },
          ],
        },
        {
          title: "Project",
          items: [
            { label: "GitHub", href: "https://github.com/ndmt1at21/tickr" },
            {
              label: "pkg.go.dev",
              href: "https://pkg.go.dev/github.com/ndmt1at21/tickr",
            },
            {
              label: "Releases",
              href: "https://github.com/ndmt1at21/tickr/releases",
            },
            {
              label: "Issues",
              href: "https://github.com/ndmt1at21/tickr/issues",
            },
          ],
        },
        {
          title: "Reference",
          items: [
            {
              label: "ARCHITECTURE.md",
              href: "https://github.com/ndmt1at21/tickr/blob/main/ARCHITECTURE.md",
            },
            {
              label: "BENCHMARKS.md",
              href: "https://github.com/ndmt1at21/tickr/blob/main/BENCHMARKS.md",
            },
          ],
        },
      ],
      copyright: `Copyright © ${new Date().getFullYear()} tickr. MIT licensed.`,
    },
    prism: {
      theme: prismThemes.github,
      darkTheme: prismThemes.dracula,
      additionalLanguages: ["go", "sql", "bash", "yaml", "json"],
    },
  } satisfies Preset.ThemeConfig,
};

export default config;
