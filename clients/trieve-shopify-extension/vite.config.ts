import { vitePlugin as remix } from "@remix-run/dev";
import { installGlobals } from "@remix-run/node";
import { defineConfig, type UserConfig, loadEnv } from "vite";
import tsconfigPaths from "vite-tsconfig-paths";

installGlobals({ nativeFetch: true });

// Related: https://github.com/remix-run/remix/issues/2835#issuecomment-1144102176
// Replace the HOST env var with SHOPIFY_APP_URL so that it doesn't break the remix server. The CLI will eventually
// stop passing in HOST, so we can remove this workaround after the next major release.
if (
  process.env.HOST &&
  (!process.env.SHOPIFY_APP_URL ||
    process.env.SHOPIFY_APP_URL === process.env.HOST)
) {
  process.env.SHOPIFY_APP_URL = process.env.HOST;
  delete process.env.HOST;
}

const host = new URL(process.env.SHOPIFY_APP_URL || "http://localhost")
  .hostname;

let hmrConfig;
if (host === "localhost") {
  hmrConfig = {
    protocol: "ws",
    host: "localhost",
    port: 64999,
    clientPort: 64999,
  };
} else {
  hmrConfig = {
    protocol: "wss",
    host: host,
    port: parseInt(process.env.FRONTEND_PORT!) || 8002,
    clientPort: 443,
  };
}

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), "");
  return {
    server: {
      host: "0.0.0.0",
      port: Number(process.env.PORT || 3000),
      hmr: hmrConfig,
      fs: {
        // See https://vitejs.dev/config/server-options.html#server-fs-allow for more information
        allow: ["app", "node_modules"],
      },
      allowedHosts: [
        (process.env.TRIEVE_AUTH_URL || env.TRIEVE_AUTH_URL)!.replace(
          "https://",
          ""
        ),
      ],
    },
    plugins: [
      remix({
        ignoredRouteFiles: ["**/.*"],
        future: {
          v3_fetcherPersist: true,
          v3_relativeSplatPath: true,
          v3_throwAbortReason: true,
          v3_lazyRouteDiscovery: true,
          v3_singleFetch: false,
          v3_routeConfig: true,
        },
      }),
      tsconfigPaths(),
    ],
    build: {
      assetsInlineLimit: 0,
    },
    ssr: {
      noExternal: [
        "@shopify/polaris-viz",
        "@juggle/resize-observer",
        "@react-spring/animated",
        "@react-spring/core",
        "@react-spring/shared",
        "@react-spring/types",
        "@react-spring/web",
        "@shopify/polaris-viz",
        "@shopify/polaris-viz-core",
        "d3-array",
        "d3-color",
        "d3-format",
        "d3-interpolate",
        "d3-path",
        "d3-scale",
        "d3-shape",
        "d3-time",
        "d3-time-format",
        "internmap",
        "use-debounce",
      ],
    },
  } satisfies UserConfig;
});
