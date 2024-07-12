import { Show } from "solid-js";
/* @refresh reload */
import { render } from "solid-js/web";

import "./index.css";
import { RouteDefinition, Router } from "@solidjs/router";
import { SearchAnalyticsPage } from "./pages/SearchAnalyticsPage";
import { UserAuthContextProvider } from "./contexts/UserAuthContext";
import { QueryClient, QueryClientProvider } from "@tanstack/solid-query";
import { TrendExplorer } from "./pages/TrendExplorer";
import { Chart, registerables } from "chart.js";
import { SolidQueryDevtools } from "@tanstack/solid-query-devtools";
import { OverviewPage } from "./pages/OverviewPage";
import { RagAnalyticsPage } from "./pages/RagAnalyticsPage";

const queryClient = new QueryClient();

Chart.register(...registerables);

const routes: RouteDefinition[] = [
  {
    path: "/",
    component: UserAuthContextProvider,
    children: [
      {
        path: "/",
        component: OverviewPage,
      },
      {
        path: "/analytics",
        component: SearchAnalyticsPage,
      },
      {
        path: "/rag",
        component: RagAnalyticsPage,
      },
      {
        path: "/trends",
        component: TrendExplorer,
      },
    ],
  },
];

const root = document.getElementById("root");

render(
  () => (
    <QueryClientProvider client={queryClient}>
      <Router>{routes}</Router>
      <Show when={import.meta.env.DEV}>
        <SolidQueryDevtools initialIsOpen={false} />
      </Show>
    </QueryClientProvider>
  ),
  // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
  root!,
);
