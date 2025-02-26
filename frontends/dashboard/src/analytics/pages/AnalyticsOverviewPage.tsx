import { subHours } from "date-fns/subHours";
import { Card } from "../components/charts/Card";
import { HeadQueries } from "../components/charts/HeadQueries";
import { QueryCounts } from "../components/charts/QueryCounts";
import { SearchUsageGraph } from "../components/charts/SearchUsageGraph";
import { CTRInfoPanel } from "../components/CTRInfoPanel";
import { DateRangeFilter } from "shared/types";
import { createSignal } from "solid-js";
import { DateRangePicker } from "shared/ui";
import { Granularity } from "trieve-ts-sdk";
import { SearchMetrics } from "../components/charts/SearchMetrics";
import { RagMetrics } from "../components/charts/RagMetrics";

export const AnalyticsOverviewPage = () => {
  const [rpsDateRange, setRpsDateRange] = createSignal<DateRangeFilter>({
    gt: subHours(new Date(), 1),
  });

  const [rpsGranularity, setRpsGranularity] =
    createSignal<Granularity>("minute");

  const [headQueriesDate, setHeadQueriesDate] = createSignal<DateRangeFilter>({
    gt: subHours(new Date(), 1),
  });

  return (
    <>
      <div class="grid grid-cols-2 items-start gap-2">
        <div class="col-span-2">
          <CTRInfoPanel />
        </div>
        <Card class="flex flex-col justify-between px-4" width={2}>
          <QueryCounts />
        </Card>
        <Card class="flex flex-col justify-between px-4" width={2}>
          <SearchMetrics />
        </Card>
        <Card class="flex flex-col justify-between px-4" width={2}>
          <RagMetrics />
        </Card>
        <Card
          title="Requests Per Second"
          controller={
            <DateRangePicker
              onChange={(e) => setRpsDateRange(e)}
              value={rpsDateRange()}
              initialSelectedPresetId={3}
              onGranularitySuggestion={(e) => setRpsGranularity(e)}
            />
          }
          class="flex flex-col justify-between px-4"
          width={1}
        >
          <SearchUsageGraph
            params={{
              filter: { date_range: rpsDateRange() },
              granularity: rpsGranularity(),
            }}
          />
        </Card>

        <Card
          controller={
            <DateRangePicker
              onChange={(e) => setHeadQueriesDate(e)}
              initialSelectedPresetId={3}
              value={headQueriesDate()}
            />
          }
          title="Head Queries"
          class="px-4"
          width={1}
        >
          <HeadQueries
            params={{
              filter: { date_range: headQueriesDate() },
            }}
          />
        </Card>
      </div>
    </>
  );
};
