import { DateRangeFilter } from "shared/types";
import { createSignal, For, Show, useContext } from "solid-js";
import { createQuery } from "@tanstack/solid-query";
import { getSearchMetrics } from "../../api/analytics";
import { toTitleCase } from "../../utils/titleCase";
import { DateRangePicker } from "shared/ui";
import { DatasetContext } from "../../../contexts/DatasetContext";
import { subDays } from "date-fns";

export const SearchMetrics = () => {
  const dataset = useContext(DatasetContext);

  const [dateRange, setDateRange] = createSignal<DateRangeFilter>({
    gt: subDays(new Date(), 7),
  });

  const searchMetricsQuery = createQuery(() => ({
    queryKey: ["searchMetrics", { filter: dateRange }],
    queryFn: () => {
      return getSearchMetrics(dateRange(), dataset.datasetId());
    },
  }));

  return (
    <div>
      <div class="flex items-baseline justify-between gap-4">
        <div>
          <div class="text-lg leading-none">Aggregate Search Metrics</div>
        </div>
        <div>
          <DateRangePicker
            value={dateRange()}
            onChange={(e) => setDateRange(e)}
          />
        </div>
      </div>
      <Show when={searchMetricsQuery.data}>
        {(data) => (
          <div class="mt-4 grid grid-cols-3 gap-4">
            <For each={Object.entries(data()).slice(1)}>
              {([key, value]) => (
                <div class="text-center">
                  <div class="opacity-50">
                    {toTitleCase(key.split("_").join(" "))}
                  </div>
                  <div class="text-lg font-semibold">
                    {((value as number | null) ?? 0).toFixed(2)}
                  </div>
                </div>
              )}
            </For>
          </div>
        )}
      </Show>
    </div>
  );
};
