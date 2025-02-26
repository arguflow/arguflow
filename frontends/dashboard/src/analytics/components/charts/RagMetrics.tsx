import { DateRangeFilter } from "shared/types";
import { createSignal, For, Show, useContext } from "solid-js";
import { createQuery } from "@tanstack/solid-query";
import { getRAGMetrics } from "../../api/analytics";
import { toTitleCase } from "../../utils/titleCase";
import { DateRangePicker } from "shared/ui";
import { DatasetContext } from "../../../contexts/DatasetContext";
import { subDays } from "date-fns";

export const RagMetrics = () => {
  const dataset = useContext(DatasetContext);

  const [dateRange, setDateRange] = createSignal<DateRangeFilter>({
    gt: subDays(new Date(), 7),
  });

  const ragMetricsQuery = createQuery(() => ({
    queryKey: ["ragMetrics", { filter: dateRange }],
    queryFn: () => {
      return getRAGMetrics(dateRange(), dataset.datasetId());
    },
  }));

  return (
    <div>
      <div class="flex items-baseline justify-between gap-4">
        <div>
          <div class="text-lg leading-none">Aggregate RAG Query Ratings</div>
        </div>
        <div>
          <DateRangePicker
            value={dateRange()}
            onChange={(e) => setDateRange(e)}
          />
        </div>
      </div>
      <Show when={ragMetricsQuery.data}>
        {(data) => (
          <div class="mt-4 grid grid-cols-2 gap-4">
            <For each={Object.entries(data())}>
              {([key, value]) => (
                <div class="text-center">
                  <div class="opacity-50">
                    {toTitleCase(key.split("_").join(" "))}
                  </div>
                  <div class="text-lg font-semibold">
                    {(value as number | null) ?? 0}
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
