import { Granularity } from "trieve-ts-sdk";
import { SearchAnalyticsFilter } from "trieve-ts-sdk";
import { DateRangePicker } from "../DateRangePicker";
import {
  formatStringDateRangeToDates,
  transformDateParams,
} from "app/queries/analytics/formatting";
import { Box } from "@shopify/polaris";

interface SearchFilterBarProps {
  granularity: Granularity;
  setGranularity: (granularity: Granularity) => void;
  filters: SearchAnalyticsFilter;
  setFilters: (filters: SearchAnalyticsFilter) => void;
}
export const SearchFilterBar = (props: SearchFilterBarProps) => {
  return (
    <div className="flex py-4 justify-end">
      <Box maxWidth="200">
        <DateRangePicker
          value={formatStringDateRangeToDates(props.filters.date_range)}
          onChange={(s) => {
            props.setFilters({
              ...props.filters,
              date_range: transformDateParams(s),
            });
          }}
        />
      </Box>
    </div>
  );
};
