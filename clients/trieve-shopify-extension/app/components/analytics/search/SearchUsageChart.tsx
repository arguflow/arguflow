import { Box, Card, Text } from "@shopify/polaris";
import { useQuery } from "@tanstack/react-query";
import { useTrieve } from "app/context/trieveContext";
import { searchUsageQuery } from "app/queries/analytics/search";
import { SearchAnalyticsFilter } from "trieve-ts-sdk";
import { Granularity } from "trieve-ts-sdk";
import { AnalyticsChart } from "../AnalyticsChart";

export const SearchUsageChart = ({
  filters,
  granularity,
}: {
  filters: SearchAnalyticsFilter;
  granularity: Granularity;
}) => {
  const { trieve } = useTrieve();
  const { data } = useQuery(searchUsageQuery(trieve, filters, granularity));

  return (
    <Card>
      <Text as="h5" variant="headingSm">
        Search Usage
      </Text>
      <Box minHeight="14px">
        <AnalyticsChart
          wholeUnits
          data={data?.usage_points}
          xAxis={"time_stamp"}
          yAxis={"requests"}
          granularity="day"
          yLabel="Requests"
          date_range={filters.date_range}
        />
      </Box>
    </Card>
  );
};
