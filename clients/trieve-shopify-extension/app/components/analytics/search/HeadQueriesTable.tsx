import { Box, Card, DataTable, Pagination, Text } from "@shopify/polaris";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useTrieve } from "app/context/trieveContext";
import { headQueriesQuery } from "app/queries/analytics/search";
import { useEffect, useState } from "react";
import { Granularity, SearchAnalyticsFilter } from "trieve-ts-sdk";

export const HeadQueriesTable = ({
  filters,
  granularity,
}: {
  filters: SearchAnalyticsFilter;
  granularity: Granularity;
}) => {
  const { trieve } = useTrieve();
  const [page, setPage] = useState(1);
  const { data } = useQuery(
    headQueriesQuery(trieve, filters, granularity, page),
  );

  const client = useQueryClient();
  useEffect(() => {
    // prefetch the next page
    client.prefetchQuery(
      headQueriesQuery(trieve, filters, granularity, page + 1),
    );
  }, [page]);

  const mappedData = data
    ? data.queries.map((query) => [query.query, query.count])
    : [];

  return (
    <Card>
      <Text as="h5" variant="headingSm">
        Most Popular Searches
      </Text>
      <Box minHeight="14px">
        <DataTable
          truncate
          increasedTableDensity
          rows={mappedData}
          columnContentTypes={["text", "numeric"]}
          headings={["Query", "Count"]}
        />
        <div className="flex justify-end">
          <Pagination
            onNext={() => {
              setPage((prevPage) => prevPage + 1);
            }}
            onPrevious={() => {
              setPage((prevPage) => prevPage - 1);
            }}
            hasPrevious={page > 1}
            hasNext={data?.queries.length == 10}
          ></Pagination>
        </div>
      </Box>
    </Card>
  );
};
