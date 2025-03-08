import { Box, Card, DataTable, Pagination, Text } from "@shopify/polaris";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useTrieve } from "app/context/trieveContext";
import {
  headQueriesQuery,
  noResultQueriesQuery,
} from "app/queries/analytics/search";
import { useEffect, useState } from "react";
import { Granularity, SearchAnalyticsFilter } from "trieve-ts-sdk";

export const NoResultQueriesTable = ({
  filters,
  granularity,
}: {
  filters: SearchAnalyticsFilter;
  granularity: Granularity;
}) => {
  const { trieve } = useTrieve();
  const [page, setPage] = useState(1);
  const { data } = useQuery(
    noResultQueriesQuery(trieve, filters, granularity, page),
  );

  const client = useQueryClient();
  useEffect(() => {
    // prefetch the next page
    client.prefetchQuery(
      noResultQueriesQuery(trieve, filters, granularity, page + 1),
    );
  }, [page]);

  const mappedData = data ? data.queries.map((query) => [query.query]) : [];

  return (
    <Card>
      <Text as="h5" variant="headingSm">
        Queries with No Results
      </Text>
      <Box minHeight="14px">
        <DataTable
          truncate
          increasedTableDensity
          rows={mappedData}
          columnContentTypes={["text", "numeric"]}
          headings={["Query"]}
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
