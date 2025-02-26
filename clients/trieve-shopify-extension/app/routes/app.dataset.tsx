import { LoaderFunctionArgs } from "@remix-run/node";
import { Link, useLoaderData } from "@remix-run/react";
import { Page, Text, Link as PolLink, Box } from "@shopify/polaris";
import { initTrieveSdk, validateTrieveAuth } from "app/auth";
import {
  defaultCrawlOptions,
  DatasetSettings as DatasetSettings,
  ExtendedCrawlOptions,
} from "app/components/CrawlSettings";
import { sendChunks } from "app/processors/getProducts";
import { authenticate } from "app/shopify.server";
import { CrawlRequest } from "trieve-ts-sdk";

export const loader = async (args: LoaderFunctionArgs) => {
  const trieve = await initTrieveSdk(args);

  if (!trieve.organizationId) {
    throw new Response("Unautorized, no organization tied to user session", {
      status: 401,
    });
  }

  const datasets = await trieve.getDatasetsFromOrganization(
    trieve.organizationId,
  );
  let datasetId = trieve.datasetId;
  if (!datasetId && trieve.organizationId) {
    datasetId = datasets[0].dataset.id;
  }
  if (!datasetId) {
    throw new Response("Error choosing default dataset, need to create one", {
      status: 500,
    });
  }
  let datasetUsage = datasets.find((d) => datasetId == d.dataset.id);
  if (!datasetUsage) {
    throw new Response("Error choosing default dataset from datasetId", {
      status: 500,
    });
  }

  trieve.datasetId = datasetUsage?.dataset.id;
  const scrapingOptions = (await trieve.trieve.fetch("/api/crawl", "get", {
    datasetId: datasetId,
  })) as unknown as CrawlRequest[];

  return {
    datasets: datasets,
    currentDatasetUsage: datasetUsage,
    crawlOptions: scrapingOptions[0],
  };
};

export const action = async (data: LoaderFunctionArgs) => {
  const { admin, session } = await authenticate.admin(data.request);
  const trieveKey = await validateTrieveAuth(data);
  let formData = await data.request.formData();

  const crawlOptions: ExtendedCrawlOptions =
    (JSON.parse(
      formData.get("crawl_options") as string,
    ) as ExtendedCrawlOptions) ?? defaultCrawlOptions;
  const datasetId = formData.get("dataset_id") as string;

  sendChunks(datasetId ?? "", trieveKey, admin, session, crawlOptions).catch(
    console.error,
  );
  return null;
};

export default function Dataset() {
  const { currentDatasetUsage, crawlOptions, datasets } =
    useLoaderData<typeof loader>();

  return (
    <Page>
      <Link to={`/app`}>
        <Box paddingBlockEnd="200">
          <PolLink>Back To Dataset</PolLink>
        </Box>
      </Link>
      <Text variant="headingXl" as="h2">
        {currentDatasetUsage?.dataset.name}
      </Text>
      <Box paddingBlockStart="400">
        <DatasetSettings
          initalCrawlOptions={
            crawlOptions?.crawl_options || defaultCrawlOptions
          }
          datasets={datasets}
          currentDatasetUsage={currentDatasetUsage}
        />
      </Box>
    </Page>
  );
}
