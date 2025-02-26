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
import { CrawlRequest, type Dataset } from "trieve-ts-sdk";

export const loader = async (args: LoaderFunctionArgs) => {
  const { session, sessionToken } = await authenticate.admin(args.request);
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

  let shopDataset = datasets.find(
    (d) => d.dataset.id == trieve.datasetId,
  )?.dataset;

  if (!datasetId && trieve.organizationId) {
    if (!shopDataset) {
      shopDataset = await trieve.createDataset({
        dataset_name: session.shop,
      });

      await prisma.apiKey.update({
        data: {
          currentDatasetId: shopDataset.id,
        },
        where: {
          userId_shop: {
            userId: sessionToken.sub as string,
            shop: session.shop,
          },
        },
      });
    }
    datasetId = shopDataset?.id;
  }

  if (!datasetId) {
    throw new Response("Error choosing default dataset, need to create one", {
      status: 500,
    });
  }

  trieve.datasetId = datasetId;
  const scrapingOptions = (await trieve.trieve.fetch("/api/crawl", "get", {
    datasetId: datasetId,
  })) as unknown as CrawlRequest[];

  return {
    userId: sessionToken.sub as string,
    shop: session.shop,
    shopDataset,
    datasets,
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

  await prisma.crawlSettings.upsert({
    create: {
      datasetId,
      shop: session.shop,
      crawlSettings: crawlOptions,
    },
    update: {
      crawlSettings: crawlOptions,
    },
    where: {
      datasetId_shop: {
        datasetId,
        shop: session.shop,
      },
    },
  });

  sendChunks(datasetId ?? "", trieveKey, admin, session, crawlOptions).catch(
    console.error,
  );
  return null;
};

export default function Dataset() {
  const { userId, shop, shopDataset, datasets, crawlOptions } =
    useLoaderData<typeof loader>();

  return (
    <Page>
      <Link to={`/app`}>
        <Box paddingBlockEnd="200">
          <PolLink>Back To Dataset</PolLink>
        </Box>
      </Link>
      <Text variant="headingXl" as="h2">
        {shopDataset?.name}
      </Text>
      <Box paddingBlockStart="400">
        <DatasetSettings
          initalCrawlOptions={
            crawlOptions?.crawl_options || defaultCrawlOptions
          }
          datasets={datasets}
          shopDataset={shopDataset as Dataset}
          userId={userId}
          shop={shop}
        />
      </Box>
    </Page>
  );
}
