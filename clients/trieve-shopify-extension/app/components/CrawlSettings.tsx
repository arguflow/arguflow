import { useSubmit } from "@remix-run/react";
import { useAppBridge } from "@shopify/app-bridge-react";
import {
  BlockStack,
  Button,
  Card,
  Checkbox,
  FormLayout,
  InlineStack,
  Select,
  Text,
  TextField,
} from "@shopify/polaris";
import { useEffect, useState } from "react";
import {
  CrawlInterval,
  CrawlOptions,
  Dataset,
  DatasetAndUsage,
  DatasetDTO,
} from "trieve-ts-sdk";

export type ExtendedCrawlOptions = Omit<CrawlOptions, "webhook_metadata"> & {
  include_metafields?: string[];
  scrape_options: {
    type: "shopify";
  };
};

export const defaultCrawlOptions: ExtendedCrawlOptions = {
  boost_titles: true,
  interval: "daily",
  limit: 1000,
  scrape_options: {
    type: "shopify",
    group_variants: false,
    tag_regexes: [],
  },
};

export const DatasetSettings = ({
  initalCrawlOptions,
  datasets,
  shopDataset,
  userId,
  shop,
}: {
  initalCrawlOptions: ExtendedCrawlOptions;
  datasets: DatasetAndUsage[];
  shopDataset: Dataset;
  userId: string;
  shop: string;
}) => {
  const [unsavedCrawlOptions, setUnsavedCrawlOptions] =
    useState(initalCrawlOptions);
  const shopify = useAppBridge();
  const submit = useSubmit();

  useEffect(() => {
    // Quickly set the nonnegotiable options for shopify to work
    setUnsavedCrawlOptions({
      ...unsavedCrawlOptions,
      boost_titles: true,
      scrape_options: {
        ...unsavedCrawlOptions.scrape_options,
        group_variants: true,
        tag_regexes: [],
        type: "shopify",
      },
    });

    if (!unsavedCrawlOptions.interval) {
      setUnsavedCrawlOptions({
        ...unsavedCrawlOptions,
        interval: "daily",
      });
    }
  }, [initalCrawlOptions]);

  const onSave = async () => {
    submit(
      {
        crawl_options: JSON.stringify(unsavedCrawlOptions),
        dataset_id: shopDataset.id,
      },
      {
        method: "POST",
      },
    );

    shopify.toast.show("Started crawl!");
  };

  const updateDefaultDataset = async (
    datasetId: string,
    userId: string,
    shop: string,
  ) => {
    const dataset = datasets.find((d) => d.dataset.id == datasetId);
    shopify.toast.show(`Indexed Dataset changed to ${dataset?.dataset.name}`);

    prisma.apiKey.update({
      data: {
        currentDatasetId: shopDataset.id,
      },
      where: {
        userId_shop: {
          userId,
          shop,
        },
      },
    });
  };

  return (
    <BlockStack gap="200">
      {datasets.length > 1 && (
        <Card>
          <Text variant="headingLg" as="h1">
            Index Settings
          </Text>

          <Select
            label="Dataset Index"
            onChange={(dataset) => {
              updateDefaultDataset(dataset, userId, shop);
            }}
            value={shopDataset.id}
            options={datasets.map((dataset) => {
              return { label: dataset.dataset.name, value: dataset.dataset.id };
            })}
          />
        </Card>
      )}

      <Card>
        <BlockStack gap="200">
          <Text variant="headingLg" as="h1">
            Crawl Settings
          </Text>

          <FormLayout>
            <TextField
              autoComplete="off"
              label="Important Product Tags (Comma Seperated)"
              helpText="Regex pattern of tags to use from the Shopify API, e.g. 'Men' to include 'Men' if it exists in a product tag."
              value={
                unsavedCrawlOptions.scrape_options?.tag_regexes?.join(",") || ""
              }
              onChange={(e) => {
                setUnsavedCrawlOptions({
                  ...unsavedCrawlOptions,
                  scrape_options: {
                    ...unsavedCrawlOptions.scrape_options,
                    tag_regexes: e.split(",").map((s) => s.trim()),
                    type: "shopify",
                  },
                });
              }}
            />

            <TextField
              autoComplete="off"
              label="Metadata fields to include (Comma Seperated)"
              helpText="Metafields to include in the response, e.g. 'color' to include the color metafield."
              value={unsavedCrawlOptions.include_metafields?.join(",") || ""}
              onChange={(e) => {
                setUnsavedCrawlOptions({
                  ...unsavedCrawlOptions,
                  include_metafields: e.split(",").map((s) => s.trim()),
                });
              }}
            />
          </FormLayout>

          <InlineStack align="end">
            <Button onClick={onSave}>Save</Button>
          </InlineStack>
        </BlockStack>
      </Card>
    </BlockStack>
  );
};
