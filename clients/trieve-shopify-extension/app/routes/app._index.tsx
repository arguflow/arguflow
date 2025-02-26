import type { LoaderFunctionArgs } from "@remix-run/node";
import {
  ClientLoaderFunctionArgs,
  json,
  Link,
  useLoaderData,
} from "@remix-run/react";
import {
  Page,
  Layout,
  Text,
  Link as PolarisLink,
  Card,
  BlockStack,
  SkeletonBodyText,
  Select,
  Button,
  InlineStack,
} from "@shopify/polaris";
import { initTrieveSdk, validateTrieveAuth } from "app/auth";
import { authenticate } from "app/shopify.server";

export const loader = async (args: LoaderFunctionArgs) => {
  const { session, sessionToken } = await authenticate.admin(args.request);
  const key = await validateTrieveAuth(args);
  const trieve = await initTrieveSdk(args);

  if (!trieve.organizationId) {
    throw new Response("Unautorized, no organization tied to user session", {
      status: 401,
    });
  }

  let datasetId = trieve.datasetId;

  const datasets = await trieve.getDatasetsFromOrganization(
    trieve.organizationId,
  );

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
            shop: `https://${session.shop}`,
          },
        },
      });
    }
    datasetId = shopDataset?.id;
  }

  return {
    datasets,
    shopDataset,
  };
};

export function HydrateFallback() {
  return (
    <Page>
      <BlockStack gap="500">
        <Layout>
          <Layout.Section variant="oneHalf">
            <BlockStack gap="500">
              <Card>
                <BlockStack gap="200">
                  <Text as="h2" variant="headingMd">
                    Select Dataset
                  </Text>
                  <SkeletonBodyText lines={8} />
                </BlockStack>
              </Card>
            </BlockStack>
          </Layout.Section>
        </Layout>
      </BlockStack>
    </Page>
  );
}

export default function Index() {
  const { datasets, shopDataset } = useLoaderData<typeof loader>();

  return (
    <Page>
      <BlockStack gap="500">
        <Layout>
          <Layout.Section variant="oneHalf">
            <BlockStack gap="500">
              <Card>
                <InlineStack gap="200" align="space-between">
                  <Text as="h2" variant="headingMd">
                    Indexed Dataset: {shopDataset?.name}
                  </Text>

                  <Link to="/app/dataset">
                    <Button>Edit settings</Button>
                  </Link>
                </InlineStack>
              </Card>
            </BlockStack>
          </Layout.Section>
        </Layout>
      </BlockStack>
    </Page>
  );
}
