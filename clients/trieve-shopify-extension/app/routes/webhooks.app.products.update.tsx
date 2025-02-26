import { data, type ActionFunctionArgs } from "@remix-run/node";
import { authenticate } from "../shopify.server";
import db from "../db.server";
import { ProductWebhook, TrieveKey } from "app/types";
import {
  chunk_to_size,
  createChunkFromProductWebhook,
  sendChunksFromWebhook,
  sendChunksToTrieve,
} from "app/processors/getProducts";
import { ChunkReqPayload } from "trieve-ts-sdk";
import { ExtendedCrawlOptions } from "app/components/CrawlSettings";

export const action = async ({ request }: ActionFunctionArgs) => {
  const { admin, payload, session, topic, shop } =
    await authenticate.webhook(request);
  console.log(`Received ${topic} webhook for ${shop}`);

  const current = payload as ProductWebhook;
  const apiKey = await db.apiKey.findFirst({
    where: { shop: `https://${shop}` },
  });

  if (!apiKey) {
    console.error(`No API key found for ${shop}`);
    return new Response();
  }

  const trieveKey: TrieveKey = {
    createdAt: new Date(apiKey.createdAt).toISOString(),
    id: apiKey.id,
    key: apiKey.key,
    organizationId: apiKey.organizationId,
    currentDatasetId: apiKey.currentDatasetId,
    userId: apiKey.userId,
  };

  let crawlSettings = await db.crawlSettings.findFirst({
    where: {
      datasetId: trieveKey.currentDatasetId ?? "",
      shop: shop,
    },
  });

  if (!crawlSettings) {
    console.error(`No crawl settings found for ${shop}`);
    return new Response();
  }

  sendChunksFromWebhook(
    current,
    trieveKey,
    trieveKey.currentDatasetId ?? "",
    admin,
    session,
    crawlSettings.crawlSettings as ExtendedCrawlOptions,
  );

  return new Response();
};
