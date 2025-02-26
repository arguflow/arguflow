import { data, type ActionFunctionArgs } from "@remix-run/node";
import { authenticate } from "../shopify.server";
import db from "../db.server";
import { ProductWebhook, TrieveKey } from "app/types";
import { deleteChunkFromTrieve } from "app/processors/getProducts";

export const action = async ({ request }: ActionFunctionArgs) => {
  const { payload, topic, shop } =
    await authenticate.webhook(request);
  console.log(`Received ${topic} webhook for ${shop}`);

  const current = payload as { id: string };
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

  deleteChunkFromTrieve(
    current.id,
    trieveKey,
    trieveKey.currentDatasetId ?? "",
  );

  return new Response();
};
