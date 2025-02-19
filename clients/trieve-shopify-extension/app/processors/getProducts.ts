import { ExtendedCrawlOptions } from "app/components/CrawlSettings";
import {
  Product,
  TrieveKey,
  ProductsResponse,
} from "app/types";
import { ChunkReqPayload } from "trieve-ts-sdk";

function createChunkFromProduct(
  product: Product,
  variant: Product["variants"]["nodes"][0],
  baseUrl: string,
  crawlOptions: ExtendedCrawlOptions,
): ChunkReqPayload {
  // Extract image URLs
  const imageUrls = product.media.nodes.map((media) => media.preview.image.url);

  // Handle text cleaning
  let productTitle = product.title || "";
  let variantTitle = variant.title || "";
  let productBodyHtml = product.bodyHtml || "";

  if (crawlOptions.heading_remove_strings) {
    crawlOptions.heading_remove_strings.forEach((removeString) => {
      productTitle = productTitle.replace(removeString, "");
      variantTitle = variantTitle.replace(removeString, "");
    });
  }

  if (crawlOptions.body_remove_strings) {
    crawlOptions.body_remove_strings.forEach((removeString) => {
      productBodyHtml = productBodyHtml.replace(removeString, "");
    });
  }

  // Create product link
  const link = `${baseUrl}/products/${product.handle}?variant=${variant.id}`;

  // Generate chunk HTML
  let chunkHtml =
    variant.title === "Default Title"
      ? `<h1>${productTitle}</h1>${productBodyHtml}`
      : `<h1>${productTitle} - ${variantTitle}</h1>${productBodyHtml}`;

  // Handle tag regexes
  if (crawlOptions.scrape_options?.tag_regexes) {
    const tagMatches = new Set<string>();

    crawlOptions.scrape_options.tag_regexes.forEach((pattern) => {
      try {
        const regex = new RegExp(pattern);
        product.tags.forEach((tag) => {
          if (regex.test(tag)) {
            tagMatches.add(`<span>${pattern}</span>`);
          }
        });
      } catch (e) {
        console.warn(`Invalid regex pattern: ${pattern}`);
      }
    });

    if (tagMatches.size > 0) {
      const tagsString = Array.from(tagMatches).join("");
      chunkHtml = `<div>${chunkHtml}</div>\n\n<div>${tagsString}</div>`;
    }
  }

  const groupVariants = crawlOptions.scrape_options?.group_variants ?? true;

  const semanticBoostPhrase = groupVariants ? variantTitle : productTitle;
  const fulltextBoostPhrase = groupVariants ? variantTitle : productTitle;
  const tags = product.tags;
  if (crawlOptions.include_metafields) {
    product.variants.nodes.forEach((v) => {
      let values: string[] = JSON.parse(
        v.metafields.nodes.find((m) =>
          crawlOptions.include_metafields?.includes(m.key),
        )?.value ?? "[]",
      );
      tags.push(...values);
    });
  }
  const metadata = {
    body_html: product.bodyHtml,
    handle: product.handle,
    id: parseInt(product.id.split("/").pop() || "0"),
    images: product.media.nodes.map((media) => ({
      src: media.preview.image.url,
    })),
    tags: product.tags,
    title: product.title,
    total_inventory: product.totalInventory,
    variants: product.variants.nodes.map((v) => ({
      id: parseInt(v.id.split("/").pop() || "0"),
      price: v.price,
      product_id: parseInt(product.id.split("/").pop() || "0"),
      title: v.title,
      inventory_quantity: v.inventoryQuantity,
    })),
  };

  return {
    chunk_html: chunkHtml,
    link,
    tag_set: product.tags,
    num_value: parseFloat(variant.price),
    metadata,
    tracking_id: groupVariants
      ? variant.id.split("/").pop()
      : product.id.split("/").pop(),
    group_tracking_ids: groupVariants
      ? [product.id.split("/").pop() ?? "0"]
      : undefined,
    image_urls: imageUrls,
    fulltext_boost:
      crawlOptions.boost_titles !== false
        ? {
            phrase: fulltextBoostPhrase,
            boost_factor: 1.3,
          }
        : undefined,
    semantic_boost:
      crawlOptions.boost_titles !== false
        ? {
            phrase: semanticBoostPhrase,
            distance_factor: 0.3,
          }
        : undefined,
    convert_html_to_text: true,
    upsert_by_tracking_id: true,
  };
}

async function sendChunksToTrieve(
  chunks: ChunkReqPayload[],
  key: TrieveKey,
  datasetId: string,
) {
  await fetch(`https://api.trieve.ai/api/chunk`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${key.key}`,
      "TR-Dataset": datasetId ?? "",
      "Content-Type": "application/json",
    },
    body: JSON.stringify(chunks),
  });
}

export const sendChunks = async (
  datasetId: string,
  key: TrieveKey,
  admin: any,
  session: any,
  crawlOptions: ExtendedCrawlOptions,
) => {
  let next_page = null;
  let started = false;
  const chunks: ChunkReqPayload[] = [];

  while (next_page != null || !started) {
    started = true;
    let next_page_query = next_page ? `after: "${next_page}"` : "";
    const response = await admin.graphql(
      `#graphql
      query {
        products(first: 120 ${next_page_query}) {
          nodes {
            id
            title
            productType
            bodyHtml
            handle
            tags
            category {
              name
            }
            totalInventory
            variants(first: 20) {
              nodes {
                id
                displayName
                price
                title
                inventoryQuantity
                metafields(first: 20) {
                  nodes {
                    key
                    value
                  }
                }
              }
            }
            media(first: 20) {
              nodes {
                preview {
                  image {
                    url
                  }
                }
              }
            }
          }
          pageInfo {
            hasNextPage
            endCursor
          }
        }
      }`,
    );

    const { data } = (await response.json()) as { data: ProductsResponse };

    const dataChunks: ChunkReqPayload[] = data.products.nodes.flatMap(
      (product) =>
        product.variants.nodes.map((variant) =>
          createChunkFromProduct(
            product,
            variant,
            `https://${session.shop}`,
            crawlOptions,
          ),
        ),
    );

    for (const batch of chunk_to_size(dataChunks, 120)) {
      sendChunksToTrieve(batch, key, datasetId ?? "");
    }

    next_page = data.products.pageInfo.hasNextPage
      ? data.products.pageInfo.endCursor
      : null;
  }

  return chunks;
};

function chunk_to_size<T>(arr: T[], size: number): T[][] {
    if (size <= 0) throw new Error('Chunk size must be greater than 0');
    
    const result: T[][] = [];
    for (let i = 0; i < arr.length; i += size) {
        result.push(arr.slice(i, i + size));
    }
    return result;
}
