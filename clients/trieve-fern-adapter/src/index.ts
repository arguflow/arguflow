#!/usr/bin/env node

/* eslint-disable @typescript-eslint/no-explicit-any */
import { Command } from 'commander';
import { parse } from 'yaml';
import { Window } from 'happy-dom';
import fs from 'node:fs';
import { join } from 'node:path';
import { TrieveSDK, ChunkReqPayload } from 'trieve-ts-sdk';
import { marked } from 'marked';
import { dereferenceSync } from '@trojs/openapi-dereference';
import pluralize from 'pluralize';

const splitHtmlIntoHeadAndBodies = (html: Element): [string, string][] => {
  const headingRegex = /h\d/gi;
  const tuples: [string, string][] = [];
  let head = '';
  let body = '';
  for (const element of html.children) {
    const isHeading =
      element.tagName == 'title' || headingRegex.test(element.tagName);
    if (!isHeading) {
      body += `${body ? '\n' : ''}` + element.textContent;
      continue;
    }

    if (isHeading && body) {
      tuples.push([head, body]);
      head = element.textContent ?? '';
      body = '';
      continue;
    }

    if (isHeading && !head) {
      head = element.textContent ?? '';
      continue;
    }

    if (isHeading && head && !body) {
      body = element.textContent ?? '';
      continue;
    }
  }

  if (head) {
    tuples.push([head, body]);
  }

  return tuples;
};

const extractPathsFromAny = (obj: any): string[] => {
  const paths = [];
  for (const key in obj) {
    if (key === 'path' && typeof obj[key] === 'string') {
      const value = obj[key] as string;
      if (value.endsWith('.mdx') || value.endsWith('.md')) {
        paths.push(obj[key]);
      }
    } else if (typeof obj[key] === 'object') {
      paths.push(...extractPathsFromAny(obj[key]));
    } else if (Array.isArray(obj[key])) {
      for (const item of obj[key]) {
        paths.push(...extractPathsFromAny(item));
      }
    }
  }
  return paths;
};

const extractChunksFromPath = async (
  path: string,
  rootUrl: string | undefined = undefined,
): Promise<ChunkReqPayload[]> => {
  const window = new Window({
    settings: {
      disableJavaScriptEvaluation: true,
    },
  });
  const document = window.document;
  const chunks: ChunkReqPayload[] = [];
  let tuples: [string, string][] = [];
  let title = '';
  let subtitle = '';
  let slug = '';
  try {
    const curPath = `${pathWithoutFileName}/${path}`;
    const file = fs.readFileSync(curPath, 'utf8');
    const parts = file.split('---');
    let content = file;
    if (parts.length >= 3) {
      const frontmatter = parts[1].trim();
      const frontmatterData = parse(frontmatter);
      title = frontmatterData.title;
      subtitle = frontmatterData.subtitle;
      slug = frontmatterData.slug;
      content = parts.slice(2).join('---');
    }

    const html = await marked(content);
    document.body.innerHTML = html;
    if (subtitle) {
      const subtitleEl = document.createElement('h2');
      subtitleEl.textContent = subtitle;
      document.body.insertBefore(subtitleEl, document.body.firstChild);
    }
    if (title) {
      const titleEl = document.createElement('h1');
      titleEl.textContent = title;
      document.body.insertBefore(titleEl, document.body.firstChild);
    }

    tuples = splitHtmlIntoHeadAndBodies(document.body as unknown as Element);
  } catch (err) {
    console.error(`Error processing path: ${path}`, err);
  }

  for (const [heading, body] of tuples) {
    if (!heading) {
      continue;
    }
    let chunk_html = `<h3>${heading}</h3>`;
    chunk_html += `<p>${body}</p>`;

    const link = `${rootUrl}/${slug ?? path.replace('.mdx', '')}`;
    const tag_set = (slug ?? path.replace('.mdx', ''))
      .split('/')
      .filter((x) => x);
    const metadata: any = {
      url: link,
      hierarchy: tag_set,
      heading: heading,
    };

    let semantic_boost_phrase = heading;
    let fulltext_boost_phrase = heading;

    if (title) {
      semantic_boost_phrase = `${title} ${semantic_boost_phrase}`;
      fulltext_boost_phrase = `${title} ${fulltext_boost_phrase}`;
      metadata['title'] = title;
    }
    if (subtitle) {
      semantic_boost_phrase = `${subtitle} ${semantic_boost_phrase}`;
      fulltext_boost_phrase = `${subtitle} ${fulltext_boost_phrase}`;
      metadata['description'] = subtitle;
    }

    const tracking_id =
      `${slug ?? path.replace('.mdx', '')}-${heading}`.replace(' ', '-');

    const chunk: ChunkReqPayload = {
      chunk_html,
      link,
      tag_set,
      tracking_id,
      metadata,
      group_tracking_ids: [path],
      convert_html_to_text: true,
    };

    if (semantic_boost_phrase) {
      chunk.semantic_boost = {
        phrase: semantic_boost_phrase,
        distance_factor: 0.3,
      };
    }

    if (fulltext_boost_phrase) {
      chunk.fulltext_boost = {
        phrase: fulltext_boost_phrase,
        boost_factor: 1.3,
      };
    }

    chunks.push(chunk);
  }

  return chunks;
};

const extractChunksFromOpenapiSpec = async (
  openapiSpecUrl: string,
  siteUrl: string | undefined = undefined,
  apiRefParent: string | undefined = undefined,
): Promise<ChunkReqPayload[]> => {
  const chunks: ChunkReqPayload[] = [];
  try {
    const openapiSpecResp = await fetch(openapiSpecUrl);
    const openapiSpec = await openapiSpecResp.text();
    // if the URL ended in .json, we'll assume it's JSON
    // otherwise, we'll assume it's YAML
    const isJson = openapiSpecUrl.endsWith('.json');
    const openapiSpecObj = isJson
      ? JSON.parse(openapiSpec)
      : parse(openapiSpec);
    const schemaWithNoRefs: any = dereferenceSync(openapiSpecObj);

    const pathObj = schemaWithNoRefs.paths;
    if (!pathObj) {
      console.error('No paths found in OpenAPI spec');
    }
    const paths = Object.keys(pathObj);
    for (const path of paths) {
      const pathData = pathObj[path];
      const methods = Object.keys(pathData);
      for (const method of methods) {
        const operationId = pathData[method].operationId;
        const summary = pathData[method].summary;
        const description = pathData[method].description;
        const [namespace, ...parts] = summary?.toLowerCase().split(' ') ?? [];
        const endpoint = namespace
          ? join(pluralize(parts.join('-')), namespace)
          : path;
        const pageLink = `${siteUrl}/${apiRefParent}/${endpoint}`;
        const metadata = {
          operation_id: operationId,
          url: pageLink,
          hierarchy: [
            apiRefParent,
            summary?.split(' ').join('-').toLowerCase() ?? path,
          ],
          summary,
          description,
        };
        const heading = `<h2><span class="openapi-method">${method.toUpperCase()}</span> ${summary}</h2>`;
        let chunk_html = heading;
        if (description) {
          chunk_html += `\n\n<p>${description}</p>`;
        }

        const chunk: ChunkReqPayload = {
          chunk_html,
          link: pageLink,
          tag_set: ['openapi-route', operationId, method],
          metadata,
          tracking_id: operationId,
          group_tracking_ids: [path],
          fulltext_boost: {
            phrase: heading,
            boost_factor: 1.3,
          },
          semantic_boost: {
            phrase: heading,
            distance_factor: 0.3,
          },
          convert_html_to_text: true,
        };

        chunks.push(chunk);
      }
    }
  } catch (err) {
    console.error(`Error processing OpenAPI spec: ${openapiSpecUrl}`, err);
  }

  return chunks;
};

const trieveApiHost = process.env.TRIEVE_API_HOST;
const trieveApiKey = process.env.TRIEVE_API_KEY;
const trieveOrganizationId = process.env.TRIEVE_ORGANIZATION_ID;
const trieveDatasetTrackingId = process.env.TRIEVE_DATASET_TRACKING_ID;
if (
  !trieveApiHost ||
  !trieveApiKey ||
  !trieveDatasetTrackingId ||
  !trieveOrganizationId
) {
  console.error('Missing required environment variables');
  process.exit(1);
}

const program = new Command();
program
  .option('-f, --file <file>', 'docs.yml file to process')
  .option('-r, --root-url <rootUrl>', 'Root URL to use for relative paths')
  .option('-s, --openapi-spec <openapiSpec>', 'URL of OpenAPI spec file')
  .option('-a, --api-ref-path <apiRefPath>', 'Path to API reference pages');

program.parse(process.argv);

const options = program.opts();
const apiRefPath = options.apiRefPath;
const filePath = options.file;
const rootUrl = options.rootUrl;
const openapiSpec = options.openapiSpec;

if (!filePath) {
  console.error('Missing required --file option', options);
  program.help();
}

const pathParts = options.file.split('/');
const pathWithoutFileName = pathParts.slice(0, pathParts.length - 1).join('/');

let chunkReqPayloads: ChunkReqPayload[] = [];

if (openapiSpec) {
  console.log('Processing OpenAPI spec...', openapiSpec);
  await extractChunksFromOpenapiSpec(openapiSpec, rootUrl, apiRefPath).then(
    (res) => {
      chunkReqPayloads = chunkReqPayloads.concat(res);
    },
  );
} else {
  console.warn('No OpenAPI spec provided, skipping...');
}

try {
  const file = fs.readFileSync(filePath, 'utf8');
  const data = parse(file);
  const paths = extractPathsFromAny(data);

  for (const path of paths) {
    await extractChunksFromPath(path, rootUrl).then((res) => {
      chunkReqPayloads = chunkReqPayloads.concat(res);
    });
  }
} catch (err) {
  console.error(`Error reading file: ${filePath}`, err);
}

export const trieve = new TrieveSDK({
  baseUrl: trieveApiHost,
  apiKey: trieveApiKey,
  datasetId: trieveDatasetTrackingId,
  organizationId: trieveOrganizationId,
});

try {
  console.info('Checking for existing dataset...');
  const dataset = await trieve.getDatasetByTrackingId(trieveDatasetTrackingId);
  trieve.datasetId = dataset.id;
  console.info('Dataset found, clearing...');
  try {
    await trieve.clearDataset(dataset.id);
  } catch (err) {
    console.error('Error clearing dataset', err);
  }
  while (true) {
    try {
      console.info('Checking for groups...');
      const groups = await trieve.getGroupsForDataset({
        page: 1,
      });

      if (groups.groups.length === 0) {
        console.info('Dataset cleared');
        break;
      }
    } catch (err) {
      console.error('Error getting groups', err);
    }
    console.info('Waiting on delete...');
  }
} catch {
  console.info('Dataset not found, creating...');
  try {
    const createdDataset = await trieve.createDataset({
      tracking_id: trieveDatasetTrackingId,
      dataset_name: trieveDatasetTrackingId,
    });
    console.info('Dataset created');
    trieve.datasetId = createdDataset.id;
  } catch (err) {
    console.error('Error creating dataset', err);
    process.exit(1);
  }
}

for (let i = 0; i < chunkReqPayloads.length; i += 120) {
  const chunkBatch = chunkReqPayloads.slice(i, i + 120);
  console.log(`Creating chunk batch ${i + 1} - ${i + 120}`);
  let retries = 0;
  while (true) {
    try {
      await trieve.createChunk(chunkBatch);
      console.log('Batch created');
      break;
    } catch (err) {
      console.error('Error creating chunk batch...', err);
      retries++;
      if (retries > 3) {
        console.error('Max retries exceeded, skipping batch');
        break;
      }
      console.log('Retrying...');
    }
  }
}

console.log('Done!');
process.exit(0);
