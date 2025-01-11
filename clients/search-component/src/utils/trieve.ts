import {
  ChunkGroup,
  ChunkMetadata,
  ChunkMetadataStringTagSet,
  CountChunkQueryResponseBody,
  SearchResponseBody,
  TrieveSDK,
} from "trieve-ts-sdk";
import {
  Chunk,
  ChunkWithHighlights,
  GroupChunk,
  GroupSearchResults,
  Props,
  SearchResults,
} from "./types";
import { defaultHighlightOptions, highlightText } from "./highlight";
import { ModalTypes, PagefindApi } from "./hooks/modal-context";

export const omit = (obj: object | null | undefined, keys: string[]) => {
  if (!obj) return obj;

  return Object.fromEntries(
    Object.entries(obj).filter(([key]) => !keys.includes(key))
  );
};

export const searchWithTrieve = async ({
  trieve,
  query_string,
  image_url,
  searchOptions = {
    search_type: "fulltext",
  },
  abortController,
  tag,
  type,
}: {
  trieve: TrieveSDK;
  query_string: string;
  image_url?: string;
  searchOptions: Props["searchOptions"];
  abortController?: AbortController;
  tag?: string;
  type?: ModalTypes;
}) => {
  const scoreThreshold =
    searchOptions.score_threshold ??
    ((searchOptions.search_type ?? "fulltext") === "fulltext" ||
    searchOptions.search_type == "bm25"
      ? 2
      : 0.3);

  let query;
  if (image_url) {
    query = {
      image_url: image_url,
    };
  } else {
    query = query_string;
  }

  let results;
  if (searchOptions.use_autocomplete === true) {
    results = (await trieve.autocomplete(
      {
        query,
        highlight_options: {
          ...defaultHighlightOptions,
          highlight_delimiters: ["?", ",", ".", "!", "\n"],
          highlight_window: type === "ecommerce" ? 5 : 10,
        },
        extend_results: true,
        score_threshold: scoreThreshold,
        page_size: searchOptions.page_size ?? 15,
        ...(tag && {
          filters: {
            must: [{ field: "tag_set", match_any: [tag] }],
          },
        }),
        typo_options: {
          correct_typos: true,
        },
        search_type: searchOptions.search_type ?? "fulltext",
        ...omit(searchOptions, ["use_autocomplete"]),
      },
      abortController?.signal
    )) as SearchResponseBody;
  } else {
    results = (await trieve.search(
      {
        query,
        highlight_options: {
          ...defaultHighlightOptions,
          highlight_delimiters: ["?", ",", ".", "!", "\n"],
          highlight_window: type === "ecommerce" ? 5 : 10,
        },
        score_threshold: scoreThreshold,
        page_size: searchOptions.page_size ?? 15,
        ...(tag && {
          filters: {
            must: [{ field: "tag_set", match_any: [tag] }],
          },
        }),
        typo_options: {
          correct_typos: true,
        },
        search_type: searchOptions.search_type ?? "fulltext",
        ...omit(searchOptions, ["use_autocomplete"]),
      },
      abortController?.signal
    )) as SearchResponseBody;
  }

  const resultsWithHighlight = results.chunks.map((chunk) => {
    const c = chunk.chunk as unknown as Chunk;
    return {
      ...chunk,
      chunk: {
        ...chunk.chunk,
        highlight:
          typeof query == "string" ? highlightText(query, c.chunk_html) : null,
      },
    };
  });

  return {
    chunks: resultsWithHighlight,
    requestID: results.id,
  } as unknown as SearchResults;
};

export const groupSearchWithTrieve = async ({
  trieve,
  query,
  searchOptions = {
    search_type: "fulltext",
  },
  abortController,
  tag,
  type,
}: {
  trieve: TrieveSDK;
  query: string;
  searchOptions: Props["searchOptions"];
  abortController?: AbortController;
  tag?: string;
  type?: ModalTypes;
}) => {
  const scoreThreshold =
    searchOptions.score_threshold ??
    ((searchOptions.search_type ?? "fulltext") === "fulltext" ||
    searchOptions.search_type == "bm25"
      ? 2
      : 0.3);

  const results = await trieve.searchOverGroups(
    {
      query,
      highlight_options: {
        ...defaultHighlightOptions,
        highlight_delimiters: ["?", ",", ".", "!", "\n"],
        highlight_window: type === "ecommerce" ? 5 : 10,
      },
      score_threshold: scoreThreshold,
      page_size: searchOptions.page_size ?? 15,
      ...(tag && {
        filters: {
          must: [{ field: "tag_set", match_any: [tag] }],
        },
      }),
      group_size: 1,
      search_type: searchOptions.search_type ?? "fulltext",
      ...omit(searchOptions, ["use_autocomplete"]),
    },
    abortController?.signal
  );

  const resultsWithHighlight = results.results.map((group) => {
    group.chunks = group.chunks.map((chunk) => {
      const c = chunk.chunk as unknown as Chunk;
      return {
        ...chunk,
        chunk: {
          ...chunk.chunk,
          highlight: highlightText(query, c.chunk_html),
        },
      };
    });
    return group;
  });

  return {
    groups: resultsWithHighlight,
    requestID: results.id,
  } as unknown as GroupSearchResults;
};

export const countChunks = async ({
  trieve,
  query,
  abortController,
  tag,
  searchOptions,
}: {
  trieve: TrieveSDK;
  query: string;
  abortController?: AbortController;
  tag?: string;
  searchOptions?: Props["searchOptions"];
}) => {
  const scoreThreshold =
    searchOptions?.score_threshold ??
    ((searchOptions?.search_type ?? "fulltext") === "fulltext" ||
    searchOptions?.search_type == "bm25"
      ? 2
      : 0.3);

  const results = await trieve.countChunksAboveThreshold(
    {
      query,
      score_threshold: scoreThreshold,
      limit: 100,
      ...(tag && {
        filters: {
          must: [{ field: "tag_set", match_any: [tag] }],
        },
      }),
      search_type: "fulltext",
      ...omit(searchOptions, ["search_type"]),
    },
    abortController?.signal
  );
  return results;
};

export const sendCtrData = async ({
  trieve,
  chunkID,
  requestID,
  index,
}: {
  trieve: TrieveSDK;
  chunkID: string;
  requestID: string;
  index: number;
}) => {
  await trieve.sendCTRAnalytics({
    ctr_type: "search",
    clicked_chunk_id: chunkID,
    request_id: requestID,
    position: index,
  });

  return null;
};

export const getSuggestedQueries = async ({
  trieve,
  query,
  count,
  abortController,
}: {
  query: string;
  trieve: TrieveSDK;
  count: number;
  abortController?: AbortController;
}) => {
  return trieve.suggestedQueries(
    {
      ...(query && { query }),
      suggestion_type: "keyword",
      suggestions_to_create: count,
      search_type: "semantic",
    },
    abortController?.signal
  );
};

export const getSuggestedQuestions = async ({
  trieve,
  abortController,
  query,
  count,
  group,
}: {
  trieve: TrieveSDK;
  abortController?: AbortController;
  query?: string;
  count: number;
  group?: ChunkGroup | null;
}) => {
  return trieve.suggestedQueries(
    {
      ...(query && { query }),
      suggestion_type: "question",
      search_type: "hybrid",
      suggestions_to_create: count,
      context: `The users previous query was "${query}", all suggestions should look like that.`,
      ...(group &&
        group?.tracking_id && {
          filters: {
            must: [
              {
                field: "group_tracking_ids",
                match_all: [group.tracking_id],
              },
            ],
          },
        }),
    },
    abortController?.signal
  );
};

export const sendFeedback = async ({ trieve }: { trieve: TrieveSDK }) => {
  return trieve;
};

export type SimpleChunk = ChunkMetadata | ChunkMetadataStringTagSet;

export const getAllChunksForGroup = async (
  groupId: string,
  trieve: TrieveSDK
): Promise<SimpleChunk[]> => {
  let moreToFind = true;
  let page = 1;
  const chunks = [];
  while (moreToFind) {
    const results = await trieve.trieve.fetch(
      "/api/chunk_group/{group_id}/{page}",
      "get",
      {
        datasetId: trieve.datasetId as string,
        groupId,
        page,
      }
    );
    if (results.chunks.length === 0) {
      moreToFind = false;
      break;
    }
    for (const chunk of results.chunks) {
      chunks.push(chunk);
    }
    page += 1;
  }
  return chunks;
};

export const searchWithPagefind = async (
  pagefind: PagefindApi,
  query: string,
  datasetId: string,
  tag?: string
) => {
  const response = await pagefind.search(
    query,
    tag && {
      filters: {
        tag_set: tag,
      },
    }
  );

  const results = await Promise.all(
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    response.results.map(async (result: any) => {
      return await result.data();
    })
  );

  // Set pagesize to 20
  const pagefindResultsMappedToTrieve = results
    .slice(0, 20)
    .map((result, i) => {
      return {
        chunk: {
          chunk_html: result.content,
          link: result.url,
          metadata: result.meta,
          created_at: "",
          dataset_id: datasetId,
          id: i.toString(),
          image_urls: result.meta.image_urls.split(", "),
          location: null,
          num_value: null,
          tag_set: result.meta.tag_set,
          time_stamp: null,
          tracking_id: null,
          updated_at: "",
          weight: 0,
        },
        highlights: [],
      };
    });

  return pagefindResultsMappedToTrieve;
};

export const groupSearchWithPagefind = async (
  pagefind: PagefindApi,
  query: string,
  datasetId: string,
  tag?: string
): Promise<GroupSearchResults> => {
  const response = await pagefind.search(
    query,
    tag && {
      filters: {
        tag_set: tag,
      },
    }
  );

  const results = await Promise.all(
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    response.results.map(async (result: any) => {
      return await result.data();
    })
  );

  const groupMap = new Map<string, ChunkWithHighlights[]>();

  let i = 0;
  for (const result of results) {
    const chunkWithHighlights = {
      chunk: {
        chunk_html: result.content,
        link: result.url,
        metadata: result.meta,
        created_at: "",
        dataset_id: datasetId,
        id: i.toString(),
        image_urls: result.meta.image_urls.split(", "),
        location: null,
        num_value: null,
        tag_set: result.meta.tag_set,
        time_stamp: null,
        tracking_id: null,
        updated_at: "",
        weight: 0,
      },
      highlights: [],
    } as unknown as ChunkWithHighlights;

    const group = result.meta.group_ids;
    if (groupMap.has(group)) {
      groupMap.get(group)?.push(chunkWithHighlights);
    } else {
      groupMap.set(group, [chunkWithHighlights]);
    }
    i++;

    if (groupMap.size >= 10 || i >= 20) {
      break;
    }
  }

  const groups: GroupChunk[] = [];
  Array.from(groupMap.entries()).forEach(([group_id, chunks]) => {
    groups.push({
      chunks: chunks,
      group: {
        created_at: "",
        dataset_id: datasetId,
        description: "",
        id: group_id,
        metadata: null,
        name: "",
        tag_set: "",
        tracking_id: null,
        updated_at: "",
      },
      requestID: "",
    } as unknown as GroupChunk);
  });

  return {
    groups: groups,
    requestID: "",
  } as unknown as GroupSearchResults;
};

export const countChunksWithPagefind = async (
  pagefind: PagefindApi,
  query: string,
  tags: {
    tag: string;
    label?: string;
    selected?: boolean;
    iconClassName?: string;
    icon?: () => JSX.Element;
  }[]
): Promise<CountChunkQueryResponseBody[]> => {
  let queryParam: string | null = query;
  if (query.trim() === "") {
    queryParam = null;
  }

  const response = await pagefind.search(queryParam);

  const tag_set = response.filters.tag_set;

  const counts: CountChunkQueryResponseBody[] = tags.map((tag) => {
    if (tag.tag in tag_set) {
      return {
        count: tag_set[tag.tag] as number,
      };
    }
    return {
      count: 0,
    };
  });

  counts.unshift({
    count: response.unfilteredResultCount as number,
  });

  return counts;
};

export const getPagefindIndex = async (trieve: TrieveSDK): Promise<string> => {
  const response = await trieve.trieve.fetch("/api/dataset/pagefind", "get", {
    datasetId: trieve.datasetId as string,
  });

  return response.url;
};

export const uploadFile = async (
  trieve: TrieveSDK,
  file_name: string,
  base64_file: string
): Promise<string> => {
  const response = await trieve.trieve.fetch("/api/file", "post", {
    datasetId: trieve.datasetId as string,
    data: {
      create_chunks: false,
      file_name: file_name,
      base64_file: base64_file,
    },
  });

  return response.file_metadata.id;
};

export const getPresignedUrl = async (
  trieve: TrieveSDK,
  fileId: string
): Promise<string> => {
  const response = await trieve.trieve.fetch("/api/file/{file_id}", "get", {
    datasetId: trieve.datasetId as string,
    fileId,
  });

  return response.s3_url;
};
