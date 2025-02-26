import {
  ChunkMetadata,
  ChunkMetadataStringTagSet,
  CountChunkQueryResponseBody,
  CTRType,
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
import { ModalProps, ModalTypes, PagefindApi } from "./hooks/modal-context";

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
  audioBase64,
  searchOptions = {
    search_type: "fulltext",
  },
  abortController,
  tags,
  type,
}: {
  trieve: TrieveSDK;
  query_string: string;
  image_url?: string;
  audioBase64?: string;
  searchOptions: Props["searchOptions"];
  abortController?: AbortController;
  tags?: string[];
  type?: ModalTypes;
}) => {
  const scoreThreshold =
    searchOptions.score_threshold ??
    ((searchOptions.search_type ?? "fulltext") === "fulltext" ||
    searchOptions.search_type == "bm25"
      ? 2
      : 0.3);

  let query;
  if (audioBase64) {
    query = {
      audio_base64: audioBase64,
    };
  } else if (image_url) {
    query = {
      image_url: image_url,
    };
  } else {
    query = query_string;
  }

  let results;
  let transcribedQuery: string | null = null;
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
        ...(tags?.length && {
          filters: {
            must: [{ field: "tag_set", match_any: tags }],
          },
        }),
        typo_options: {
          correct_typos: true,
        },
        search_type: searchOptions.search_type ?? "fulltext",
        ...omit(searchOptions, ["use_autocomplete"]),
      },
      abortController?.signal,
      (headers: Record<string, string>) => {
        if (headers["x-tr-query"] && audioBase64) {
          transcribedQuery = headers["x-tr-query"];
        }
      }
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
        ...(tags?.length && {
          filters: {
            must: [{ field: "tag_set", match_any: tags }],
          },
        }),
        typo_options: {
          correct_typos: true,
        },
        search_type: searchOptions.search_type ?? "fulltext",
        ...omit(searchOptions, ["use_autocomplete"]),
      },
      abortController?.signal,
      (headers: Record<string, string>) => {
        if (headers["x-tr-query"] && audioBase64) {
          transcribedQuery = headers["x-tr-query"];
        }
      }
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
    transcribedQuery,
  } as unknown as SearchResults;
};

export const groupSearchWithTrieve = async ({
  trieve,
  query_string,
  image_url,
  audioBase64,
  searchOptions = {
    search_type: "fulltext",
  },
  abortController,
  tags,
  type,
}: {
  trieve: TrieveSDK;
  query_string: string;
  image_url?: string;
  audioBase64?: string;
  searchOptions: Props["searchOptions"];
  abortController?: AbortController;
  tags?: string[];
  type?: ModalTypes;
}) => {
  const scoreThreshold =
    searchOptions.score_threshold ??
    ((searchOptions.search_type ?? "fulltext") === "fulltext" ||
    searchOptions.search_type == "bm25"
      ? 2
      : 0.3);

  let query;
  if (audioBase64) {
    query = {
      audio_base64: audioBase64,
    };
  } else if (image_url) {
    query = {
      image_url: image_url,
    };
  } else {
    query = query_string;
  }
  let transcribedQuery: string | null = null;
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
      ...(tags?.length && {
        filters: {
          must: [{ field: "tag_set", match_any: tags }],
        },
      }),
      group_size: 1,
      search_type: searchOptions.search_type ?? "fulltext",
      ...omit(searchOptions, ["use_autocomplete"]),
    },
    abortController?.signal,
    (headers: Record<string, string>) => {
      if (headers["x-tr-query"] && audioBase64) {
        transcribedQuery = headers["x-tr-query"];
      }
    }
  );

  const resultsWithHighlight = results.results.map((group) => {
    group.chunks = group.chunks.map((chunk) => {
      const c = chunk.chunk as unknown as Chunk;
      return {
        ...chunk,
        chunk: {
          ...chunk.chunk,
          highlight:
            typeof query == "string"
              ? highlightText(query, c.chunk_html)
              : null,
        },
      };
    });
    return group;
  });

  return {
    groups: resultsWithHighlight,
    requestID: results.id,
    transcribedQuery,
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
  type,
  chunkID,
  requestID,
  index,
}: {
  trieve: TrieveSDK;
  chunkID: string;
  requestID: string;
  type: CTRType;
  index: number;
}) => {
  await trieve.sendCTRAnalytics({
    ctr_type: type,
    clicked_chunk_id: chunkID,
    request_id: requestID,
    position: index,
  });

  return null;
};

export const trackViews = async ({
  trieve,
  type,
  requestID,
  items,
}: {
  trieve: TrieveSDK;
  requestID: string;
  type: CTRType;
  items: string[];
}) => {
  trieve.trieve.fetch("/api/analytics/events", "put", {
    datasetId: trieve.datasetId ?? "",
    data: {
      event_name: "View",
      event_type: "view",
      items: items,
      request: {
        request_id: requestID,
        request_type: type,
      },
    },
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
  groupTrackingId,
  props: modalProps,
  prevUserMessages,
  is_followup,
  chunks,
}: {
  trieve: TrieveSDK;
  abortController?: AbortController;
  query?: string;
  count: number;
  groupTrackingId?: string | null;
  is_followup?: boolean,
  prevUserMessages?: string[],
  props?: ModalProps;
  chunks?: Chunk[] | null,
}) => {

  let context: string;
  if (groupTrackingId && modalProps?.cleanGroupName) {
    context = `The user is specifically and exclusively interested in the ${modalProps.cleanGroupName}. Suggest short questions limited to 3-6 words based on the reference content.`;
  } else if (prevUserMessages && is_followup && chunks) {
    const cleanedChunks = chunks.map((chunk) => {
      return {
        chunk_html: chunk.chunk_html,
        title: chunk.metadata.heading || chunk.metadata.title || chunk.metadata.page_title,
        price: chunk.num_value
      }
    });
    context = `The previous messages were ${JSON.stringify(prevUserMessages)}. The AI presented ${JSON.stringify(cleanedChunks)}. You are the user asking follow up questions to the AI. Keep your query recommendations short, limited to 3-6 words.`;
  } else if (query) {
    context = `The user's previous query was "${query}", all suggestions should look like that.`;
  } else {
    context = "Keep your query recommendations short, limited to 3-6 words";
  }

  return trieve.suggestedQueries(
    {
      ...(query && { query }),
      suggestion_type: "question",
      search_type: "hybrid",
      suggestions_to_create: count,
      is_followup: is_followup ?? false,
      context,
      ...(groupTrackingId &&
        groupTrackingId && {
          filters: {
            must: [
              {
                field: "group_tracking_ids",
                match_all: [groupTrackingId],
              },
            ],
          },
        }),
    },
    abortController?.signal
  );
};

export type SimpleChunk = ChunkMetadata | ChunkMetadataStringTagSet;

export const searchWithPagefind = async (
  pagefind: PagefindApi,
  query: string,
  datasetId: string,
  tags?: string[]
) => {
  const response = await pagefind.search(
    query,
    tags && {
      filters: {
        tag_set: tags,
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
  tags?: string[]
): Promise<GroupSearchResults> => {
  const response = await pagefind.search(
    query,
    tags && {
      filters: {
        tag_set: tags,
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
