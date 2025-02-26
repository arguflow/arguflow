/* eslint-disable @typescript-eslint/no-unsafe-return */
/* eslint-disable @typescript-eslint/no-unsafe-argument */
/* eslint-disable @typescript-eslint/no-unsafe-call */
/* eslint-disable @typescript-eslint/no-explicit-any */
/* eslint-disable @typescript-eslint/no-unsafe-assignment */
/* eslint-disable @typescript-eslint/no-unsafe-member-access */
import {
  Show,
  createEffect,
  createSignal,
  For,
  Match,
  Switch,
  useContext,
  onCleanup,
  createMemo,
  on,
  Setter,
  Accessor,
} from "solid-js";
import {
  type ChunkGroupDTO,
  type ScoreChunkDTO,
  ChunkBookmarksDTO,
  GroupScoreChunkDTO,
  isChunkGroupPageDTO,
} from "../utils/apiTypes";
import { FullScreenModal } from "./Atoms/FullScreenModal";
import { PaginationController } from "./Atoms/PaginationController";
import { ConfirmModal } from "./Atoms/ConfirmModal";
import { Portal } from "solid-js/web";
import { AiOutlineRobot } from "solid-icons/ai";
import { ChatPopup } from "./ChatPopup";
import { IoDocumentOutline, IoDocumentsOutline } from "solid-icons/io";
import { DatasetAndUserContext } from "./Contexts/DatasetAndUserContext";
import {
  FaSolidChevronDown,
  FaSolidChevronUp,
  FaSolidDownload,
} from "solid-icons/fa";
import { createToast } from "./ShowToasts";
import {
  isSortByField,
  isSortBySearchType,
  SearchStore,
} from "../hooks/useSearch";
import { downloadFile } from "../utils/downloadFile";
import ScoreChunk from "./ScoreChunk";
import { FiEdit, FiEye } from "solid-icons/fi";
import { ServerTimings } from "./ServerTimings";
import { VsChevronRight } from "solid-icons/vs";
import { useCtrClickForChunk } from "../hooks/useCtrAnalytics";
import { BiRegularChevronDown, BiRegularChevronUp } from "solid-icons/bi";
import { Tooltip } from "shared/ui";

export interface ResultsPageProps {
  search: SearchStore;
  rateQuery: Accessor<boolean>;
  setRatingQuery: Setter<boolean>;
}

export type ServerTiming = {
  name: string;
  duration: number;
};

const parseServerTimings = (labels: string[]): ServerTiming[] => {
  return labels.map((label) => {
    const [name, rawDuration] = label.split(";");
    return {
      name,
      duration: parseInt(rawDuration.substring(4)),
    };
  });
};

const ResultsPage = (props: ResultsPageProps) => {
  const apiHost = import.meta.env.VITE_API_HOST as string;
  const datasetAndUserContext = useContext(DatasetAndUserContext);

  const $dataset = datasetAndUserContext.currentDataset;
  const { registerClickForChunk } = useCtrClickForChunk();
  const [loading, setLoading] = createSignal(false);
  const [page, setPage] = createSignal(1);

  const [chunkCollections, setChunkCollections] = createSignal<ChunkGroupDTO[]>(
    [],
  );
  const $currentUser = datasetAndUserContext.user;
  const [resultChunks, setResultChunks] = createSignal<ScoreChunkDTO[]>([]);
  const [groupResultChunks, setGroupResultChunks] = createSignal<
    GroupScoreChunkDTO[]
  >([]);
  const [clientSideRequestFinished, setClientSideRequestFinished] =
    createSignal(false);
  const [showConfirmDeleteModal, setShowConfirmDeleteModal] =
    createSignal(false);
  const [totalCollectionPages, setTotalCollectionPages] = createSignal(0);
  // eslint-disable-next-line @typescript-eslint/no-empty-function
  const [onDelete, setOnDelete] = createSignal(() => {});
  const [bookmarks, setBookmarks] = createSignal<ChunkBookmarksDTO[]>([]);
  const [openChat, setOpenChat] = createSignal(false);
  const [selectedIds, setSelectedIds] = createSignal<string[]>([]);
  const [noResults, setNoResults] = createSignal(false);
  const [totalPages, setTotalPages] = createSignal(0);
  const [searchID, setSearchID] = createSignal("");
  const [rating, setRating] = createSignal({
    rating: 5,
    note: "",
  });
  const [correctedQuery, setCorrectedQuery] = createSignal("");

  const [serverTimings, setServerTimings] = createSignal<ServerTiming[]>([]);
  const [showServerTimings, setShowServerTimings] = createSignal(false);
  const [allGroupsList, setAllGroupsList] = createSignal<ChunkGroupDTO[]>([]);

  const fetchChunkCollections = () => {
    if (!$currentUser?.()) return;
    const dataset = $dataset?.();
    if (!dataset) return;
    void fetch(`${apiHost}/dataset/groups/${dataset.dataset.id}/1`, {
      method: "GET",
      credentials: "include",
      headers: {
        "X-API-version": "2.0",
        "TR-Dataset": dataset.dataset.id,
      },
    }).then((response) => {
      if (response.ok) {
        void response.json().then((data) => {
          // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
          setChunkCollections(data.groups);
          // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
          setTotalCollectionPages(data.total_pages);
        });
      }
    });
  };

  const rateQuery = () => {
    const dataset = $dataset?.();
    if (!dataset) return;
    void fetch(`${apiHost}/analytics/search`, {
      method: "PUT",
      credentials: "include",
      headers: {
        "X-API-version": "2.0",
        "Content-Type": "application/json",
        "TR-Dataset": dataset.dataset.id,
      },
      body: JSON.stringify({
        query_id: searchID(),
        rating: rating().rating,
        note: rating().note,
      }),
    }).then((response) => {
      if (response.ok) {
        createToast({
          type: "success",
          message: "Query rated successfully",
        });
      } else {
        void response.json().then((data) => {
          createToast({
            type: "error",
            message: data.message,
          });
        });
      }
    });
  };

  const handleDownloadFile = (file_id?: string) => {
    const datasetId = $dataset?.()?.dataset.id;
    if (file_id && datasetId) {
      void downloadFile(file_id, datasetId);
    }
  };

  const fetchBookmarks = () => {
    const dataset = $dataset?.();
    if (!dataset) return;

    void fetch(`${apiHost}/chunk_group/chunks`, {
      method: "POST",
      credentials: "include",
      headers: {
        "X-API-version": "2.0",
        "Content-Type": "application/json",
        "TR-Dataset": dataset.dataset.id,
      },
      body: JSON.stringify({
        chunk_ids: resultChunks().flatMap((c) => {
          return c.chunk.id;
        }),
      }),
    }).then((response) => {
      if (response.ok) {
        void response.json().then((data) => {
          const chunkBookmarks = data as ChunkBookmarksDTO[];
          setBookmarks(chunkBookmarks);
        });
      }
    });
  };

  const fetchAllGroups = async () => {
    let currentPage = 1;
    let hasMore = true;
    const currentDataset = $dataset?.();
    if (!currentDataset) return;

    while (hasMore) {
      const response = await fetch(
        `${apiHost}/dataset/groups/${currentDataset.dataset.id}/${currentPage}`,
        {
          method: "GET",
          credentials: "include",
          headers: {
            "X-API-version": "2.0",
            "TR-Dataset": currentDataset.dataset.id,
            "Content-Type": "application/json",
          },
        },
      );

      if (response.ok) {
        // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
        const data = await response.json();
        if (isChunkGroupPageDTO(data)) {
          setAllGroupsList((prevGroups) => {
            return [...prevGroups, ...data.groups];
          });
          hasMore = currentPage < data.total_pages && currentPage < 100;
          currentPage++;
        } else {
          hasMore = false;
        }
      } else {
        hasMore = false;
      }

      if (hasMore) {
        await new Promise((resolve) => setTimeout(resolve, 750));
      }
    }
  };

  createEffect(() => {
    const currentDataset = $dataset?.();
    if (!currentDataset) return;

    void fetchAllGroups();
  });

  createEffect(() => {
    fetchChunkCollections();
    fetchBookmarks();
  });

  const dataset = createMemo(() => {
    if ($dataset) {
      return $dataset();
    } else {
      return null;
    }
  });

  const ShowServerTimings = () => {
    return (
      <div class="w-full self-start">
        <button
          onClick={() => {
            setShowServerTimings(!showServerTimings());
          }}
          class="flex cursor-pointer items-center space-x-2 self-start"
        >
          <label class="flex items-center space-x-2">
            <span class="text-sm font-medium text-gray-900 dark:text-white">
              Show Server Timings
            </span>
            <div class="text-primary-600 focus:ring-primary-500 h-3 w-3 rounded border-gray-300">
              <VsChevronRight
                classList={{
                  "transition-transform": true,
                  "rotate-90": showServerTimings(),
                }}
              />
            </div>
          </label>
        </button>
        <div class="w-full">
          <Show when={showServerTimings()}>
            <ServerTimings timings={serverTimings()} />
          </Show>
        </div>
      </div>
    );
  };

  createEffect(
    on([() => props.search.debounced.version, dataset, page], () => {
      const dataset = $dataset?.();
      if (!dataset) return;

      let sort_by;
      let mmr;

      if (isSortBySearchType(props.search.debounced.sort_by)) {
        props.search.debounced.sort_by.rerank_type != ""
          ? (sort_by = props.search.debounced.sort_by)
          : (sort_by = undefined);
      } else if (isSortByField(props.search.debounced.sort_by)) {
        props.search.debounced.sort_by.field != ""
          ? (sort_by = props.search.debounced.sort_by)
          : (sort_by = undefined);
      }

      if (!props.search.debounced.mmr.use_mmr) {
        mmr = undefined;
      } else {
        mmr = props.search.debounced.mmr;
      }

      let query: (string | number)[][] | string | { audio_base64: string } =
        props.search.debounced.audioBase64
          ? {
              audio_base64: props.search.debounced.audioBase64,
            }
          : props.search.debounced.query;

      query =
        props.search.debounced.multiQueries.length > 0
          ? props.search.debounced.multiQueries
              .map((q) => [q.query, q.weight])
              .filter((q) => q[0] != "")
          : query;

      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const requestBody: any = {
        query: query,
        page: page(),
        filters: props.search.debounced.filters ?? undefined,
        search_type: props.search.debounced.searchType.includes("autocomplete")
          ? props.search.debounced.searchType.replace("autocomplete-", "")
          : props.search.debounced.searchType,
        score_threshold: props.search.debounced.scoreThreshold,
        sort_options: {
          sort_by: sort_by,
          mmr: mmr,
        },
        slim_chunks: props.search.debounced.slimChunks ?? false,
        page_size: props.search.debounced.pageSize ?? 10,
        get_total_pages: props.search.debounced.getTotalPages ?? true,
        typo_options: {
          correct_typos: props.search.debounced.correctTypos,
          one_typo_word_range: {
            min: props.search.debounced.oneTypoWordRangeMin,
            max: props.search.debounced.oneTypoWordRangeMax,
          },
          two_typo_word_range: {
            min: props.search.debounced.twoTypoWordRangeMin,
            max: props.search.debounced.twoTypoWordRangeMax,
          },
          disable_on_word: props.search.debounced.disableOnWords,
          prioritize_domain_specifc_words:
            props.search.debounced.prioritize_domain_specifc_words,
        },
        highlight_options: {
          highlight_results: props.search.debounced.highlightResults ?? true,
          highlight_strategy:
            props.search.debounced.highlightStrategy ?? "exactmatch",
          highlight_threshold: props.search.debounced.highlightThreshold,
          highlight_delimiters: props.search.debounced.highlightDelimiters ?? [
            "?",
            ".",
            "!",
          ],
          highlight_max_length: props.search.debounced.highlightMaxLength ?? 8,
          highlight_max_num: props.search.debounced.highlightMaxNum ?? 3,
          highlight_window: props.search.debounced.highlightWindow ?? 0,
          pre_tag: props.search.debounced.highlightPreTag ?? "<mark><b>",
          post_tag: props.search.debounced.highlightPostTag ?? "</b></mark>",
        },

        group_size: props.search.debounced.group_size ?? 3,
        use_quote_negated_terms: props.search.debounced.useQuoteNegatedTerms,
        remove_stop_words: props.search.debounced.removeStopWords,
        scoring_options: {},
      };

      if (
        props.search.debounced.scoringOptions?.fulltext_boost?.boost_factor &&
        props.search.debounced.scoringOptions?.fulltext_boost?.phrase
      ) {
        requestBody.scoring_options.fulltext_boost = {
          boost_factor:
            props.search.debounced.scoringOptions.fulltext_boost.boost_factor,
          phrase: props.search.debounced.scoringOptions.fulltext_boost.phrase,
        };
      }
      if (
        props.search.debounced.scoringOptions?.semantic_boost
          ?.distance_factor &&
        props.search.debounced.scoringOptions?.semantic_boost?.phrase
      ) {
        requestBody.scoring_options.semantic_boost = {
          distance_factor:
            props.search.debounced.scoringOptions.semantic_boost
              .distance_factor,
          phrase: props.search.debounced.scoringOptions.semantic_boost.phrase,
        };
      }

      let searchRoute = "";
      let groupUnique = false;
      if (
        !props.search.debounced.query ||
        props.search.debounced.query === ""
      ) {
        searchRoute = "chunks/scroll";
        if (sort_by && isSortByField(sort_by)) {
          requestBody["sort_by"] = sort_by;
        }
      } else {
        searchRoute = "chunk/search";
        groupUnique = props.search.debounced.groupUniqueSearch;
        if (groupUnique) {
          searchRoute = "chunk_group/group_oriented_search";
        }

        if (props.search.debounced.searchType.includes("autocomplete")) {
          searchRoute = "chunk/autocomplete";
          requestBody["extend_results"] =
            props.search.debounced.extendResults ?? false;
        }
      }

      setLoading(true);

      setGroupResultChunks([]);
      setResultChunks([]);
      setNoResults(false);

      const abortController = new AbortController();

      void fetch(`${apiHost}/${searchRoute}`, {
        method: "POST",
        headers: {
          "X-API-version": "2.0",
          "Content-Type": "application/json",
          "TR-Dataset": dataset.dataset.id,
        },
        signal: abortController.signal,
        credentials: "include",
        body: JSON.stringify(requestBody),
      }).then((response) => {
        if (response.ok) {
          void response.json().then((data) => {
            let resultingChunks: any = [];
            if (groupUnique) {
              const groupResult = data.results as GroupScoreChunkDTO[];
              setTotalPages(data.total_pages);
              setSearchID(data.id);
              setCorrectedQuery(data.corrected_query);
              setGroupResultChunks(groupResult);

              resultingChunks = groupResult.flatMap((groupChunkDTO) => {
                return groupChunkDTO.chunks;
              });
            } else {
              resultingChunks = data.chunks;
              resultingChunks = resultingChunks.map((chunk: any) => {
                if (!Object.keys(chunk).includes("score")) {
                  return {
                    chunk: chunk,
                    score: 0,
                  };
                } else {
                  return chunk;
                }
              });
              setSearchID(data.id);
              setResultChunks(resultingChunks);
              setCorrectedQuery(data.corrected_query);
              setTotalPages(data.total_pages);
            }

            if (resultingChunks.length === 0) {
              setNoResults(true);
            }

            // Handle server timing
            const serverTiming = response.headers.get("Server-Timing");
            if (serverTiming) {
              const metrics = serverTiming.split(",");
              try {
                setServerTimings(parseServerTimings(metrics));
              } catch {
                setServerTimings([]);
                console.error("Failed to parse server timing");
              }
            }
          });
        } else {
          void response
            .json()
            .then((data) => {
              createToast({
                type: "error",
                message: data.message,
              });
            })
            .catch(() => {
              createToast({
                type: "error",
                message: "An unknown error occurred while searching",
              });
            });

          setNoResults(true);
        }

        setClientSideRequestFinished(true);

        setLoading(false);
      });

      onCleanup(() => {
        abortController.abort("cleanup");
      });
    }),
  );

  createEffect(() => {
    if (!openChat()) {
      setSelectedIds((prev) => (prev.length < 10 ? prev : []));
    }
  });

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const renderMetadataElements = (value: any) => {
    if (Array.isArray(value)) {
      // Determine if the array consists solely of objects
      const allObjects = value.every(
        (item) => typeof item === "object" && item !== null,
      );

      return (
        <div>
          <For each={value}>
            {/* eslint-disable-next-line @typescript-eslint/no-explicit-any */}
            {(item: any, itemIndex: () => number) => (
              <span>
                {typeof item === "object"
                  ? renderMetadataElements(item)
                  : item.toString()}
                {itemIndex() < value.length - 1 &&
                  (allObjects ? (
                    <hr class="my-2 border-neutral-400 dark:border-neutral-400" />
                  ) : (
                    <span>, </span>
                  ))}
              </span>
            )}
          </For>
        </div>
      );
    } else if (typeof value === "object" && value !== null) {
      return (
        <div class="pl-2">
          <For each={Object.keys(value)}>
            {(subKey: string) => (
              <div>
                <div class="flex space-x-1">
                  <span class="font-semibold italic text-neutral-700 dark:text-neutral-200">
                    {subKey}:
                  </span>
                  <span class="text-neutral-700 dark:text-neutral-300">
                    {renderMetadataElements(value[subKey])}
                  </span>
                </div>
              </div>
            )}
          </For>
        </div>
      );
    } else {
      return value !== null && value !== undefined ? value.toString() : "null";
    }
  };

  return (
    <>
      <Show when={openChat()}>
        <Portal>
          <FullScreenModal isOpen={openChat} setIsOpen={setOpenChat}>
            <div class="max-h-[75vh] min-h-[75vh] min-w-[75vw] max-w-[75vw] overflow-y-auto rounded-md scrollbar-thin">
              <ChatPopup
                chunks={resultChunks}
                groupChunks={groupResultChunks}
                selectedIds={selectedIds}
                setOpenChat={setOpenChat}
              />
            </div>
          </FullScreenModal>
        </Portal>
      </Show>
      <div class="flex w-full flex-col items-center gap-4 pt-5">
        <Switch>
          <Match when={loading()}>
            <div
              class="text-primary inline-block h-12 w-12 animate-spin rounded-full border-4 border-solid border-current border-magenta border-r-transparent align-[-0.125em] motion-reduce:animate-[spin_1.5s_linear_infinite]"
              role="status"
            >
              <span class="!absolute !-m-px !h-px !w-px !overflow-hidden !whitespace-nowrap !border-0 !p-0 ![clip:rect(0,0,0,0)]">
                Loading...
              </span>
            </div>
          </Match>
          <Match when={noResults()}>
            <div class="mt-6 flex flex-col items-center">
              <p class="text-3xl">No Results Found</p>
              <p class="text-lg">You may need to adjust your filters</p>
            </div>
          </Match>
          <Match when={!loading() && groupResultChunks().length == 0}>
            <Show when={correctedQuery()}>
              <div class="flex w-full flex-col">
                <div class="text-lg">
                  {" "}
                  Showing results for{" "}
                  <a
                    class="font-bold text-blue-500"
                    href={`${new URL(window.location.href).origin}${
                      new URL(window.location.href).pathname
                    }?${new URLSearchParams({
                      ...Object.fromEntries(
                        new URL(window.location.href).searchParams,
                      ),
                      query: correctedQuery(),
                    }).toString()}`}
                    onClick={() => {
                      props.search.setSearch({
                        query: correctedQuery(),
                      });
                    }}
                  >
                    {correctedQuery()}
                  </a>
                </div>
                <div>
                  Search instead for
                  <a
                    class="pl-1 font-bold text-blue-500"
                    href={`${new URL(window.location.href).origin}${
                      new URL(window.location.href).pathname
                    }?${new URLSearchParams({
                      ...Object.fromEntries(
                        new URL(window.location.href).searchParams,
                      ),
                      query: `"${props.search.debounced.query}"`,
                    }).toString()}`}
                    onClick={() => {
                      props.search.setSearch({
                        query: `"${props.search.debounced.query}"`,
                      });
                    }}
                  >
                    {props.search.debounced.query}
                  </a>
                </div>
              </div>
            </Show>

            <ShowServerTimings />
            <div class="flex w-full max-w-screen-2xl flex-col space-y-4">
              <For each={resultChunks()}>
                {(chunk, i) => (
                  <div>
                    <ScoreChunk
                      totalGroupPages={totalCollectionPages()}
                      chunkGroups={chunkCollections()}
                      allGroupsList={allGroupsList()}
                      chunk={chunk.chunk}
                      score={chunk.score}
                      bookmarks={bookmarks()}
                      setOnDelete={setOnDelete}
                      setShowConfirmModal={setShowConfirmDeleteModal}
                      showExpand={clientSideRequestFinished()}
                      defaultShowMetadata={props.search.state.slimChunks}
                      setChunkGroups={setChunkCollections}
                      setSelectedIds={setSelectedIds}
                      selectedIds={selectedIds}
                      registerClickForChunk={({ id, eventType }) =>
                        registerClickForChunk({
                          id: id,
                          eventType: eventType,
                          position: i(),
                          searchID: searchID(),
                        })
                      }
                    />
                  </div>
                )}
              </For>
            </div>
            <Show when={resultChunks().length > 0}>
              <div class="mx-auto my-12 flex items-center space-x-2">
                <PaginationController
                  setPage={setPage}
                  page={page()}
                  totalPages={totalPages()}
                />
              </div>
            </Show>
            <div>
              <div
                data-dial-init
                class="group fixed bottom-6 right-6"
                onMouseEnter={() => {
                  document
                    .getElementById("speed-dial-menu-text-outside-button")
                    ?.classList.remove("hidden");
                  document
                    .getElementById("speed-dial-menu-text-outside-button")
                    ?.classList.add("flex");
                }}
                onMouseLeave={() => {
                  document
                    .getElementById("speed-dial-menu-text-outside-button")
                    ?.classList.add("hidden");
                  document
                    .getElementById("speed-dial-menu-text-outside-button")
                    ?.classList.remove("flex");
                }}
              >
                <div
                  id="speed-dial-menu-text-outside-button"
                  class="mb-4 hidden flex-col items-center space-y-2"
                >
                  <button
                    type="button"
                    class="relative h-[52px] w-[52px] items-center justify-center rounded-lg border border-gray-200 bg-white text-gray-500 shadow-sm hover:bg-gray-50 hover:text-gray-900 focus:outline-none focus:ring-4 focus:ring-gray-300 dark:border-gray-600 dark:bg-gray-700 dark:text-gray-400 dark:hover:bg-gray-600 dark:hover:text-white dark:focus:ring-gray-400"
                    onClick={() => {
                      setSelectedIds(
                        resultChunks()
                          .flatMap((c) => {
                            return c.chunk.id;
                          })
                          .slice(0, 10),
                      );
                      setOpenChat(true);
                    }}
                  >
                    <IoDocumentsOutline class="mx-auto h-7 w-7" />
                    <span class="font-sm absolute -left-[8.5rem] top-1/2 mb-px block -translate-y-1/2 break-words bg-white/30 text-sm backdrop-blur-xl dark:bg-black/30">
                      Chat with all results
                    </span>
                  </button>
                  <button
                    type="button"
                    class="relative h-[52px] w-[52px] items-center justify-center rounded-lg border border-gray-200 bg-white text-gray-500 shadow-sm hover:bg-gray-50 hover:text-gray-900 focus:outline-none focus:ring-4 focus:ring-gray-300 dark:border-gray-600 dark:bg-gray-700 dark:text-gray-400 dark:hover:bg-gray-600 dark:hover:text-white dark:focus:ring-gray-400"
                    onClick={() => {
                      setOpenChat(true);
                    }}
                  >
                    <IoDocumentOutline class="mx-auto h-7 w-7" />
                    <span class="font-sm absolute -left-[10.85rem] top-1/2 mb-px block -translate-y-1/2 bg-white/30 text-sm backdrop-blur-xl dark:bg-black/30">
                      Chat with selected results
                    </span>
                  </button>
                </div>
                <button
                  type="button"
                  class="flex h-14 w-14 items-center justify-center rounded-lg bg-magenta-500 text-white hover:bg-magenta-400 focus:outline-none focus:ring-4 focus:ring-magenta-300 dark:bg-magenta-500 dark:hover:bg-magenta-400 dark:focus:ring-magenta-600"
                >
                  <AiOutlineRobot class="h-7 w-7 fill-current text-white" />
                  <span class="sr-only">Open actions menu</span>
                </button>
              </div>
            </div>
          </Match>
          <Match when={!loading() && groupResultChunks().length > 0}>
            <ShowServerTimings />
            <For each={groupResultChunks()}>
              {(groupResult) => {
                const [groupExpanded, setGroupExpanded] = createSignal(true);
                const [expandGroupMetadata, setExpandGroupMetadata] =
                  createSignal(false);

                const toggle = () => {
                  setGroupExpanded(!groupExpanded());
                };

                return (
                  <div class="flex w-full max-w-screen-2xl flex-col gap-4">
                    <div
                      classList={{
                        "flex items-center space-x-4 rounded bg-neutral-100 px-4 py-4 dark:bg-neutral-800":
                          true,
                        "-mb-2": groupExpanded(),
                      }}
                    >
                      <button onClick={toggle}>
                        <Show when={groupExpanded()}>
                          <FaSolidChevronUp />
                        </Show>
                        <Show when={!groupExpanded()}>
                          <FaSolidChevronDown />
                        </Show>
                      </button>
                      <div class="flex w-full items-center">
                        <div class="w-full">
                          <div class="flex space-x-2">
                            <span class="font-semibold text-neutral-800 dark:text-neutral-200">
                              ID:{" "}
                            </span>
                            <span class="line-clamp-1 break-all">
                              {groupResult.group.id}
                            </span>
                          </div>
                          <Show when={groupResult.group.tracking_id}>
                            <div class="flex space-x-2">
                              <span class="font-semibold text-neutral-800 dark:text-neutral-200">
                                Tracking ID:{" "}
                              </span>
                              <span class="line-clamp-1 break-all">
                                {groupResult.group.tracking_id}
                              </span>
                            </div>
                          </Show>
                          <Show when={groupResult.group.name}>
                            <div class="flex w-full flex-row justify-between">
                              <div class="flex space-x-2">
                                <span class="font-semibold text-neutral-800 dark:text-neutral-200">
                                  Name:{" "}
                                </span>
                                <span class="line-clamp-1 break-all">
                                  {groupResult.group.name}
                                </span>
                              </div>
                            </div>
                          </Show>
                          <Show
                            when={
                              groupResult.group.tag_set?.filter((tag) => tag)
                                .length
                            }
                          >
                            <div class="flex w-full flex-row justify-between">
                              <div class="flex space-x-2">
                                <span class="text-nowrap font-semibold text-neutral-800 dark:text-neutral-200">
                                  Tag Set:{" "}
                                </span>
                                <span class="line-clamp-1 break-all">
                                  {groupResult.group.tag_set?.join(",")}
                                </span>
                              </div>
                            </div>
                          </Show>
                          <Show
                            when={
                              Object.keys(groupResult.group.metadata ?? {})
                                .length > 0
                            }
                          >
                            <button
                              class="mt-2 flex w-fit items-center space-x-1 rounded-md border bg-neutral-200/50 px-2 py-1 font-semibold text-magenta-500 hover:bg-neutral-200/90 dark:bg-neutral-700/60 dark:text-magenta-400"
                              onClick={() =>
                                setExpandGroupMetadata((prev) => !prev)
                              }
                            >
                              <span>
                                {expandGroupMetadata()
                                  ? "Collapse Metadata"
                                  : "Expand Metadata"}
                              </span>
                              <Switch>
                                <Match when={expandGroupMetadata()}>
                                  <BiRegularChevronUp class="h-5 w-5 fill-current" />
                                </Match>
                                <Match when={!expandGroupMetadata()}>
                                  <BiRegularChevronDown class="h-5 w-5 fill-current" />
                                </Match>
                              </Switch>
                            </button>
                          </Show>
                          <Show when={expandGroupMetadata()}>
                            <div class="pl-2 pt-2">
                              <For
                                each={Object.keys(
                                  groupResult.group.metadata ?? {},
                                )}
                              >
                                {(key) => (
                                  <Show
                                    when={
                                      // eslint-disable-next-line @typescript-eslint/no-explicit-any
                                      (groupResult.group.metadata as any)[
                                        key
                                      ] !== undefined
                                    }
                                  >
                                    <div class="mb-4">
                                      <div class="flex space-x-2">
                                        <span class="font-semibold text-neutral-800 dark:text-neutral-200">
                                          {key}:{" "}
                                        </span>
                                        <span class="line-clamp-1 break-all">
                                          {groupResult.group.metadata &&
                                            renderMetadataElements(
                                              groupResult.group.metadata[
                                                key as keyof typeof groupResult.group.metadata
                                              ],
                                            )}
                                        </span>
                                      </div>
                                    </div>
                                  </Show>
                                )}
                              </For>
                            </div>
                          </Show>
                        </div>
                        <div class="flex items-center space-x-3">
                          <Show when={groupResult.file_id}>
                            {(fileId) => (
                              <button
                                title="Download uploaded file"
                                class="h-fit text-neutral-400 dark:text-neutral-300"
                                onClick={() => handleDownloadFile(fileId())}
                              >
                                <FaSolidDownload />
                              </button>
                            )}
                          </Show>
                          <Tooltip
                            body={
                              <a
                                href={`/group/${
                                  groupResult.group.id
                                }?dataset=${dataset()?.dataset.id}&edit=true`}
                              >
                                <FiEdit class="h-5 w-5" />
                              </a>
                            }
                            tooltipText="Edit chunk"
                          />
                          <a
                            title="Open group to edit, view its chunks, or test group recommendations"
                            href={`/group/${
                              groupResult.group.id
                            }?dataset=${dataset()?.dataset.id}`}
                          >
                            <FiEye class="h-5 w-5" />
                          </a>
                        </div>
                      </div>
                    </div>
                    <Show when={groupExpanded()}>
                      <For each={groupResult.chunks}>
                        {(chunk, i) => (
                          <div class="ml-5 flex space-y-4">
                            <ScoreChunk
                              totalGroupPages={totalCollectionPages()}
                              allGroupsList={allGroupsList()}
                              chunkGroups={chunkCollections()}
                              chunk={chunk.chunk}
                              score={chunk.score}
                              bookmarks={bookmarks()}
                              setOnDelete={setOnDelete}
                              setShowConfirmModal={setShowConfirmDeleteModal}
                              showExpand={clientSideRequestFinished()}
                              setChunkGroups={setChunkCollections}
                              setSelectedIds={setSelectedIds}
                              selectedIds={selectedIds}
                              registerClickForChunk={({ id, eventType }) =>
                                registerClickForChunk({
                                  id: id,
                                  eventType: eventType,
                                  position: i(),
                                  searchID: searchID(),
                                })
                              }
                            />
                          </div>
                        )}
                      </For>
                    </Show>
                  </div>
                );
              }}
            </For>
            <Show when={groupResultChunks().length > 0}>
              <div class="mx-auto my-12 flex items-center space-x-2">
                <PaginationController
                  setPage={setPage}
                  page={page()}
                  totalPages={totalPages()}
                />
              </div>
            </Show>
            <div>
              <div
                data-dial-init
                class="group fixed bottom-6 right-6"
                onMouseEnter={() => {
                  document
                    .getElementById("speed-dial-menu-text-outside-button")
                    ?.classList.remove("hidden");
                  document
                    .getElementById("speed-dial-menu-text-outside-button")
                    ?.classList.add("flex");
                }}
                onMouseLeave={() => {
                  document
                    .getElementById("speed-dial-menu-text-outside-button")
                    ?.classList.add("hidden");
                  document
                    .getElementById("speed-dial-menu-text-outside-button")
                    ?.classList.remove("flex");
                }}
              >
                <div
                  id="speed-dial-menu-text-outside-button"
                  class="mb-4 hidden flex-col items-center space-y-2"
                >
                  <button
                    type="button"
                    class="relative h-[52px] w-[52px] items-center justify-center rounded-lg border border-gray-200 bg-white text-gray-500 shadow-sm hover:bg-gray-50 hover:text-gray-900 focus:outline-none focus:ring-4 focus:ring-gray-300 dark:border-gray-600 dark:bg-gray-700 dark:text-gray-400 dark:hover:bg-gray-600 dark:hover:text-white dark:focus:ring-gray-400"
                    onClick={() => {
                      setSelectedIds(
                        groupResultChunks()
                          .flatMap((g) => {
                            return g.chunks;
                          })
                          .flatMap((c) => {
                            return c.chunk.id;
                          }),
                      );
                      setOpenChat(true);
                    }}
                  >
                    <IoDocumentsOutline class="mx-auto h-7 w-7" />
                    <span class="font-sm absolute -left-[8.5rem] top-1/2 mb-px block -translate-y-1/2 break-words bg-white/30 text-sm backdrop-blur-xl dark:bg-black/30">
                      Chat with all results
                    </span>
                  </button>
                  <button
                    type="button"
                    class="relative h-[52px] w-[52px] items-center justify-center rounded-lg border border-gray-200 bg-white text-gray-500 shadow-sm hover:bg-gray-50 hover:text-gray-900 focus:outline-none focus:ring-4 focus:ring-gray-300 dark:border-gray-600 dark:bg-gray-700 dark:text-gray-400 dark:hover:bg-gray-600 dark:hover:text-white dark:focus:ring-gray-400"
                    onClick={() => {
                      setOpenChat(true);
                    }}
                  >
                    <IoDocumentOutline class="mx-auto h-7 w-7" />
                    <span class="font-sm absolute -left-[10.85rem] top-1/2 mb-px block -translate-y-1/2 bg-white/30 text-sm backdrop-blur-xl dark:bg-black/30">
                      Chat with selected results
                    </span>
                  </button>
                </div>
                <button
                  type="button"
                  class="flex h-14 w-14 items-center justify-center rounded-lg bg-magenta-500 text-white hover:bg-magenta-400 focus:outline-none focus:ring-4 focus:ring-magenta-300 dark:bg-magenta-500 dark:hover:bg-magenta-400 dark:focus:ring-magenta-600"
                >
                  <AiOutlineRobot class="h-7 w-7 fill-current text-white" />
                  <span class="sr-only">Open actions menu</span>
                </button>
              </div>
            </div>
          </Match>
        </Switch>
      </div>
      <ConfirmModal
        showConfirmModal={showConfirmDeleteModal}
        setShowConfirmModal={setShowConfirmDeleteModal}
        onConfirm={onDelete}
        message="Are you sure you want to delete this chunk?"
      />
      <Show when={props.rateQuery()}>
        <FullScreenModal
          isOpen={props.rateQuery}
          setIsOpen={props.setRatingQuery}
        >
          <div class="min-w-[250px] sm:min-w-[300px]">
            <div class="mb-4 text-center text-xl font-bold text-black dark:text-white">
              Rate query:
            </div>
            <div>
              <label class="block text-lg font-medium text-black dark:text-white">
                Rating: {rating().rating}
              </label>
              <input
                type="range"
                class="min-w-full"
                value={rating().rating}
                min="0"
                max="10"
                onInput={(e) => {
                  setRating({
                    rating: parseInt(e.target.value),
                    note: rating().note,
                  });
                }}
              />
              <div class="flex justify-between space-x-1">
                <label class="block text-sm font-medium text-black dark:text-white">
                  0
                </label>
                <label class="block items-end text-sm font-medium text-black dark:text-white">
                  10
                </label>
              </div>
            </div>
            <div>
              <label class="block text-lg font-medium text-black dark:text-white">
                Note:
              </label>
              <textarea
                class="max-md w-full justify-start rounded-md bg-neutral-200 px-2 py-1 dark:bg-neutral-700 dark:text-white"
                value={rating().note}
                onInput={(e) => {
                  setRating({
                    rating: rating().rating,
                    note: (e.target as HTMLTextAreaElement).value,
                  });
                }}
              />
            </div>
            <div class="mx-auto flex w-fit flex-col space-y-3 pt-2">
              <button
                class="flex space-x-2 rounded-md bg-magenta-500 p-2 text-white"
                onClick={() => {
                  rateQuery();
                  props.setRatingQuery(false);
                }}
              >
                Submit Rating
              </button>
            </div>
          </div>
        </FullScreenModal>
      </Show>
    </>
  );
};

export default ResultsPage;
