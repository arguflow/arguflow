/* eslint-disable @typescript-eslint/no-unsafe-call */
/* eslint-disable @typescript-eslint/no-unsafe-argument */
/* eslint-disable @typescript-eslint/no-unsafe-assignment */
/* eslint-disable @typescript-eslint/no-unsafe-member-access */
/* eslint-disable @typescript-eslint/no-unsafe-return */
/* eslint-disable @typescript-eslint/no-explicit-any */
import {
  Show,
  createEffect,
  createSignal,
  For,
  onCleanup,
  Switch,
  Match,
  createMemo,
  useContext,
  on,
} from "solid-js";
import {
  type ChunkGroupDTO,
  type ChunkGroupBookmarkDTO,
  ScoreChunkDTO,
  isScoreChunkDTO,
  isChunkGroupPageDTO,
  ChunkMetadata,
  ChunkBookmarksDTO,
  GroupScoreChunkDTO,
} from "../utils/apiTypes";
import { FullScreenModal } from "./Atoms/FullScreenModal";
import { FiEdit, FiTrash } from "solid-icons/fi";
import { ConfirmModal } from "./Atoms/ConfirmModal";
import { PaginationController } from "./Atoms/PaginationController";
import SearchForm from "./SearchForm";
import ChunkMetadataDisplay from "./ChunkMetadataDisplay";
import { Portal } from "solid-js/web";
import { ChatPopup } from "./ChatPopup";
import { AiOutlineRobot } from "solid-icons/ai";
import { IoDocumentOutline, IoDocumentsOutline } from "solid-icons/io";
import { useNavigate } from "@solidjs/router";
import { DatasetAndUserContext } from "./Contexts/DatasetAndUserContext";
import {
  FaSolidChevronDown,
  FaSolidChevronUp,
  FaSolidDownload,
} from "solid-icons/fa";
import {
  isSortByField,
  isSortBySearchType,
  useSearch,
} from "../hooks/useSearch";
import { downloadFile } from "../utils/downloadFile";
import ScoreChunk from "./ScoreChunk";
import {
  BiRegularChevronDown,
  BiRegularChevronUp,
  BiRegularXCircle,
} from "solid-icons/bi";
import { createToast } from "./ShowToasts";
import { FiEye } from "solid-icons/fi";
import { useCtrClickForChunk } from "../hooks/useCtrAnalytics";
import { ChunkGroupAndFileId } from "trieve-ts-sdk";
import { JsonInput } from "shared/ui";

export interface GroupPageProps {
  groupID: string;
}

export const GroupPage = (props: GroupPageProps) => {
  const apiHost: string = import.meta.env.VITE_API_HOST as string;
  const datasetAndUserContext = useContext(DatasetAndUserContext);
  const search = useSearch();
  const { registerClickForChunk } = useCtrClickForChunk();
  const $dataset = datasetAndUserContext.currentDataset;
  const navigate = useNavigate();

  const searchChunkMetadatasWithVotes: ScoreChunkDTO[] = [];

  const [page, setPage] = createSignal<number>(1);

  const [searchLoading, setSearchLoading] = createSignal(false);
  const [chunkMetadatas, setChunkMetadatas] = createSignal<ChunkMetadata[]>([]);
  const [searchWithinGroupResults, setSearchWithinGroupResults] = createSignal<
    ScoreChunkDTO[]
  >(searchChunkMetadatasWithVotes);
  const [clientSideRequestFinished, setClientSideRequestFinished] =
    createSignal(false);
  const [groupInfo, setGroupInfo] = createSignal<ChunkGroupAndFileId | null>(
    null,
  );
  const [chunkGroups, setChunkGroups] = createSignal<ChunkGroupDTO[]>([]);
  const [bookmarks, setBookmarks] = createSignal<ChunkBookmarksDTO[]>([]);
  const [fetchingGroups, setFetchingGroups] = createSignal(false);
  const [deleting, setDeleting] = createSignal(false);
  const [editing, setEditing] = createSignal(false);
  const [descendTagSet, setDescendTagSet] = createSignal(false);
  const [expandGroupMetadata, setExpandGroupMetadata] = createSignal(false);
  const $currentUser = datasetAndUserContext.user;
  const [totalPages, setTotalPages] = createSignal(0);
  const [loadingRecommendations, setLoadingRecommendations] =
    createSignal(false);
  const [recommendedChunks, setRecommendedChunks] = createSignal<
    ScoreChunkDTO[]
  >([]);
  const [showConfirmDeleteModal, setShowConfirmDeleteModal] =
    createSignal(false);
  const [showConfirmGroupDeleteModal, setShowConfirmGroupDeleteModal] =
    createSignal(false);
  const [deleteChunksInGroup, setDeleteChunksInGroup] = createSignal(false);
  const [totalGroupPages, setTotalGroupPages] = createSignal(1);
  // eslint-disable-next-line @typescript-eslint/no-empty-function
  const [onDelete, setOnDelete] = createSignal(() => {});
  // eslint-disable-next-line @typescript-eslint/no-empty-function
  const [onGroupDelete, setOnGroupDelete] = createSignal<
    (delete_chunks: boolean) => void
  >(() => {});
  const [openChat, setOpenChat] = createSignal(false);
  const [selectedIds, setSelectedIds] = createSignal<string[]>([]);
  const [groupRecommendations, setGroupRecommendations] = createSignal(false);
  const [groupRecommendedChunks, setGroupRecommendedChunks] = createSignal<
    GroupScoreChunkDTO[]
  >([]);
  const [searchID, setSearchID] = createSignal("");
  const [openRateQueryModal, setOpenRateQueryModal] = createSignal(false);
  const [rating, setRating] = createSignal({
    rating: 5,
    note: "",
  });
  const [allGroupsList, setAllGroupsList] = createSignal<ChunkGroupDTO[]>([]);

  createEffect(() => {
    fetchBookmarks();
    const urlParams = new URLSearchParams(window.location.search);
    const editSearch = urlParams.get("edit") === "true";
    setEditing(editSearch);
  });

  createEffect((prevGroupId) => {
    const curGroupId = props.groupID;
    const urlParams = new URLSearchParams(window.location.search);
    const editSearch = urlParams.get("edit") === "true";
    if (curGroupId !== prevGroupId) {
      setPage(1);
      setGroupRecommendations(false);
      setGroupRecommendedChunks([]);
      setRecommendedChunks([]);
      setLoadingRecommendations(false);
      setSearchLoading(false);
      setEditing(editSearch);
    }

    return curGroupId;
  }, "");

  const sendRating = () => {
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
    const resultsLength = chunkMetadatas().length;
    if (!openChat()) {
      setSelectedIds((prev) => (prev.length < resultsLength ? prev : []));
    }
  });

  createEffect((prevGroupRecVal) => {
    const curGroupRecVal = groupRecommendations();

    if (curGroupRecVal && !prevGroupRecVal) {
      setRecommendedChunks([]);
    } else {
      setGroupRecommendedChunks([]);
    }

    return curGroupRecVal;
  });

  const dataset = createMemo(() => {
    if ($dataset) {
      return $dataset();
    } else {
      return null;
    }
  });

  createEffect(
    on([() => search.debounced.version, page, dataset], () => {
      const abortController = new AbortController();
      let group_id: string | null = null;
      const currentDataset = $dataset?.();
      if (!currentDataset) return;

      if (search.debounced.query === "") {
        void fetch(`${apiHost}/chunk_group/${props.groupID}/${page()}`, {
          method: "GET",
          credentials: "include",
          signal: abortController.signal,
          headers: {
            "X-API-version": "2.0",
            "TR-Dataset": currentDataset.dataset.id,
          },
        }).then((response) => {
          if (response.ok) {
            void response.json().then((data) => {
              const groupBookmarks = data as ChunkGroupBookmarkDTO;
              group_id = groupBookmarks.group.id;
              setGroupInfo(groupBookmarks.group);
              setTotalPages(groupBookmarks.total_pages);
              setChunkMetadatas(groupBookmarks.chunks);
            });
          } else if (response.status == 404) {
            setGroupInfo(null);
            setTotalPages(0);
            setChunkMetadatas([]);

            createToast({
              type: "error",
              message: "Group not found for this dataset",
            });
          }
          setClientSideRequestFinished(true);
        });
      } else {
        setSearchLoading(true);

        let sort_by;

        if (isSortBySearchType(search.debounced.sort_by)) {
          search.debounced.sort_by.rerank_type != ""
            ? (sort_by = search.debounced.sort_by)
            : (sort_by = undefined);
        } else if (isSortByField(search.debounced.sort_by)) {
          search.debounced.sort_by.field != ""
            ? (sort_by = search.debounced.sort_by)
            : (sort_by = undefined);
        }

        void fetch(`${apiHost}/chunk_group/search`, {
          method: "POST",
          headers: {
            "X-API-version": "2.0",
            "Content-Type": "application/json",
            "TR-Dataset": currentDataset.dataset.id,
          },
          signal: abortController.signal,
          credentials: "include",
          body: JSON.stringify({
            query: search.debounced.query,
            page: page(),
            score_threshold: search.debounced.scoreThreshold,
            group_id: props.groupID,
            search_type: search.debounced.searchType,
            slim_chunks: search.debounced.slimChunks,
            page_size: search.debounced.pageSize,
            get_total_pages: search.debounced.getTotalPages,
            typo_options: {
              correct_typos: search.debounced.correctTypos,
              one_typo_word_range: {
                min: search.debounced.oneTypoWordRangeMin,
                max: search.debounced.oneTypoWordRangeMax,
              },
              two_typo_word_range: {
                min: search.debounced.twoTypoWordRangeMin,
                max: search.debounced.twoTypoWordRangeMax,
              },
              disable_on_words: search.debounced.disableOnWords,
            },
            highlight_options: {
              highlight_results: search.debounced.highlightResults,
              highlight_strategy: search.debounced.highlightStrategy,
              highlight_threshold: search.debounced.highlightThreshold,
              highlight_delimiters: search.debounced.highlightDelimiters,
              highlight_max_length: search.debounced.highlightMaxLength,
              highlight_window: search.debounced.highlightWindow,
              pre_tag: search.debounced.highlightPreTag,
              post_tag: search.debounced.highlightPostTag,
            },
            sort_options: {
              sort_by: sort_by,
            },
            use_quote_negated_terms: search.debounced.useQuoteNegatedTerms,
            remove_stop_words: search.debounced.removeStopWords,
          }),
        }).then((response) => {
          if (response.ok) {
            void response.json().then((data) => {
              setSearchID(data.id);
              setTotalPages(data.total_pages);
              setSearchWithinGroupResults(data.chunks);
            });
          }
          setClientSideRequestFinished(true);
          setSearchLoading(false);
        });

        onCleanup(() => {
          abortController.abort("cleanup");
        });
      }

      fetchChunkGroups();

      setOnGroupDelete(() => {
        return (delete_chunks: boolean) => {
          setDeleting(true);
          if (group_id === null) return;

          void fetch(
            `${apiHost}/chunk_group/${group_id}?delete_chunks=${delete_chunks.toString()}`,
            {
              method: "DELETE",
              credentials: "include",
              headers: {
                "X-API-version": "2.0",
                "Content-Type": "application/json",
                "TR-Dataset": currentDataset.dataset.id,
              },
              signal: abortController.signal,
            },
          ).then((response) => {
            setDeleting(false);
            if (response.ok) {
              navigate(`/`);
            }
            if (response.status == 403) {
              setDeleting(false);
            }
          });
        };
      });
    }),
  );

  // Fetch the chunk groups for the auth'ed user
  const fetchChunkGroups = () => {
    const currentDataset = $dataset?.();
    if (!currentDataset) return;
    if (!$currentUser?.()) return;

    void fetch(`${apiHost}/dataset/groups/${currentDataset.dataset.id}/1`, {
      method: "GET",
      credentials: "include",
      headers: {
        "X-API-version": "2.0",
        "TR-Dataset": currentDataset.dataset.id,
      },
    }).then((response) => {
      if (response.ok) {
        void response.json().then((data) => {
          if (isChunkGroupPageDTO(data)) {
            setChunkGroups(data.groups);
            setTotalGroupPages(data.total_pages);
          }
        });
      }
    });
  };

  const handleDownloadFile = (group: ChunkGroupAndFileId | null) => {
    const datasetId = $dataset?.()?.dataset.id;
    if (group && group.file_id && datasetId) {
      void downloadFile(group.file_id, datasetId);
    }
  };

  const fetchBookmarks = () => {
    const currentDataset = $dataset?.();
    if (!currentDataset) return;

    void fetch(`${apiHost}/chunk_group/chunks`, {
      method: "POST",
      credentials: "include",
      headers: {
        "X-API-version": "2.0",
        "Content-Type": "application/json",
        "TR-Dataset": currentDataset.dataset.id,
      },
      body: JSON.stringify({
        chunk_ids: chunkMetadatas().flatMap((m) => {
          return m.id;
        }),
      }),
    }).then((response) => {
      if (response.ok) {
        void response.json().then((data: ChunkBookmarksDTO[]) => {
          setBookmarks(data);
        });
      }
    });
  };

  const updateGroup = () => {
    const currentDataset = $dataset?.();
    if (!currentDataset) return;

    setFetchingGroups(true);
    const body = {
      group_id: groupInfo()?.id,
      name: groupInfo()?.name,
      tracking_id:
        groupInfo()?.tracking_id == "" ? null : groupInfo()?.tracking_id,
      tag_set: groupInfo()?.tag_set,
      description: groupInfo()?.description,
      metadata: groupInfo()?.metadata,
      update_chunks: descendTagSet(),
    };
    void fetch(`${apiHost}/chunk_group`, {
      method: "PUT",
      credentials: "include",
      body: JSON.stringify(body),
      headers: {
        "X-API-version": "2.0",
        "Content-Type": "application/json",
        "TR-Dataset": currentDataset.dataset.id,
      },
    }).then((response) => {
      setFetchingGroups(false);
      if (response.ok) {
        setEditing(false);
        const searchParams = new URLSearchParams(window.location.search);
        searchParams.set("edit", "false");
        navigate(`?${searchParams.toString()}`);
      }
    });
  };

  const fetchRecommendations = (
    ids: string[],
    prevRecommendations: ScoreChunkDTO[],
    prevGroupRecommendations: GroupScoreChunkDTO[],
    groupRecommendations: boolean,
  ) => {
    const currentDataset = $dataset?.();
    if (!currentDataset) return;

    setLoadingRecommendations(true);

    let apiPath = `/chunk/recommend`;
    let reqPayload: any = {
      positive_chunk_ids: ids,
      limit: prevRecommendations.length + 10,
    };

    if (groupRecommendations) {
      apiPath = `/chunk_group/recommend`;

      reqPayload = {
        positive_group_ids: [props.groupID],
        limit: prevRecommendations.length + 10,
        group_size: 3,
      };

      setRecommendedChunks([]);
    } else {
      setGroupRecommendedChunks([]);
    }

    void fetch(`${apiHost}${apiPath}`, {
      method: "POST",
      credentials: "include",
      headers: {
        "X-API-version": "2.0",
        "Content-Type": "application/json",
        "TR-Dataset": currentDataset.dataset.id,
      },
      body: JSON.stringify(reqPayload),
    }).then((response) => {
      if (response.ok) {
        void response.json().then((data) => {
          if (groupRecommendations) {
            const typedData = data.results as GroupScoreChunkDTO[];
            const dedupedData = typedData.filter((d) => {
              return !prevGroupRecommendations.some(
                (c) => c.group.id == d.group.id,
              );
            });
            const newRecommendations = [
              ...prevGroupRecommendations,
              ...dedupedData,
            ];
            setGroupRecommendedChunks(newRecommendations);
          } else {
            const typedData = data.chunks as ScoreChunkDTO[];
            const dedupedData = typedData.filter((d) => {
              return !prevRecommendations.some((c) => c.chunk.id == d.chunk.id);
            });
            const newRecommendations = [...prevRecommendations, ...dedupedData];
            setRecommendedChunks(newRecommendations);
          }
        });
      } else {
        const newEvent = new CustomEvent("show-toast", {
          detail: {
            type: "error",
            message: "Failed to fetch recommendations",
          },
        });
        window.dispatchEvent(newEvent);
      }

      setLoadingRecommendations(false);
    });
  };

  const chatPopupChunks = createMemo(() => {
    const curSearchMetadatasWithVotes = searchWithinGroupResults();
    if (curSearchMetadatasWithVotes.length > 0) {
      return curSearchMetadatasWithVotes;
    }
    const curMetadatasWithVotes = chunkMetadatas();
    return curMetadatasWithVotes.map((m) => {
      return {
        chunk: m,
        score: 0,
      } as unknown as ScoreChunkDTO;
    });
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
                chunks={chatPopupChunks}
                selectedIds={selectedIds}
                setOpenChat={setOpenChat}
              />
            </div>
          </FullScreenModal>
        </Portal>
      </Show>
      <div class="mx-auto flex w-full max-w-screen-2xl flex-col items-center space-y-2 px-4">
        <div class="flex w-full items-center justify-end space-x-2">
          <Show when={groupInfo()?.file_id ?? ""}>
            <button
              title="Download uploaded file"
              class="h-fit text-neutral-400 dark:text-neutral-300"
              onClick={() => {
                handleDownloadFile(groupInfo());
              }}
            >
              <FaSolidDownload />
            </button>
          </Show>
          <button
            classList={{
              "h-fit text-red-700 dark:text-red-400": true,
              "animate-pulse": deleting(),
            }}
            onClick={() => setShowConfirmGroupDeleteModal(true)}
          >
            <FiTrash class="h-5 w-5" />
          </button>
          <button onClick={() => setEditing((prev) => !prev)}>
            <FiEdit class="h-5 w-5" />
          </button>
        </div>
        <Show when={!editing()}>
          <Show when={groupInfo()?.name}>
            <div class="flex space-x-2">
              <span class="font-semibold text-neutral-800 dark:text-neutral-200">
                Name:{" "}
              </span>
              <span class="line-clamp-1 break-all">{groupInfo()?.name}</span>
            </div>
          </Show>
          <Show when={groupInfo()?.tracking_id}>
            <div class="flex space-x-2">
              <span class="font-semibold text-neutral-800 dark:text-neutral-200">
                Tracking ID:{" "}
              </span>
              <span class="line-clamp-1 break-all">
                {groupInfo()?.tracking_id}
              </span>
            </div>
          </Show>
          <Show when={groupInfo()?.tag_set?.filter((tag) => tag).length}>
            <div class="flex space-x-2">
              <span class="font-semibold text-neutral-800 dark:text-neutral-200">
                Tag Set:{" "}
              </span>
              <span class="line-clamp-1 break-all">
                {groupInfo()?.tag_set?.join(",")}
              </span>
            </div>
          </Show>
          <Show when={groupInfo()?.description}>
            <div class="flex gap-x-2">
              <span class="font-semibold text-neutral-800 dark:text-neutral-200">
                Description:{" "}
              </span>
              <span class="line-clamp-1 break-all">
                {groupInfo()?.description}
              </span>
            </div>
          </Show>
          <Show when={Object.keys(groupInfo()?.metadata ?? {}).length > 0}>
            <button
              class="mt-2 flex w-fit items-center space-x-1 rounded-md border bg-neutral-200/50 px-2 py-1 font-semibold text-magenta-500 hover:bg-neutral-200/90 dark:bg-neutral-700/60 dark:text-magenta-400"
              onClick={() => setExpandGroupMetadata((prev) => !prev)}
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
              <For each={Object.keys(groupInfo()?.metadata ?? {})}>
                {(key) => (
                  <Show
                    when={
                      // eslint-disable-next-line @typescript-eslint/no-explicit-any
                      (groupInfo()?.metadata as any)[key] !== undefined
                    }
                  >
                    <div class="mb-4">
                      <div class="flex space-x-2">
                        <span class="font-semibold text-neutral-800 dark:text-neutral-200">
                          {key}:{" "}
                        </span>
                        <span class="line-clamp-1 break-all">
                          {groupInfo()?.metadata &&
                            renderMetadataElements(
                              (groupInfo()?.metadata as any)[key],
                            )}
                        </span>
                      </div>
                    </div>
                  </Show>
                )}
              </For>
            </div>
          </Show>
        </Show>
        <Show when={editing()}>
          <div class="vertical-align-left mt-8 grid w-full max-w-6xl auto-rows-max grid-cols-[1fr,3fr] gap-y-2">
            <h1 class="text-md min-[320px]:text-md sm:text-md mt-10 text-left font-bold">
              Name:
            </h1>
            <input
              type="text"
              class="mt-10 max-h-fit w-full rounded-md bg-neutral-200 px-2 py-1 dark:bg-neutral-700"
              value={groupInfo()?.name}
              onInput={(e) => {
                const curGroupInfo = groupInfo();
                if (curGroupInfo) {
                  setGroupInfo({
                    ...curGroupInfo,
                    name: e.target.value,
                  });
                }
              }}
            />
            <h1 class="text-md min-[320px]:text-md sm:text-md text-left font-bold">
              Tracking ID:
            </h1>
            <input
              type="text"
              class="max-h-fit w-full rounded-md bg-neutral-200 px-2 py-1 dark:bg-neutral-700"
              value={groupInfo()?.tracking_id ?? ""}
              onInput={(e) => {
                const curGroupInfo = groupInfo();
                if (curGroupInfo) {
                  setGroupInfo({
                    ...curGroupInfo,
                    tracking_id: e.target.value,
                  });
                }
              }}
            />
            <h1 class="text-md min-[320px]:text-md sm:text-md text-left font-bold">
              Tag Set (Comma Separated):
            </h1>
            <input
              type="text"
              class="max-h-fit w-full rounded-md bg-neutral-200 px-2 py-1 dark:bg-neutral-700"
              value={groupInfo()?.tag_set?.join(",") ?? ""}
              onInput={(e) => {
                const curGroupInfo = groupInfo();
                if (curGroupInfo) {
                  setGroupInfo({
                    ...curGroupInfo,
                    tag_set: e.target.value
                      .split(",")
                      .map((t) => t.trim())
                      .filter((t) => t.length > 0),
                  });
                }
              }}
            />
            <h1 class="text-md min-[320px]:text-md sm:text-md text-left font-bold">
              Descend Tag Set Update to Chunks:
            </h1>
            <input
              class="mt-1 h-4 w-4"
              type="checkbox"
              checked={descendTagSet()}
              onChange={(e) => {
                setDescendTagSet(e.target.checked);
              }}
            />
            <h1 class="text-md min-[320px]:text-md sm:text-md text-left font-bold">
              Description:
            </h1>
            <textarea
              class="max-md w-full justify-start rounded-md bg-neutral-200 px-2 py-1 dark:bg-neutral-700"
              value={groupInfo()?.description}
              onInput={(e) => {
                const curGroupInfo = groupInfo();
                if (curGroupInfo) {
                  setGroupInfo({
                    ...curGroupInfo,
                    description: e.target.value,
                  });
                }
              }}
            />
            <h1 class="text-md min-[320px]:text-md sm:text-md text-left font-bold">
              Metadata:
            </h1>
            <JsonInput
              onValueChange={(j) => {
                const curGroupInfo = groupInfo();
                if (curGroupInfo) {
                  setGroupInfo({
                    ...curGroupInfo,
                    metadata: j,
                  });
                }
              }}
              value={() => groupInfo()?.metadata ?? {}}
            />
          </div>
          <div class="mt-4 flex w-full max-w-screen-2xl justify-end gap-x-2">
            <button
              classList={{
                "flex space-x-2 rounded bg-neutral-500 p-2 text-white": true,
                "animate-pulse": fetchingGroups(),
              }}
              onClick={() => {
                setEditing(false);
                const searchParams = new URLSearchParams(
                  window.location.search,
                );
                searchParams.set("edit", "false");
                navigate(`?${searchParams.toString()}`);
              }}
            >
              Cancel
            </button>
            <button
              classList={{
                "rounded bg-magenta-500 p-2 text-white": true,
                "animate-pulse": fetchingGroups(),
              }}
              onClick={() => updateGroup()}
            >
              Save
            </button>
          </div>
        </Show>
        <div class="flex w-full max-w-screen-2xl flex-col space-y-4 border-t border-neutral-500">
          <div class="mx-auto w-full">
            <div class="mx-auto my-4 w-full">
              <SearchForm
                search={search}
                groupID={props.groupID}
                openRateQueryModal={setOpenRateQueryModal}
              />
            </div>
          </div>
          <Switch>
            <Match when={searchLoading()}>
              <div class="animate-pulse text-center text-2xl font-bold">
                Loading...
              </div>
            </Match>
            <Match when={!searchLoading()}>
              <For
                each={
                  search.state.query == ""
                    ? chunkMetadatas()
                    : searchWithinGroupResults()
                }
              >
                {(chunk) => (
                  <div class="mt-4">
                    <ScoreChunk
                      allGroupsList={allGroupsList()}
                      totalGroupPages={totalGroupPages()}
                      chunk={!isScoreChunkDTO(chunk) ? chunk : chunk.chunk}
                      score={isScoreChunkDTO(chunk) ? chunk.score : 0}
                      group={true}
                      chunkGroups={chunkGroups()}
                      bookmarks={bookmarks()}
                      setOnDelete={setOnDelete}
                      setShowConfirmModal={setShowConfirmDeleteModal}
                      showExpand={clientSideRequestFinished()}
                      defaultShowMetadata={search.state.slimChunks}
                      setChunkGroups={setChunkGroups}
                      setSelectedIds={setSelectedIds}
                      selectedIds={selectedIds}
                    />
                  </div>
                )}
              </For>
            </Match>
          </Switch>
          <div class="mx-auto my-12 flex items-center justify-center space-x-2">
            <PaginationController
              setPage={setPage}
              page={page()}
              totalPages={totalPages()}
            />
          </div>
          <Show when={recommendedChunks().length > 0}>
            <div class="mx-auto mt-8 w-full">
              <div class="flex w-full flex-col items-center rounded-md p-2">
                <div class="text-xl font-semibold">Related Chunks</div>
              </div>
              <For each={recommendedChunks()}>
                {(chunk, i) => (
                  <>
                    <div class="mt-4">
                      <ChunkMetadataDisplay
                        allGroupsList={allGroupsList()}
                        totalGroupPages={totalGroupPages()}
                        chunk={chunk.chunk}
                        chunkGroups={chunkGroups()}
                        bookmarks={[]}
                        setShowConfirmModal={setShowConfirmDeleteModal}
                        fetchChunkGroups={fetchChunkGroups}
                        setChunkGroups={setChunkGroups}
                        setOnDelete={setOnDelete}
                        showExpand={true}
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
                  </>
                )}
              </For>
            </div>
          </Show>
          <Show when={groupRecommendedChunks().length > 0}>
            <div class="mx-auto mt-8 w-full">
              <div class="flex w-full flex-col items-center rounded-md p-2">
                <div class="text-xl font-semibold">Related Chunks</div>
              </div>
              <For each={groupRecommendedChunks()}>
                {(groupResult) => {
                  const [groupExpanded, setGroupExpanded] = createSignal(false);

                  const toggle = () => {
                    setGroupExpanded(!groupExpanded());
                  };

                  return (
                    <div class="flex w-full max-w-screen-2xl flex-col space-y-4 px-4">
                      <div
                        onClick={toggle}
                        classList={{
                          "flex items-center space-x-4 rounded bg-neutral-100 py-4 dark:bg-neutral-800 px-4 mt-4":
                            true,
                          "-mb-2": groupExpanded(),
                        }}
                      >
                        <Show when={groupExpanded()}>
                          <FaSolidChevronUp />
                        </Show>
                        <Show when={!groupExpanded()}>
                          <FaSolidChevronDown />
                        </Show>
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
                              <div class="flex space-x-2">
                                <span class="font-semibold text-neutral-800 dark:text-neutral-200">
                                  Name:{" "}
                                </span>
                                <span class="line-clamp-1 break-all">
                                  {groupResult.group.name}
                                </span>
                              </div>
                            </Show>
                          </div>
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
                      <Show when={groupExpanded()}>
                        <For each={groupResult.chunks}>
                          {(chunk, i) => (
                            <div class="ml-5 flex space-y-4">
                              <ScoreChunk
                                allGroupsList={allGroupsList()}
                                totalGroupPages={totalGroupPages()}
                                chunkGroups={chunkGroups()}
                                chunk={chunk.chunk}
                                score={chunk.score}
                                bookmarks={bookmarks()}
                                setOnDelete={setOnDelete}
                                setShowConfirmModal={setShowConfirmDeleteModal}
                                showExpand={clientSideRequestFinished()}
                                setChunkGroups={setChunkGroups}
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
            </div>
          </Show>
          <Show when={chunkMetadatas().length > 0}>
            <div class="mx-auto mt-8 flex w-full space-x-2">
              <button
                classList={{
                  "w-full rounded  bg-neutral-100 p-2 text-center hover:bg-neutral-100 dark:bg-neutral-700 dark:hover:bg-neutral-800":
                    true,
                  "animate-pulse": loadingRecommendations(),
                }}
                onClick={() =>
                  fetchRecommendations(
                    chunkMetadatas().map((m) => m.id),
                    recommendedChunks(),
                    groupRecommendedChunks(),
                    groupRecommendations(),
                  )
                }
              >
                {recommendedChunks().length == 0 ? "Get" : "Get More"} Related
                {groupRecommendations() ? " Groups" : " Chunks"}
              </button>
              <div class="flex items-center space-x-2 justify-self-center">
                <label class="text-sm">Groups</label>
                <input
                  class="h-4 w-4"
                  type="checkbox"
                  checked={groupRecommendations()}
                  onChange={(e) => {
                    if (e.target.checked) {
                      setGroupRecommendations(true);
                    } else {
                      setGroupRecommendations(false);
                    }
                  }}
                />
              </div>
            </div>
          </Show>
          <Show
            when={
              chunkMetadatas().length == 0 &&
              searchWithinGroupResults().length == 0 &&
              clientSideRequestFinished()
            }
          >
            <div class="flex w-full flex-col items-center rounded-md p-2">
              <div class="text-xl font-semibold">
                This group is currently empty
              </div>
            </div>
          </Show>
        </div>
      </div>
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
                const searchResults = searchWithinGroupResults();
                if (searchResults.length > 0) {
                  setSelectedIds(
                    searchResults
                      .flatMap((c) => {
                        return c.chunk.id;
                      })
                      .slice(0, 10),
                  );
                  setOpenChat(true);
                } else {
                  setSelectedIds(
                    chunkMetadatas()
                      .flatMap((c) => {
                        return c.id ?? "";
                      })
                      .slice(0, 10),
                  );
                }
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
            <AiOutlineRobot class="h-7 w-7" />
            <span class="sr-only">Open actions menu</span>
          </button>
        </div>
      </div>
      <ConfirmModal
        showConfirmModal={showConfirmDeleteModal}
        setShowConfirmModal={setShowConfirmDeleteModal}
        onConfirm={onDelete}
        message="Are you sure you want to delete this chunk?"
      />
      <FullScreenModal
        isOpen={openRateQueryModal}
        setIsOpen={setOpenRateQueryModal}
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
                sendRating();
                setOpenRateQueryModal(false);
              }}
            >
              Submit Rating
            </button>
          </div>
        </div>
      </FullScreenModal>
      <Show when={showConfirmGroupDeleteModal()}>
        <FullScreenModal
          isOpen={showConfirmGroupDeleteModal}
          setIsOpen={setShowConfirmGroupDeleteModal}
        >
          <div class="min-w-[250px] sm:min-w-[300px]">
            <BiRegularXCircle class="mx-auto h-8 w-8 fill-current !text-red-500" />
            <div class="mb-4 text-center text-xl font-bold text-current dark:text-white">
              {"Are you sure you want to delete this group?"}
            </div>
            <div class="flex items-center space-x-2 justify-self-center text-current dark:text-white">
              <label class="text-sm">Delete chunks</label>
              <input
                class="h-4 w-4"
                type="checkbox"
                checked={deleteChunksInGroup()}
                onChange={(e) => {
                  setDeleteChunksInGroup(e.target.checked);
                }}
              />
            </div>
            <div class="mx-auto mt-4 flex w-fit space-x-3">
              <button
                class="flex items-center space-x-2 rounded-md bg-magenta-500 p-2 text-white"
                onClick={() => {
                  setShowConfirmGroupDeleteModal(false);
                  const onGroupDelFunc = onGroupDelete();
                  onGroupDelFunc(deleteChunksInGroup());
                }}
              >
                Delete
                <FiTrash class="h-5 w-5" />
              </button>
              <button
                class="flex space-x-2 rounded-md bg-neutral-500 p-2 text-white"
                onClick={() => setShowConfirmGroupDeleteModal(false)}
              >
                Cancel
              </button>
            </div>
          </div>
        </FullScreenModal>
      </Show>
    </>
  );
};
