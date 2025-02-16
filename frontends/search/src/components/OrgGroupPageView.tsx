import { FiTrash } from "solid-icons/fi";
import {
  indirectHasOwnProperty,
  isChunkGroupPageDTO,
  type ChunkGroupDTO,
} from "../utils/apiTypes";
import {
  For,
  Setter,
  Show,
  createEffect,
  createSignal,
  createMemo,
  useContext,
} from "solid-js";
import {
  BiRegularChevronLeft,
  BiRegularChevronRight,
  BiRegularX,
} from "solid-icons/bi";
import { getLocalTime } from "./ChunkMetadataDisplay";
import { DatasetAndUserContext } from "./Contexts/DatasetAndUserContext";
import { useDatasetServerConfig } from "../hooks/useDatasetServerConfig";
import { downloadFile } from "../utils/downloadFile";
import { FaSolidDownload } from "solid-icons/fa";

export interface GroupUserPageViewProps {
  setOnDelete: Setter<(delete_chunks: boolean) => void>;
  setShowConfirmModal: Setter<boolean>;
}

export type GetChunkGroupCountResponse = {
  count: number;
  group_id: string;
};

export const GroupUserPageView = (props: GroupUserPageViewProps) => {
  const apiHost = import.meta.env.VITE_API_HOST as string;
  const datasetAndUserContext = useContext(DatasetAndUserContext);

  const $dataset = datasetAndUserContext.currentDataset;
  const $user = datasetAndUserContext.user;
  const [groups, setGroups] = createSignal<ChunkGroupDTO[]>([]);
  const [allGroups, setAllGroups] = createSignal<ChunkGroupDTO[]>([]);
  const [groupCounts, setGroupCounts] = createSignal<
    GetChunkGroupCountResponse[]
  >([]);
  const [groupPage, setGroupPage] = createSignal(1);
  const [groupPageCount, setGroupPageCount] = createSignal(1);
  const [deleting, setDeleting] = createSignal(false);
  const [loading, setLoading] = createSignal(true);
  const [searchQuery, setSearchQuery] = createSignal("");
  const [searchResults, setSearchResults] = createSignal<ChunkGroupDTO[]>([]);

  const groupsList = createMemo(() => allGroups());

  const serverConfig = useDatasetServerConfig();

  createEffect(() => {
    const currentDataset = $dataset?.();
    if (!currentDataset) return;

    const all_counts = groups().map(async (group) => {
      const response = await fetch(`${apiHost}/chunk_group/count`, {
        method: "POST",
        credentials: "include",
        body: JSON.stringify({ group_id: group.id }),
        headers: {
          "X-API-version": "2.0",
          "TR-Dataset": currentDataset.dataset.id,
          "Content-Type": "application/json",
        },
      });

      if (response.ok) {
        // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
        const data = await response.json();
        console.log("data", data);
        if (
          data !== null &&
          typeof data === "object" &&
          indirectHasOwnProperty(data, "count") &&
          // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
          typeof data.count === "number" &&
          indirectHasOwnProperty(data, "group_id") &&
          // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
          typeof data.group_id === "string"
        ) {
          console.log("Invalid response", data);
          return {
            // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-member-access
            group_id: data.group_id,
            // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-member-access
            count: data.count,
          } as GetChunkGroupCountResponse;
        }
      }
    });

    void Promise.all(all_counts).then((counts) => {
      const filteredGroupCounts = counts.filter((c) => c !== undefined);
      console.log("setGroupCounts", filteredGroupCounts);
      setGroupCounts(filteredGroupCounts);
    });
  });

  createEffect(() => {
    const currentDataset = $dataset?.();
    if (!currentDataset) return;

    const fetchAllGroups = async () => {
      const allGroupsArray: ChunkGroupDTO[] = [];
      let currentPage = 1;
      let hasMore = true;

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
            allGroupsArray.push(...data.groups);
            hasMore = currentPage < data.total_pages;
            currentPage++;
          } else {
            hasMore = false;
          }
        } else {
          hasMore = false;
        }
      }

      setAllGroups(allGroupsArray);
    };

    void fetchAllGroups();
  });

  createEffect(() => {
    const userId = $user?.()?.id;
    if (userId === undefined) return;

    const currentDataset = $dataset?.();
    if (!currentDataset) return;

    if (searchQuery()) return;

    setLoading(true);

    void fetch(
      `${apiHost}/dataset/groups/${currentDataset.dataset.id}/${groupPage()}`,
      {
        method: "GET",
        credentials: "include",
        headers: {
          "X-API-version": "2.0",
          "TR-Dataset": currentDataset.dataset.id,
        },
      },
    ).then((response) => {
      if (response.ok) {
        void response.json().then((data) => {
          setLoading(false);
          if (isChunkGroupPageDTO(data)) {
            setGroups(data.groups);
            setGroupPageCount(data.total_pages == 0 ? 1 : data.total_pages);
          } else {
            console.error("Invalid response", data);
          }
        });
      } else {
        setLoading(false);
      }
    });
  });

  createEffect(() => {
    const groupListOrEmpty = groupsList() ?? [];
    if (searchQuery() === "") {
      setSearchResults(groups());
    } else {
      const query = searchQuery().toLowerCase();
      const results = groupListOrEmpty.filter((item) =>
        item.name.toLowerCase().includes(query),
      );
      setSearchResults(results);
      setGroupPage(1);
      setGroupPageCount(Math.ceil(results.length / 10));
    }
  });

  const deleteGroup = (group: ChunkGroupDTO) => {
    const currentDataset = $dataset?.();
    if (!currentDataset) return;

    props.setOnDelete(() => {
      return (delete_chunks: boolean) => {
        setDeleting(true);
        void fetch(
          `${apiHost}/chunk_group/${
            group.id
          }?delete_chunks=${delete_chunks.toString()}`,
          {
            method: "DELETE",
            credentials: "include",
            headers: {
              "X-API-version": "2.0",
              "Content-Type": "application/json",
              "TR-Dataset": currentDataset.dataset.id,
            },
          },
        ).then((response) => {
          if (response.ok) {
            setDeleting(false);
            if (!delete_chunks)
              setGroups((prev) => {
                return prev.filter((c) => c.id != group.id);
              });
          }
          if (response.status == 403) {
            setDeleting(false);
          }
          if (response.status == 401) {
            setDeleting(false);
          }
        });
      };
    });

    props.setShowConfirmModal(true);
  };

  const handleDownloadFile = (group: ChunkGroupDTO) => {
    const datasetId = $dataset?.()?.dataset.id;
    if (group.file_id && datasetId) {
      void downloadFile(group.file_id, datasetId);
    }
  };

  return (
    <>
      <div class="w-full">
        <div class="flex flex-row space-x-80">
          <div class="w-full text-right text-2xl font-bold">
            {$dataset?.()?.dataset.name}'s Groups
          </div>
          <div class="flex flex-row items-center justify-center gap-1">
            <input
              placeholder="Search groups..."
              class="mb-2 flex w-max items-center justify-between rounded bg-neutral-100 p-3 px-3 text-sm text-black outline-none transition-all duration-300 hover:bg-neutral-200 dark:bg-neutral-700 dark:hover:text-white dark:focus:text-white"
              onInput={(e) => {
                const target = e.target as HTMLInputElement;
                setSearchQuery(target.value);
              }}
              value={searchQuery()}
            />
            <Show when={searchQuery()}>
              <button
                onClick={(e) => {
                  e.stopPropagation();
                  setSearchQuery("");
                }}
              >
                <BiRegularX class="mb-2 h-5 w-5 fill-current" />
              </button>
            </Show>
          </div>
        </div>

        <Show when={loading()}>
          <div class="animate-pulse text-center text-2xl font-bold">
            Loading...
          </div>
        </Show>

        <Show when={!loading() && searchResults().length === 0}>
          <div class="my-10 flex flex-row items-center justify-center text-center text-2xl font-bold">
            {searchQuery()
              ? "No matching groups found"
              : "No groups found for this dataset"}
          </div>
        </Show>

        <Show when={!loading() && searchResults().length > 0}>
          <div class="mt-2 inline-block min-w-full py-2 align-middle">
            <table class="min-w-full divide-y divide-gray-300 dark:divide-gray-700">
              <thead>
                <tr>
                  <th
                    scope="col"
                    class="py-3.5 pl-4 pr-3 text-left text-base font-semibold dark:text-white sm:pl-[18px]"
                  >
                    Name
                  </th>
                  <th
                    scope="col"
                    class="px-3 py-3.5 text-left text-base font-semibold dark:text-white"
                  >
                    Chunk Count
                  </th>
                  <th
                    scope="col"
                    class="px-3 py-3.5 text-left text-base font-semibold dark:text-white"
                  >
                    Created at
                  </th>
                  <Show when={$user?.() != undefined}>
                    <th
                      scope="col"
                      class="relative hidden py-3.5 pl-3 pr-4 sm:pr-0"
                    >
                      <span class="sr-only">Delete</span>
                    </th>
                  </Show>
                </tr>
              </thead>
              <tbody class="divide-y divide-gray-200 dark:divide-gray-800">
                <For
                  each={
                    searchQuery()
                      ? searchResults().slice(
                          (groupPage() - 1) * 10,
                          groupPage() * 10,
                        )
                      : groups()
                  }
                >
                  {(group) => (
                    <tr>
                      <td class="cursor-pointer whitespace-nowrap text-wrap py-4 pl-4 pr-3 text-sm font-semibold text-gray-900 dark:text-white">
                        <a
                          class="w-full underline"
                          href={`/group/${group.id}?dataset=${$dataset?.()
                            ?.dataset.id}`}
                        >
                          {group.name}
                        </a>
                      </td>
                      <td class="whitespace-nowrap text-wrap px-3 py-4 text-sm text-gray-900 dark:text-gray-300">
                        {
                          groupCounts().find((c) => c.group_id == group.id)
                            ?.count
                        }
                      </td>
                      <td class="whitespace-nowrap px-3 py-4 text-left text-sm text-gray-900 dark:text-gray-300">
                        {getLocalTime(group.created_at).toLocaleDateString() +
                          " " +
                          //remove seconds from time
                          getLocalTime(group.created_at)
                            .toLocaleTimeString()
                            .replace(/:\d+\s/, " ")}
                      </td>
                      <td class="relative whitespace-nowrap py-4 pl-3 pr-4 text-right text-sm font-medium sm:pr-0">
                        <div class="flex items-center gap-3">
                          <Show
                            when={
                              serverConfig()?.["DOCUMENT_DOWNLOAD_FEATURE"] !=
                                false && group.file_id
                            }
                          >
                            <button
                              title="Download uploaded file"
                              class="h-fit text-neutral-400 dark:text-neutral-300"
                              onClick={() => {
                                handleDownloadFile(group);
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
                            onClick={() => deleteGroup(group)}
                          >
                            <FiTrash class="h-5 w-5" />
                          </button>
                        </div>
                      </td>
                    </tr>
                  )}
                </For>
              </tbody>
            </table>
          </div>
          <div class="mt-4 flex items-center justify-between">
            <div />
            <div class="flex items-center">
              <div class="text-sm text-neutral-400">
                {groupPage()} / {groupPageCount()}
              </div>
              <button
                class="disabled:text-neutral-400 dark:disabled:text-neutral-500"
                disabled={groupPage() == 1}
                onClick={() => {
                  setGroupPage((prev) => prev - 1);
                }}
              >
                <BiRegularChevronLeft class="h-6 w-6 fill-current" />
              </button>
              <button
                class="disabled:text-neutral-400 dark:disabled:text-neutral-500"
                disabled={groupPage() == groupPageCount()}
                onClick={() => {
                  setGroupPage((prev) => prev + 1);
                }}
              >
                <BiRegularChevronRight class="h-6 w-6 fill-current" />
              </button>
            </div>
          </div>
        </Show>
      </div>
    </>
  );
};
