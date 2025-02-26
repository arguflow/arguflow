/* eslint-disable @typescript-eslint/no-unsafe-return */
/* eslint-disable @typescript-eslint/no-unsafe-argument */
/* eslint-disable @typescript-eslint/no-unsafe-call */
/* eslint-disable @typescript-eslint/no-unsafe-assignment */
/* eslint-disable @typescript-eslint/no-unsafe-member-access */

import {
  Accessor,
  For,
  Setter,
  Show,
  Switch,
  createEffect,
  createMemo,
  createSignal,
  Match,
  useContext,
} from "solid-js";
import {
  ChunkBookmarksDTO,
  ChunkMetadata,
  indirectHasOwnProperty,
  type ChunkGroupDTO,
} from "../utils/apiTypes";
import { BiRegularChevronDown, BiRegularChevronUp } from "solid-icons/bi";
import BookmarkPopover from "./BookmarkPopover";
import { FiChevronDown, FiChevronUp, FiEye } from "solid-icons/fi";
import sanitizeHtml from "sanitize-html";
import { FiEdit, FiTrash } from "solid-icons/fi";
import { Tooltip } from "shared/ui";
import { FullScreenModal } from "./Atoms/FullScreenModal";
import { A, useLocation } from "@solidjs/router";
import { DatasetAndUserContext } from "./Contexts/DatasetAndUserContext";
import { CTRPopup } from "./CTRPopup";

export const sanitzerOptions = {
  allowedTags: [...sanitizeHtml.defaults.allowedTags, "font", "button", "span"],
  allowedAttributes: {
    ...sanitizeHtml.defaults.allowedAttributes,
    button: ["onclick"],
  },
};

export const formatDate = (date: Date) => {
  const month = date.getMonth() + 1;
  const day = date.getDate();
  const year = date.getFullYear();

  const formattedMonth = month < 10 ? `0${month}` : month;
  const formattedDay = day < 10 ? `0${day}` : day;

  return `${formattedMonth}/${formattedDay}/${year}`;
};

export interface ScoreChunkProps {
  chunkGroups?: ChunkGroupDTO[];
  totalGroupPages?: number;
  group?: boolean;
  chunk: ChunkMetadata;
  score: number;
  setShowModal?: Setter<boolean>;
  setOnDelete?: Setter<() => void>;
  setShowConfirmModal?: Setter<boolean>;
  initialExpanded?: boolean;
  bookmarks?: ChunkBookmarksDTO[];
  showExpand?: boolean;
  defaultShowMetadata?: boolean;
  setChunkGroups?: Setter<ChunkGroupDTO[]>;
  allGroupsList: ChunkGroupDTO[];
  counter?: string;
  order?: string;
  setSelectedIds: Setter<string[]>;
  selectedIds: Accessor<string[]>;
  chat?: boolean;
  registerClickForChunk?: ({
    eventType,
    id,
  }: {
    eventType: string;
    id: string;
  }) => Promise<void>;
}

const ScoreChunk = (props: ScoreChunkProps) => {
  const datasetAndUserContext = useContext(DatasetAndUserContext);
  const $currentDataset = datasetAndUserContext.currentDataset;
  const $currentUser = datasetAndUserContext.user;
  const apiHost = import.meta.env.VITE_API_HOST as string;

  const [expanded, setExpanded] = createSignal(props.initialExpanded ?? false);
  const [showPropsModal, setShowPropsModal] = createSignal(false);
  const [deleting, setDeleting] = createSignal(false);
  const [deleted, setDeleted] = createSignal(false);
  const [showMetadata, setShowMetadata] = createSignal(false);
  const [expandMetadata, setExpandMetadata] = createSignal(
    props.defaultShowMetadata ?? false,
  );
  const [showImages, setShowImages] = createSignal(true);
  const [imageLinks, setImageLinks] = createSignal<string[] | null>(null);

  createEffect(() => {
    if (
      !props.chunk.metadata ||
      !indirectHasOwnProperty(props.chunk, "image_urls")
    ) {
      return null;
    }

    setImageLinks(props.chunk.image_urls);
  });

  createEffect(() => {
    if (!showPropsModal() || !props.setShowModal) return;

    props.setShowModal(true);
    setShowPropsModal(false);
  });

  const location = useLocation();
  const isInChunkViewer = createMemo(() => {
    return location.pathname.startsWith("/chunk/");
  });

  const deleteChunk = () => {
    if (!props.setOnDelete) return;
    const dataset = $currentDataset?.();
    if (!dataset) return;

    const curChunkMetadataId = props.chunk.id;

    props.setOnDelete(() => {
      return () => {
        setDeleting(true);
        void fetch(`${apiHost}/chunk/${curChunkMetadataId}`, {
          method: "DELETE",
          headers: {
            "X-API-version": "2.0",
            "TR-Dataset": dataset.dataset.id,
          },
          credentials: "include",
        }).then((response) => {
          setDeleting(false);
          if (response.ok) {
            setDeleted(true);
            return;
          }
          alert("Failed to delete chunk");
        });
      };
    });

    props.setShowConfirmModal?.(true);
  };

  const useExpand = createMemo(() => {
    if (!props.chunk.chunk_html) return false;
    return props.chunk.chunk_html.split(" ").length > 20 * 15;
  });

  const currentUserRole = createMemo(() => {
    const curUser = $currentUser?.();
    const curDatasetOrg = $currentDataset?.()?.dataset.organization_id;
    // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
    const curUserOrg = curUser?.user_orgs?.find(
      (org) => org.organization_id === curDatasetOrg,
    );
    return curUserOrg?.role ?? 0;
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
      <Show when={!deleted()}>
        <div
          class="mx-auto flex w-full flex-col items-center rounded-md bg-neutral-100 p-2 dark:!bg-neutral-800"
          id={
            "doc_" +
            (props.chat ? (props.order ?? "") + props.counter : props.chunk.id)
          }
        >
          <div class="flex w-full flex-col space-y-2">
            <div class="flex h-fit items-center space-x-1">
              <Show when={!props.chat}>
                <input
                  id="default-checkbox"
                  type="checkbox"
                  onClick={() => {
                    const chunkId = props.chunk.id;
                    props.setSelectedIds((prev) => {
                      if (prev.includes(chunkId)) {
                        return prev.filter((id) => id !== chunkId);
                      }
                      return [...prev, chunkId];
                    });
                  }}
                  checked={props.selectedIds().includes(props.chunk.id)}
                  class="h-4 w-4 rounded border-gray-300 bg-gray-100 text-green-500 focus:ring-2 focus:ring-blue-500 dark:border-gray-600 dark:bg-gray-700 dark:ring-offset-gray-800 dark:focus:ring-blue-600"
                />
              </Show>
              <Show when={props.chat}>
                <span class="font-semibold">
                  Doc: {props.counter?.toString()}
                </span>
              </Show>
              <div class="flex-1" />

              <Show when={props.registerClickForChunk}>
                {(registerClickForChunk) => (
                  <CTRPopup
                    onSubmit={(eventType: string) =>
                      void registerClickForChunk()({
                        eventType,
                        id: props.chunk.id,
                      })
                    }
                  />
                )}
              </Show>
              <Show when={!isInChunkViewer()}>
                <Tooltip
                  body={
                    <A
                      href={`/chunk/${
                        props.chunk.id
                      }?dataset=${$currentDataset?.()?.dataset.id}`}
                    >
                      <FiEye class="h-5 w-5" />
                    </A>
                  }
                  tooltipText="Open chunk to test recommendations for similar chunks"
                />
              </Show>
              <Show when={currentUserRole() > 0}>
                <Tooltip
                  body={
                    <A
                      href={`/chunk/edit/${
                        props.chunk.id
                      }?dataset=${$currentDataset?.()?.dataset.id}`}
                    >
                      <FiEdit class="h-5 w-5" />
                    </A>
                  }
                  tooltipText="Edit chunk"
                />
              </Show>
              <Show when={currentUserRole() > 0}>
                <Tooltip
                  body={
                    <button
                      classList={{
                        "h-fit text-red-700 dark:text-red-400 cursor-pointer":
                          true,
                        "animate-pulse": deleting(),
                      }}
                      title="Delete"
                      onClick={() => deleteChunk()}
                    >
                      <FiTrash class="h-5 w-5" />
                    </button>
                  }
                  tooltipText="Delete chunk"
                />
              </Show>

              <Show when={props.chunkGroups}>
                {(chunkGroups) => (
                  <BookmarkPopover
                    totalGroupPages={props.totalGroupPages ?? 0}
                    allGroupsList={props.allGroupsList}
                    chunkGroups={chunkGroups()}
                    chunkMetadata={props.chunk}
                    setLoginModal={props.setShowModal}
                    bookmarks={[]}
                    setChunkGroups={props.setChunkGroups}
                  />
                )}
              </Show>
            </div>
            <div class="flex w-full flex-col">
              <Show when={props.chunk.link}>
                <a
                  class="line-clamp-1 w-fit break-all text-magenta-500 underline dark:text-turquoise-400"
                  target="_blank"
                  href={props.chunk.link ?? ""}
                >
                  {props.chunk.link}
                </a>
              </Show>
              <div class="grid w-fit auto-cols-min grid-cols-[1fr,3fr] gap-x-2">
                <Show when={props.score != 0}>
                  <span class="font-semibold">Score: </span>
                  <span>{props.score.toPrecision(3)}</span>
                </Show>
              </div>
              <div class="flex space-x-2">
                <span class="whitespace-nowrap text-nowrap font-semibold text-neutral-800 dark:text-neutral-200">
                  ID:{" "}
                </span>
                <span class="line-clamp-1 break-all">{props.chunk.id}</span>
              </div>
              <Show when={props.chunk.tracking_id}>
                <div class="flex space-x-2">
                  <span class="whitespace-nowrap text-nowrap font-semibold text-neutral-800 dark:text-neutral-200">
                    Tracking ID:{" "}
                  </span>
                  <span class="line-clamp-1 break-all">
                    {props.chunk.tracking_id}
                  </span>
                </div>
              </Show>
              <Show
                when={
                  props.chunk.tag_set &&
                  props.chunk.tag_set?.filter((tag) => tag).length
                }
              >
                <div class="flex space-x-2">
                  <span class="text-nowrap font-semibold text-neutral-800 dark:text-neutral-200">
                    Tag Set:{" "}
                  </span>
                  <span class="line-clamp-1 break-all">
                    {props.chunk.tag_set?.join(",")}
                  </span>
                </div>
              </Show>
              <Show when={props.chunk.time_stamp}>
                <div class="flex space-x-2">
                  <span class="text-nowrap font-semibold text-neutral-800 dark:text-neutral-200">
                    Time Stamp:{" "}
                  </span>
                  <span class="line-clamp-1 break-all">
                    {formatDate(new Date(props.chunk.time_stamp ?? ""))}
                  </span>
                </div>
              </Show>
              <Show when={props.chunk.num_value}>
                <div class="flex gap-x-2">
                  <span class="text-nowrap font-semibold text-neutral-800 dark:text-neutral-200">
                    Num Value:{" "}
                  </span>
                  <span class="line-clamp-1 break-all">
                    {props.chunk.num_value}
                  </span>
                </div>
              </Show>
              <Show
                when={
                  props.chunk.location &&
                  props.chunk.location.lat &&
                  props.chunk.location.lon
                }
              >
                <div class="flex space-x-2">
                  <span class="font-semibold text-neutral-800 dark:text-neutral-200">
                    Location:{" "}
                  </span>
                  <span class="line-clamp-1 break-all">
                    [{props.chunk.location?.lat}, {props.chunk.location?.lon}]
                  </span>
                </div>
              </Show>
              <Show when={imageLinks() != null}>
                <button
                  class="mt-2 flex w-fit items-center space-x-1 rounded-md border bg-neutral-200/50 px-2 py-1 font-semibold text-magenta-500 hover:bg-neutral-200/90 dark:bg-neutral-700/60 dark:text-magenta-400"
                  onClick={() => setShowImages((prev) => !prev)}
                >
                  <Switch>
                    <Match when={showImages()}>
                      Collapse Images <FiChevronUp class="h-5 w-5" />
                    </Match>
                    <Match when={!showImages()}>
                      Expand Images <FiChevronDown class="h-5 w-5" />
                    </Match>
                  </Switch>
                </button>
                <Show when={showImages()}>
                  <div class="my-2 flex space-x-2 overflow-x-auto rounded-md pl-2">
                    <For each={imageLinks() ?? []}>
                      {(link) => (
                        <img
                          class="w-40 rounded-md"
                          src={link ?? ""}
                          alt={link}
                        />
                      )}
                    </For>
                  </div>
                </Show>
              </Show>
              <Show when={Object.keys(props.chunk.metadata ?? {}).length > 0}>
                <button
                  class="mt-2 flex w-fit items-center space-x-1 rounded-md border bg-neutral-200/50 px-2 py-1 font-semibold text-magenta-500 hover:bg-neutral-200/90 dark:bg-neutral-700/60 dark:text-magenta-400"
                  onClick={() => setExpandMetadata((prev) => !prev)}
                >
                  <span>
                    {expandMetadata() ? "Collapse Metadata" : "Expand Metadata"}
                  </span>
                  <Switch>
                    <Match when={expandMetadata()}>
                      <BiRegularChevronUp class="h-5 w-5 fill-current" />
                    </Match>
                    <Match when={!expandMetadata()}>
                      <BiRegularChevronDown class="h-5 w-5 fill-current" />
                    </Match>
                  </Switch>
                </button>
              </Show>
              <Show when={expandMetadata()}>
                <div class="pl-2 pt-2">
                  <For each={Object.keys(props.chunk.metadata ?? {})}>
                    {(key) => (
                      <Show
                        when={
                          // eslint-disable-next-line @typescript-eslint/no-explicit-any
                          (props.chunk.metadata as any)[key] !== undefined
                        }
                      >
                        <div class="mb-4">
                          <div class="flex space-x-2">
                            <span class="font-semibold text-neutral-800 dark:text-neutral-200">
                              {key}:{" "}
                            </span>
                            <span class="line-clamp-1 break-all">
                              {props.chunk.metadata &&
                                renderMetadataElements(
                                  props.chunk.metadata[key],
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
          </div>
          <div class="mb-1 mt-2 h-1 w-full border-b border-neutral-300 dark:border-neutral-600" />
          <div
            id="score-chunk-html"
            classList={{
              "line-clamp-4 gradient-mask-b-0": useExpand() && !expanded(),
              "text-ellipsis max-w-[100%] w-full break-words space-y-5 leading-normal !text-black dark:!text-white":
                true,
            }}
            style={
              useExpand() && !expanded() ? { "-webkit-line-clamp": 15 } : {}
            }
            // eslint-disable-next-line solid/no-innerhtml
            innerHTML={sanitizeHtml(
              props.chunk.chunk_html !== undefined
                ? props.chunk.chunk_html.replaceAll("\n", "<br>")
                : "",
              sanitzerOptions,
            )}
          />
          <Show when={useExpand()}>
            <button
              classList={{
                "ml-2 font-semibold": true,
                "animate-pulse": !props.showExpand,
              }}
              disabled={!props.showExpand}
              onClick={() => setExpanded((prev) => !prev)}
            >
              {expanded() ? (
                <div class="flex flex-row items-center">
                  <div>Show Less</div>{" "}
                  <BiRegularChevronUp class="h-8 w-8 fill-current" />
                </div>
              ) : (
                <div class="flex flex-row items-center">
                  <div>Show More</div>{" "}
                  <BiRegularChevronDown class="h-8 w-8 fill-current" />
                </div>
              )}
            </button>
          </Show>
        </div>
      </Show>
      <Show when={showMetadata()}>
        <FullScreenModal isOpen={showMetadata} setIsOpen={setShowMetadata}>
          <div class="flex max-h-[60vh] max-w-[75vw] flex-col space-y-2 overflow-auto scrollbar-thin scrollbar-track-neutral-200 scrollbar-thumb-neutral-400 scrollbar-thumb-rounded-md dark:text-white dark:scrollbar-track-neutral-800 dark:scrollbar-thumb-neutral-600">
            <For each={Object.keys(props.chunk.metadata ?? {})}>
              {(metadataKey) => (
                <div class="flex flex-wrap space-x-2">
                  <span>{`"${metadataKey}":`}</span>
                  <span>{`"${
                    // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-explicit-any, @typescript-eslint/restrict-template-expressions
                    typeof (props.chunk.metadata as any)[metadataKey] ===
                    "object"
                      ? JSON.stringify(
                          // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-explicit-any
                          (props.chunk.metadata as any)[metadataKey],
                        )
                      : // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-explicit-any
                        (props.chunk.metadata as any)[metadataKey]
                  }"`}</span>
                </div>
              )}
            </For>
          </div>
        </FullScreenModal>
      </Show>
    </>
  );
};

export default ScoreChunk;
