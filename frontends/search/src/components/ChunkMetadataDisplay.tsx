/* eslint-disable @typescript-eslint/no-unsafe-member-access */
/* eslint-disable @typescript-eslint/no-unsafe-call */
import {
  For,
  Setter,
  Show,
  createMemo,
  createSignal,
  Switch,
  Match,
  useContext,
  createEffect,
} from "solid-js";
import {
  indirectHasOwnProperty,
  type ChunkBookmarksDTO,
  type ChunkGroupDTO,
  ChunkMetadata,
} from "../utils/apiTypes";
import { BiRegularChevronDown, BiRegularChevronUp } from "solid-icons/bi";
import sanitizeHtml from "sanitize-html";
import { FiChevronDown, FiChevronUp, FiEye } from "solid-icons/fi";
import BookmarkPopover from "./BookmarkPopover";
import { FiEdit, FiTrash } from "solid-icons/fi";
import { formatDate, sanitzerOptions } from "./ScoreChunk";
import { FaRegularFileCode } from "solid-icons/fa";
import { FullScreenModal } from "./Atoms/FullScreenModal";
import { DatasetAndUserContext } from "./Contexts/DatasetAndUserContext";
import { createToast } from "./ShowToasts";
import { Tooltip } from "shared/ui";
import { useLocation } from "@solidjs/router";
import { CTRPopup } from "./CTRPopup";

export const getLocalTime = (strDate: string | Date) => {
  const utcDate = new Date(strDate);

  const timeZoneOffsetMinutes = new Date().getTimezoneOffset();

  const localTime = new Date(
    utcDate.getTime() - timeZoneOffsetMinutes * 60 * 1000,
  );

  return localTime;
};

export interface ChunkMetadataDisplayProps {
  totalGroupPages: number;
  signedInUserId?: string;
  viewingUserId?: string;
  chunk: ChunkMetadata;
  score?: number;
  chunkGroups: ChunkGroupDTO[];
  bookmarks: ChunkBookmarksDTO[];
  allGroupsList: ChunkGroupDTO[];
  setShowConfirmModal: Setter<boolean>;
  fetchChunkGroups: () => void;
  setChunkGroups: Setter<ChunkGroupDTO[]>;
  setOnDelete: Setter<() => void>;
  showExpand?: boolean;
  registerClickForChunk?: ({
    eventType,
    id,
  }: {
    eventType: string;
    id: string;
  }) => Promise<void>;
}

const ChunkMetadataDisplay = (props: ChunkMetadataDisplayProps) => {
  const apiHost = import.meta.env.VITE_API_HOST as string;
  const datasetAndUserContext = useContext(DatasetAndUserContext);

  const [expanded, setExpanded] = createSignal(false);
  const [deleting, setDeleting] = createSignal(false);
  const [deleted, setDeleted] = createSignal(false);
  const [showMetadata, setShowMetadata] = createSignal(false);
  const [expandMetadata, setExpandMetadata] = createSignal(false);
  const [imageLinks, setImageLinks] = createSignal<string[] | null>(null);
  const [showImages, setShowImages] = createSignal(true);

  const $currentDataset = datasetAndUserContext.currentDataset;

  const location = useLocation();
  const isInChunkViewer = createMemo(() => {
    return location.pathname.startsWith("/chunk/");
  });

  const onDelete = () => {
    if (props.signedInUserId !== props.viewingUserId) return;
    const curChunkId = props.chunk.id;

    props.setOnDelete(() => {
      return () => {
        setDeleting(true);
        void fetch(`${apiHost}/chunk/${curChunkId}`, {
          method: "DELETE",
          credentials: "include",
          headers: {
            "X-API-version": "2.0",
            "TR-Dataset": $currentDataset?.()?.dataset.id ?? "",
          },
        }).then((response) => {
          setDeleting(false);
          if (response.ok) {
            setDeleted(true);
            return;
          }
          createToast({
            type: "error",
            message: "Failed to delete the chunk",
          });
        });
      };
    });

    props.setShowConfirmModal(true);
  };

  createEffect(() => {
    if (
      !props.chunk.metadata ||
      !indirectHasOwnProperty(props.chunk, "image_urls")
    ) {
      return null;
    }

    setImageLinks(props.chunk.image_urls);
  });

  const useExpand = createMemo(() => {
    if (!props.chunk.chunk_html) return false;
    return props.chunk.chunk_html.split(" ").length > 20 * 15;
  });

  return (
    <>
      <Show when={!deleted()}>
        <div class="flex w-full flex-col items-center rounded-md bg-neutral-100 p-2 dark:bg-neutral-800">
          <div class="flex w-full flex-col space-y-2">
            <div class="flex h-fit items-center space-x-1">
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

              <Tooltip
                body={
                  <Show when={Object.keys(props.chunk.metadata ?? {}).length}>
                    <button
                      class="h-fit"
                      onClick={() => setShowMetadata(true)}
                      title="View Images"
                    >
                      <FaRegularFileCode class="h-5 w-5 fill-current" />
                    </button>
                  </Show>
                }
                tooltipText="View Full Metadata"
              />
              <Show when={props.signedInUserId == props.viewingUserId}>
                <button
                  classList={{
                    "h-fit text-red-700 dark:text-red-400": true,
                    "animate-pulse": deleting(),
                  }}
                  title="Delete"
                  onClick={() => onDelete()}
                >
                  <FiTrash class="h-5 w-5" />
                </button>
              </Show>
              <Show when={props.signedInUserId == props.viewingUserId}>
                <a
                  title="Edit"
                  href={`/chunk/edit/${props.chunk.id}?dataset=${
                    $currentDataset?.()?.dataset.id ?? ""
                  }`}
                >
                  <FiEdit class="h-5 w-5" />
                </a>
              </Show>
              <Show when={!isInChunkViewer()}>
                <Tooltip
                  body={
                    <a
                      title="Open chunk to test recommendations for similar chunks"
                      href={`/chunk/${props.chunk.id}?dataset=${
                        $currentDataset?.()?.dataset.id ?? ""
                      }`}
                    >
                      <FiEye class="h-5 w-5" />
                    </a>
                  }
                  tooltipText="Open to test recommendations for similar chunks"
                />
              </Show>

              <BookmarkPopover
                totalGroupPages={props.totalGroupPages}
                allGroupsList={props.allGroupsList}
                chunkGroups={props.chunkGroups}
                chunkMetadata={props.chunk}
                bookmarks={props.bookmarks.filter(
                  (bookmark) => bookmark.chunk_uuid == props.chunk.id,
                )}
                setChunkGroups={props.setChunkGroups}
              />
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
                <Show when={props.score}>
                  <span class="font-semibold">Score: </span>
                  <span>{props.score?.toPrecision(3)}</span>
                </Show>
              </div>
              <div class="flex space-x-2">
                <span class="font-semibold text-neutral-800 dark:text-neutral-200">
                  ID:{" "}
                </span>
                <span class="break-all">{props.chunk.id}</span>
              </div>
              <Show when={props.chunk.tracking_id}>
                <div class="flex space-x-2">
                  <span class="font-semibold text-neutral-800 dark:text-neutral-200">
                    Tracking ID:{" "}
                  </span>
                  <span class="break-all">{props.chunk.tracking_id}</span>
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
                  <span class="break-all">
                    {formatDate(new Date(props.chunk.time_stamp ?? ""))}
                  </span>
                </div>
              </Show>
              <Show when={props.chunk.num_value}>
                <div class="flex gap-x-2">
                  <span class="text-nowrap font-semibold text-neutral-800 dark:text-neutral-200">
                    Num Value:{" "}
                  </span>
                  <span class="break-all">{props.chunk.num_value}</span>
                </div>
              </Show>
              <Show
                when={
                  props.chunk.location != null &&
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
                <div class="pl-2">
                  <For each={Object.keys(props.chunk.metadata ?? {})}>
                    {(key) => (
                      <>
                        <Show
                          when={
                            // eslint-disable-next-line @typescript-eslint/no-explicit-any, @typescript-eslint/no-unsafe-assignment
                            (props.chunk.metadata as any)[key]
                          }
                        >
                          <div class="flex space-x-2">
                            <span class="font-semibold text-neutral-800 dark:text-neutral-200">
                              {key}:{" "}
                            </span>
                            <span class="line-clamp-1 break-all">
                              {props.chunk.metadata &&
                                indirectHasOwnProperty(
                                  props.chunk.metadata,
                                  key,
                                ) &&
                                // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-explicit-any, @typescript-eslint/no-unsafe-call
                                (props.chunk.metadata as any)[key]}
                            </span>
                          </div>
                        </Show>
                      </>
                    )}
                  </For>
                </div>
              </Show>
            </div>
          </div>
          <div class="mb-1 h-1 w-full border-b border-neutral-300 dark:border-neutral-600" />
          <div
            id="score-chunk-html"
            classList={{
              "line-clamp-4 gradient-mask-b-0": useExpand() && !expanded(),
              "text-ellipsis max-w-[100%] break-words space-y-5 leading-normal !text-black dark:!text-white":
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
              // eslint-disable-next-line @typescript-eslint/no-unsafe-argument
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
          <div class="flex max-h-[60vh] max-w-[75vw] flex-col space-y-2 overflow-auto scrollbar-thin scrollbar-track-neutral-200 scrollbar-thumb-neutral-400 scrollbar-thumb-rounded-md dark:scrollbar-track-neutral-800 dark:scrollbar-thumb-neutral-600">
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

export default ChunkMetadataDisplay;
