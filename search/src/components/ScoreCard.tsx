import {
  For,
  Setter,
  Show,
  createEffect,
  createMemo,
  createSignal,
} from "solid-js";
import {
  indirectHasOwnProperty,
  type CardBookmarksDTO,
  type CardCollectionDTO,
  type CardMetadataWithVotes,
} from "../../utils/apiTypes";
import { BiRegularChevronDown, BiRegularChevronUp } from "solid-icons/bi";
import {
  RiArrowsArrowDownCircleFill,
  RiArrowsArrowDownCircleLine,
  RiArrowsArrowUpCircleFill,
  RiArrowsArrowUpCircleLine,
} from "solid-icons/ri";
import BookmarkPopover from "./BookmarkPopover";
import { VsFileSymlinkFile } from "solid-icons/vs";
import sanitizeHtml from "sanitize-html";
import { FiEdit, FiGlobe, FiLock, FiTrash, FiCheck } from "solid-icons/fi";
import { FaRegularFileImage } from "solid-icons/fa";
import { Tooltip } from "./Atoms/Tooltip";
import { AiOutlineCopy } from "solid-icons/ai";
import CommunityBookmarkPopover from "./CommunityBookmarkPopover";
import { FullScreenModal } from "./Atoms/FullScreenModal";

export const sanitzerOptions = {
  allowedTags: [...sanitizeHtml.defaults.allowedTags, "font"],
  allowedAttributes: {
    ...sanitizeHtml.defaults.allowedAttributes,
    "*": ["style"],
  },
};

export interface ScoreCardProps {
  signedInUserId?: string;
  cardCollections: CardCollectionDTO[];
  totalCollectionPages: number;
  collection?: boolean;
  card: CardMetadataWithVotes;
  score: number;
  setShowModal: Setter<boolean>;
  setOnDelete: Setter<() => void>;
  setShowConfirmModal: Setter<boolean>;
  initialExpanded?: boolean;
  bookmarks: CardBookmarksDTO[];
  showExpand?: boolean;
  setCardCollections: Setter<CardCollectionDTO[]>;
}

const ScoreCard = (props: ScoreCardProps) => {
  const apiHost = import.meta.env.PUBLIC_API_HOST as string;

  const frontMatterVals = (
    (import.meta.env.PUBLIC_FRONTMATTER_VALS as string | undefined) ??
    "link,tag_set"
  ).split(",");

  const linesBeforeShowMore = (() => {
    const parsedLinesBeforeShowMore = Number.parseInt(
      (import.meta.env.PUBLIC_LINES_BEFORE_SHOW_MORE as string | undefined) ??
        "4",
      10,
    );
    return Number.isNaN(parsedLinesBeforeShowMore)
      ? 4
      : parsedLinesBeforeShowMore;
  })();

  const [expanded, setExpanded] = createSignal(props.initialExpanded ?? false);
  const [userVote, setUserVote] = createSignal(0);
  const [totalVote, setTotalVote] = createSignal(
    // eslint-disable-next-line solid/reactivity
    props.card.total_upvotes - props.card.total_downvotes,
  );
  const [showPropsModal, setShowPropsModal] = createSignal(false);
  const [deleting, setDeleting] = createSignal(false);
  const [deleted, setDeleted] = createSignal(false);
  const [copied, setCopied] = createSignal(false);
  const [showImageModal, setShowImageModal] = createSignal(false);

  const imgInformation = createMemo(() => {
    const imgRangeStartKey = import.meta.env
      .PUBLIC_IMAGE_RANGE_START_KEY as string;
    const imgRangeEndKey = import.meta.env.PUBLIC_IMAGE_RANGE_END_KEY as string;

    if (
      !imgRangeStartKey ||
      !props.card.metadata ||
      !indirectHasOwnProperty(props.card.metadata, imgRangeStartKey) ||
      !indirectHasOwnProperty(props.card.metadata, imgRangeEndKey)
    ) {
      return null;
    }

    // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-explicit-any
    const imgRangeStartVal = (props.card.metadata as any)[
      imgRangeStartKey
    ] as unknown as string;
    // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-explicit-any
    const imgRangeEndVal = (props.card.metadata as any)[
      imgRangeEndKey
    ] as unknown as string;
    const imgRangeStart = parseInt(imgRangeStartVal.replace(/\D+/g, ""), 10);
    const imgRangeEnd = parseInt(imgRangeEndVal.replace(/\D+/g, ""), 10);
    const imgRangePrefix = imgRangeStartVal.slice(
      0,
      -imgRangeStart.toString().length,
    );

    return {
      imgRangeStart,
      imgRangeEnd,
      imgRangePrefix,
    };
  });

  createEffect(() => {
    if (!showPropsModal()) return;

    props.setShowModal(true);
    setShowPropsModal(false);
  });

  createEffect(() => {
    if (props.card.vote_by_current_user === null) {
      return;
    }
    const userVote = props.card.vote_by_current_user ? 1 : -1;
    setUserVote(userVote);
    const newTotalVote =
      props.card.total_upvotes - props.card.total_downvotes - userVote;
    setTotalVote(newTotalVote);
  });

  const deleteVote = (prev_vote: number) => {
    void fetch(`${apiHost}/vote/${props.card.id}`, {
      method: "DELETE",
      credentials: "include",
    }).then((response) => {
      if (!response.ok) {
        setUserVote(prev_vote);
        if (response.status === 401) setShowPropsModal(true);
      }
    });
  };

  const createVote = (prev_vote: number, new_vote: number) => {
    if (new_vote === 0) {
      deleteVote(prev_vote);
      return;
    }

    void fetch(`${apiHost}/vote`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      credentials: "include",
      body: JSON.stringify({
        card_metadata_id: props.card.id,
        vote: new_vote === 1 ? true : false,
      }),
    }).then((response) => {
      if (!response.ok) {
        setUserVote(prev_vote);
        if (response.status === 401) setShowPropsModal(true);
      }
    });
  };

  const deleteCard = () => {
    if (props.signedInUserId !== props.card.author?.id) return;

    const curCardMetadataId = props.card.id;

    props.setOnDelete(() => {
      return () => {
        setDeleting(true);
        void fetch(`${apiHost}/card/${curCardMetadataId}`, {
          method: "DELETE",
          credentials: "include",
        }).then((response) => {
          setDeleting(false);
          if (response.ok) {
            setDeleted(true);
            window.location.href = "/";
            return;
          }
          alert("Failed to delete card");
        });
      };
    });

    props.setShowConfirmModal(true);
  };

  const copyCard = () => {
    navigator.clipboard
      .write([
        new ClipboardItem({
          "text/html": new Blob([props.card.card_html ?? ""], {
            type: "text/html",
          }),
          "text/plain": new Blob([props.card.content], {
            type: "text/plain",
          }),
        }),
      ])
      .then(() => {
        setCopied(true);
        setTimeout(() => {
          setCopied(false);
        }, 2000);
      })
      .catch((err: string) => {
        alert("Failed to copy to clipboard: " + err);
      });
  };

  return (
    <>
      <Show when={!deleted()}>
        <div class="mx-auto flex w-full max-w-[calc(100%-32px)] flex-col items-center rounded-md bg-neutral-100 p-2 dark:!bg-neutral-800 min-[360px]:max-w-[calc(100%-64px)]">
          <div class="flex w-full flex-col space-y-2">
            <div class="flex h-fit items-center space-x-1">
              <Show when={props.card.private}>
                <Tooltip
                  body={<FiLock class="h-5 w-5 text-green-500" />}
                  tooltipText="Private. Only you can see this card."
                />
              </Show>
              <Show when={!props.card.private}>
                <Tooltip
                  body={<FiGlobe class="h-5 w-5 text-green-500" />}
                  tooltipText="Publicly visible"
                />
              </Show>
              <div class="flex-1" />
              <Show when={imgInformation()}>
                <button
                  class="h-fit"
                  onClick={() => setShowImageModal(true)}
                  title="View Images"
                >
                  <FaRegularFileImage class="h-5 w-5 fill-current" />
                </button>
              </Show>
              <Show when={!copied()}>
                <button class="h-fit" onClick={() => copyCard()}>
                  <AiOutlineCopy class="h-5 w-5 fill-current" />
                </button>
              </Show>
              <Show when={copied()}>
                <FiCheck class="text-green-500" />
              </Show>
              <Show when={props.signedInUserId == props.card.author?.id}>
                <button
                  classList={{
                    "h-fit text-red-700 dark:text-red-400": true,
                    "animate-pulse": deleting(),
                  }}
                  title="Delete"
                  onClick={() => deleteCard()}
                >
                  <FiTrash class="h-5 w-5" />
                </button>
              </Show>
              <Show when={props.signedInUserId == props.card.author?.id}>
                <a title="Edit" href={`/card/edit/${props.card.id}`}>
                  <FiEdit class="h-5 w-5" />
                </a>
              </Show>
              <a title="Open" href={`/card/${props.card.id}`}>
                <VsFileSymlinkFile class="h-5 w-5 fill-current" />
              </a>
              <CommunityBookmarkPopover
                bookmarks={props.bookmarks.filter(
                  (bookmark) => bookmark.card_uuid === props.card.id,
                )}
              />
              <BookmarkPopover
                signedInUserId={props.signedInUserId}
                totalCollectionPages={props.totalCollectionPages}
                cardCollections={props.cardCollections}
                cardMetadata={props.card}
                setLoginModal={props.setShowModal}
                bookmarks={props.bookmarks.filter(
                  (bookmark) => bookmark.card_uuid === props.card.id,
                )}
                setCardCollections={props.setCardCollections}
              />
            </div>
            <div class="flex w-full items-start">
              <div class="flex flex-col items-center pr-2">
                <Show when={!props.card.private}>
                  <button
                    onClick={(e) => {
                      e.preventDefault();
                      setUserVote((prev) => {
                        const new_val = prev === 1 ? 0 : 1;
                        createVote(prev, new_val);
                        return new_val;
                      });
                    }}
                  >
                    <Show when={userVote() === 1}>
                      <RiArrowsArrowUpCircleFill class="h-8 w-8 fill-current !text-turquoise-500" />
                    </Show>
                    <Show when={userVote() != 1}>
                      <RiArrowsArrowUpCircleLine class="h-8 w-8 fill-current" />
                    </Show>
                  </button>
                  <span class="my-1">{totalVote() + userVote()}</span>
                  <button
                    onClick={(e) => {
                      e.preventDefault();
                      setUserVote((prev) => {
                        const new_val = prev === -1 ? 0 : -1;
                        createVote(prev, new_val);
                        return new_val;
                      });
                    }}
                  >
                    <Show when={userVote() === -1}>
                      <RiArrowsArrowDownCircleFill class="h-8 w-8 fill-current !text-turquoise-500" />
                    </Show>
                    <Show when={userVote() != -1}>
                      <RiArrowsArrowDownCircleLine class="h-8 w-8 fill-current" />
                    </Show>
                  </button>
                </Show>
              </div>
              <div class="flex w-full flex-col">
                <For each={frontMatterVals}>
                  {(frontMatterVal) => (
                    <>
                      <Show when={props.card.link && frontMatterVal == "link"}>
                        <a
                          class="line-clamp-1 w-fit break-all text-magenta-500 underline dark:text-turquoise-400"
                          target="_blank"
                          href={props.card.link ?? ""}
                        >
                          {props.card.link}
                        </a>
                      </Show>
                      <Show
                        when={
                          // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
                          frontMatterVal !== "link" &&
                          props.card.metadata &&
                          indirectHasOwnProperty(
                            props.card.metadata,
                            frontMatterVal,
                          ) &&
                          // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-explicit-any
                          (props.card.metadata as any)[frontMatterVal]
                        }
                      >
                        <div class="flex space-x-2">
                          <span class="font-semibold text-neutral-800 dark:text-neutral-200">
                            {frontMatterVal}:{" "}
                          </span>
                          <span class="line-clamp-1 break-all">
                            {props.card.metadata &&
                              indirectHasOwnProperty(
                                props.card.metadata,
                                frontMatterVal,
                              ) &&
                              // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-explicit-any, @typescript-eslint/no-unsafe-call
                              (props.card.metadata as any)[
                                frontMatterVal
                              ].replace(/ +/g, " ")}
                          </span>
                        </div>
                      </Show>
                    </>
                  )}
                </For>
                <div class="grid w-fit auto-cols-min grid-cols-[1fr,3fr] gap-x-2 text-neutral-800 dark:text-neutral-200">
                  <Show when={props.score != 0}>
                    <span class="font-semibold">Similarity: </span>
                    <span>{props.score.toPrecision(3)}</span>
                  </Show>
                </div>
              </div>
            </div>
          </div>
          <div class="mb-1 h-1 w-full border-b border-neutral-300 dark:border-neutral-600" />
          <div
            classList={{
              "line-clamp-4 gradient-mask-b-0": !expanded(),
              "text-ellipsis max-w-[100%] break-words space-y-5 leading-normal":
                true,
            }}
            style={
              !expanded() ? { "-webkit-line-clamp": linesBeforeShowMore } : {}
            }
            // eslint-disable-next-line solid/no-innerhtml
            innerHTML={sanitizeHtml(
              props.card.card_html !== undefined
                ? props.card.card_html
                    .replaceAll("line-height", "lh")
                    .replace("\n", " ")
                    .replace(`<br>`, " ")
                    .replace(`\\n`, " ")
                : "",
              sanitzerOptions,
            )}
          />
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
        </div>
      </Show>
      <Show when={showImageModal()}>
        <FullScreenModal isOpen={showImageModal} setIsOpen={setShowImageModal}>
          <div class="flex max-h-[75vh] max-w-[75vw] flex-col space-y-2 overflow-auto">
            <For
              each={Array.from({
                length:
                  (imgInformation()?.imgRangeEnd ?? 0) -
                  (imgInformation()?.imgRangeStart ?? 0) +
                  1,
              })}
            >
              {(_, i) => (
                <img
                  class="mx-auto my-auto"
                  src={`${apiHost}/image/${
                    imgInformation()?.imgRangePrefix ?? ""
                  }${(imgInformation()?.imgRangeStart ?? 0) + i()}.png`}
                />
              )}
            </For>
          </div>
        </FullScreenModal>
      </Show>
    </>
  );
};

export default ScoreCard;
