import { Show, createEffect, createSignal, For, onMount } from "solid-js";
import {
  isUserDTO,
  type CardCollectionDTO,
  type CardsWithTotalPagesDTO,
  type ScoreCardDTO,
  type UserDTO,
  CardBookmarksDTO,
  isCardCollectionPageDTO,
} from "../../utils/apiTypes";
import { BiRegularLogIn, BiRegularXCircle } from "solid-icons/bi";
import { FullScreenModal } from "./Atoms/FullScreenModal";
import { PaginationController } from "./Atoms/PaginationController";
import { ConfirmModal } from "./Atoms/ConfirmModal";
import { ScoreCardArray } from "./ScoreCardArray";

export interface Filters {
  tagSet: string[];
  link: string[];
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  metadataFilters: any;
}
export interface ResultsPageProps {
  query: string;
  page: number;
  defaultResultCards: CardsWithTotalPagesDTO;
  filters: Filters;
  searchType: string;
}

const ResultsPage = (props: ResultsPageProps) => {
  const apiHost = import.meta.env.PUBLIC_API_HOST as string;
  const initialResultCards = props.defaultResultCards.score_cards;
  const initialTotalPages = props.defaultResultCards.total_card_pages;

  const [cardCollections, setCardCollections] = createSignal<
    CardCollectionDTO[]
  >([]);
  const [user, setUser] = createSignal<UserDTO | undefined>();
  const [resultCards, setResultCards] =
    createSignal<ScoreCardDTO[]>(initialResultCards);
  const [clientSideRequestFinished, setClientSideRequestFinished] =
    createSignal(false);
  const [showNeedLoginModal, setShowNeedLoginModal] = createSignal(false);
  const [showConfirmDeleteModal, setShowConfirmDeleteModal] =
    createSignal(false);
  const [totalCollectionPages, setTotalCollectionPages] = createSignal(0);
  // eslint-disable-next-line @typescript-eslint/no-empty-function
  const [onDelete, setOnDelete] = createSignal(() => {});
  const [bookmarks, setBookmarks] = createSignal<CardBookmarksDTO[]>([]);
  const [totalPages, setTotalPages] = createSignal(initialTotalPages);

  const fetchCardCollections = () => {
    if (!user()) return;

    void fetch(`${apiHost}/card_collection/1`, {
      method: "GET",
      credentials: "include",
    }).then((response) => {
      if (response.ok) {
        void response.json().then((data) => {
          if (isCardCollectionPageDTO(data)) {
            setCardCollections(data.collections);
            setTotalCollectionPages(data.total_pages);
          }
        });
      }
    });
  };

  const fetchBookmarks = () => {
    void fetch(`${apiHost}/card_collection/bookmark`, {
      method: "POST",
      credentials: "include",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        card_ids: resultCards().flatMap((c) => {
          return c.metadata.map((m) => m.id);
        }),
      }),
    }).then((response) => {
      if (response.ok) {
        void response.json().then((data) => {
          const cardBookmarks = data as CardBookmarksDTO[];
          setBookmarks(cardBookmarks);
        });
      }
    });
  };

  // Fetch the user info for the auth'ed user
  createEffect(() => {
    void fetch(`${apiHost}/auth`, {
      method: "GET",
      credentials: "include",
    }).then((response) => {
      if (response.ok) {
        void response.json().then((data) => {
          isUserDTO(data) ? setUser(data) : setUser(undefined);
        });
        return;
      }
    });
  });

  createEffect(() => {
    const abortController = new AbortController();

    void fetch(`${apiHost}/card/${props.searchType}/${props.page}`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      credentials: "include",
      signal: abortController.signal,
      body: JSON.stringify({
        content: props.query,
        tag_set: props.filters.tagSet,
        link: props.filters.link,
        // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
        filters: props.filters.metadataFilters,
      }),
    }).then((response) => {
      if (response.ok) {
        void response.json().then((data) => {
          // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
          const result = data.score_cards as ScoreCardDTO[];
          setResultCards(result);
          // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
          setTotalPages(data.total_card_pages);
          setClientSideRequestFinished(true);
        });
      } else {
        setClientSideRequestFinished(true);
      }
    });

    fetchCardCollections();

    return () => {
      abortController.abort();
    };
  });

  onMount(() => {
    fetchBookmarks();
  });

  return (
    <>
      <div class="mt-12 flex w-full flex-col items-center space-y-4">
        <Show when={resultCards().length === 0 && !clientSideRequestFinished()}>
          <div
            class="text-primary inline-block h-12 w-12 animate-spin rounded-full border-4 border-solid border-current border-magenta border-r-transparent align-[-0.125em] motion-reduce:animate-[spin_1.5s_linear_infinite]"
            role="status"
          >
            <span class="!absolute !-m-px !h-px !w-px !overflow-hidden !whitespace-nowrap !border-0 !p-0 ![clip:rect(0,0,0,0)]">
              Loading...
            </span>
          </div>
        </Show>
        <Show when={resultCards().length === 0 && clientSideRequestFinished()}>
          <button
            onClick={() => {
              window.location.href = `/search?q=${props.query}&page=${
                props.page + 1
              }`;
            }}
          >
            <div class="text-2xl">No results found</div>
          </button>
        </Show>
        <div class="flex w-full max-w-6xl flex-col space-y-4 px-1 min-[360px]:px-4 sm:px-8 md:px-20">
          <For each={resultCards()}>
            {(card) => (
              <div>
                <ScoreCardArray
                  totalCollectionPages={totalCollectionPages()}
                  signedInUserId={user()?.id}
                  cardCollections={cardCollections()}
                  cards={card.metadata}
                  score={card.score}
                  setShowModal={setShowNeedLoginModal}
                  bookmarks={bookmarks()}
                  setOnDelete={setOnDelete}
                  setShowConfirmModal={setShowConfirmDeleteModal}
                  showExpand={clientSideRequestFinished()}
                  setCardCollections={setCardCollections}
                />
              </div>
            )}
          </For>
        </div>
      </div>
      <div class="mx-auto my-12 flex items-center space-x-2">
        <PaginationController page={props.page} totalPages={totalPages()} />
      </div>
      <Show when={showNeedLoginModal()}>
        <FullScreenModal
          isOpen={showNeedLoginModal}
          setIsOpen={setShowNeedLoginModal}
        >
          <div class="min-w-[250px] sm:min-w-[300px]">
            <BiRegularXCircle class="mx-auto h-8 w-8 fill-current !text-red-500" />
            <div class="mb-4 text-center text-xl font-bold">
              Cannot vote or use bookmarks without an account
            </div>
            <div class="mx-auto flex w-fit flex-col space-y-3">
              <a
                class="flex space-x-2 rounded-md bg-magenta-500 p-2 text-white"
                href="/auth/register"
              >
                Register
                <BiRegularLogIn class="h-6 w-6 fill-current" />
              </a>
            </div>
          </div>
        </FullScreenModal>
      </Show>
      <ConfirmModal
        showConfirmModal={showConfirmDeleteModal}
        setShowConfirmModal={setShowConfirmDeleteModal}
        onConfirm={onDelete}
        message="Are you sure you want to delete this card?"
      />
    </>
  );
};

export default ResultsPage;
