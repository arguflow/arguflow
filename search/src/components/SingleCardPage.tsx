import { For, Show, createEffect, createMemo, createSignal } from "solid-js";
import {
  isUserDTO,
  type CardCollectionDTO,
  type CardMetadataWithVotes,
  type UserDTO,
  isCardMetadataWithVotes,
  SingleCardDTO,
  CardBookmarksDTO,
  isCardCollectionPageDTO,
  CardMetadata,
} from "../../utils/apiTypes";
import ScoreCard from "./ScoreCard";
import { FullScreenModal } from "./Atoms/FullScreenModal";
import { BiRegularLogIn, BiRegularXCircle } from "solid-icons/bi";
import { ConfirmModal } from "./Atoms/ConfirmModal";
import CardMetadataDisplay from "./CardMetadataDisplay";

export interface SingleCardPageProps {
  cardId: string | undefined;
  defaultResultCard: SingleCardDTO;
}
export const SingleCardPage = (props: SingleCardPageProps) => {
  const apiHost = import.meta.env.PUBLIC_API_HOST as string;
  const initialCardMetadata = props.defaultResultCard.metadata;

  const [showNeedLoginModal, setShowNeedLoginModal] = createSignal(false);
  const [cardMetadata, setCardMetadata] =
    createSignal<CardMetadataWithVotes | null>(initialCardMetadata);
  const [error, setError] = createSignal("");
  const [fetching, setFetching] = createSignal(true);
  const [cardCollections, setCardCollections] = createSignal<
    CardCollectionDTO[]
  >([]);
  const [user, setUser] = createSignal<UserDTO | undefined>();
  const [bookmarks, setBookmarks] = createSignal<CardBookmarksDTO[]>([]);
  const [showConfirmDeleteModal, setShowConfirmDeleteModal] =
    createSignal(false);
  const [totalCollectionPages, setTotalCollectionPages] = createSignal(0);
  // eslint-disable-next-line @typescript-eslint/no-empty-function
  const [onDelete, setOnDelete] = createSignal(() => {});
  const [clientSideRequestFinished, setClientSideRequestFinished] =
    createSignal(false);
  const [loadingRecommendations, setLoadingRecommendations] =
    createSignal(false);
  const [recommendedCards, setRecommendedCards] = createSignal<CardMetadata[]>(
    [],
  );

  if (props.defaultResultCard.status == 401) {
    setError("You are not authorized to view this card.");
  }
  if (props.defaultResultCard.status == 404) {
    setError("This card could not be found.");
  }

  // Fetch the card collections for the auth'ed user
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
        card_ids: cardMetadata()?.id ? [cardMetadata()?.id] : [],
      }),
    }).then((response) => {
      if (response.ok) {
        void response.json().then((data) => {
          setBookmarks(data as CardBookmarksDTO[]);
        });
      }
    });
  };

  const fetchRecommendations = (
    ids: string[],
    prev_recommendations: CardMetadata[],
  ) => {
    setLoadingRecommendations(true);
    void fetch(`${apiHost}/card/recommend`, {
      method: "POST",
      credentials: "include",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        positive_card_ids: ids,
        limit: prev_recommendations.length + 10,
      }),
    }).then((response) => {
      if (response.ok) {
        void response.json().then((data) => {
          const typed_data = data as CardMetadata[];
          const deduped_data = typed_data.filter((d) => {
            return !prev_recommendations.some((c) => c.id == d.id);
          });
          const new_recommendations = [
            ...prev_recommendations,
            ...deduped_data,
          ];
          setLoadingRecommendations(false);
          setRecommendedCards(new_recommendations);
        });
      }
    });
  };

  createEffect(() => {
    fetchCardCollections();
    fetchBookmarks();
  });

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
      }
    });
  });

  createEffect(() => {
    setFetching(true);
    void fetch(`${apiHost}/card/${props.cardId ?? ""}`, {
      method: "GET",
      credentials: "include",
    }).then((response) => {
      if (response.ok) {
        void response.json().then((data) => {
          if (!isCardMetadataWithVotes(data)) {
            setError("This card could not be found.");
            setFetching(false);
            return;
          }

          setCardMetadata(data);
          setError("");
        });
      }
      if (response.status == 403) {
        setError("You are not authorized to view this card.");
      }
      if (response.status == 404) {
        setError("This card could not be found.");
      }
      if (response.status == 401) {
        setError("Sign in to view this card.");
        setShowNeedLoginModal(true);
      }
      setClientSideRequestFinished(true);
      setFetching(false);
    });
  });

  const getCard = createMemo(() => {
    if (error().length > 0) {
      return null;
    }
    const curCardMetadata = cardMetadata();
    if (!curCardMetadata) {
      return null;
    }

    return (
      <ScoreCard
        totalCollectionPages={totalCollectionPages()}
        signedInUserId={user()?.id}
        card={curCardMetadata}
        score={0}
        setShowModal={setShowNeedLoginModal}
        cardCollections={cardCollections()}
        bookmarks={bookmarks()}
        setOnDelete={setOnDelete}
        setShowConfirmModal={setShowConfirmDeleteModal}
        initialExpanded={true}
        showExpand={clientSideRequestFinished()}
        setCardCollections={setCardCollections}
        counter={0}
        total={1}
        begin={0}
        end={0}
      />
    );
  });

  return (
    <>
      <div class="mt-2 flex w-full flex-col items-center justify-center">
        <div class="flex w-full max-w-6xl flex-col justify-center px-4 sm:px-8 md:px-20">
          <Show when={error().length > 0 && !fetching()}>
            <div class="flex w-full flex-col items-center rounded-md p-2">
              <div class="text-xl font-bold text-red-500">{error()}</div>
            </div>
          </Show>
          <Show when={!cardMetadata() && fetching()}>
            <div class="flex w-full flex-col items-center justify-center space-y-4">
              <div class="animate-pulse text-xl">Loading document chunk...</div>
              <div
                class="text-primary inline-block h-12 w-12 animate-spin rounded-full border-4 border-solid border-current border-magenta border-r-transparent align-[-0.125em] motion-reduce:animate-[spin_1.5s_linear_infinite]"
                role="status"
              >
                <span class="!absolute !-m-px !h-px !w-px !overflow-hidden !whitespace-nowrap !border-0 !p-0 ![clip:rect(0,0,0,0)]">
                  Loading...
                </span>
              </div>
            </div>
          </Show>
          {getCard()}
          <Show when={cardMetadata()}>
            <Show when={recommendedCards().length > 0}>
              <div class="mx-auto mt-8 w-full max-w-[calc(100%-32px)] min-[360px]:max-w-[calc(100%-64px)]">
                <div class="flex w-full flex-col items-center rounded-md p-2">
                  <div class="text-xl font-semibold">Related Cards</div>
                </div>

                <For each={recommendedCards()}>
                  {(card) => (
                    <>
                      <div class="mt-4">
                        <CardMetadataDisplay
                          totalCollectionPages={totalCollectionPages()}
                          signedInUserId={user()?.id}
                          viewingUserId={user()?.id}
                          card={card}
                          cardCollections={cardCollections()}
                          bookmarks={bookmarks()}
                          setShowModal={setShowNeedLoginModal}
                          setShowConfirmModal={setShowConfirmDeleteModal}
                          fetchCardCollections={fetchCardCollections}
                          setCardCollections={setCardCollections}
                          setOnDelete={setOnDelete}
                          showExpand={true}
                        />
                      </div>
                    </>
                  )}
                </For>
              </div>
            </Show>
            <div class="mx-auto mt-8 w-full max-w-[calc(100%-32px)] min-[360px]:max-w-[calc(100%-64px)]">
              <button
                classList={{
                  "w-full rounded  bg-neutral-100 p-2 text-center hover:bg-neutral-100 dark:bg-neutral-700 dark:hover:bg-neutral-800":
                    true,
                  "animate-pulse": loadingRecommendations(),
                }}
                onClick={() =>
                  fetchRecommendations(
                    [cardMetadata()?.qdrant_point_id ?? ""],
                    recommendedCards(),
                  )
                }
              >
                {recommendedCards().length == 0 ? "Get" : "Get More"} Related
                Cards
              </button>
            </div>
          </Show>
        </div>
      </div>
      <Show when={showNeedLoginModal()}>
        <FullScreenModal
          isOpen={showNeedLoginModal}
          setIsOpen={setShowNeedLoginModal}
        >
          <div class="min-w-[250px] sm:min-w-[300px]">
            <BiRegularXCircle class="mx-auto h-8 w-8 fill-current !text-red-500" />
            <div class="mb-4 text-center text-xl font-bold">
              You must be signed in to vote, bookmark, or view this card it if
              it's private
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
