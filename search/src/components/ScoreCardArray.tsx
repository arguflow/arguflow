import type { Setter } from "solid-js";
import { Show, createSignal, onMount } from "solid-js";
import type {
  CardMetadataWithVotes,
  CardCollectionDTO,
} from "../../utils/apiTypes";
import type { ScoreCardProps } from "./ScoreCard";
import { FiChevronLeft, FiChevronRight } from "solid-icons/fi";
import ScoreCard from "./ScoreCard";

export type ScoreCardAraryProps = Omit<ScoreCardProps, "card"> & {
  cards: CardMetadataWithVotes[];
  setCardCollections: Setter<CardCollectionDTO[]>;
};

export const ScoreCardArray = (props: ScoreCardAraryProps) => {
  const [curCard, setCurCard] = createSignal(0);
  const [beginTime, setBeginTime] = createSignal(Date.now());
  const [endTime, setEndTime] = createSignal(0);
  const dateValue =
    (import.meta.env.PUBLIC_DATE_RANGE_VALUE as string | undefined) ?? "Date";
  onMount(() => {
    props.cards.forEach((card) => {
      if (card.metadata) {
        const dateObject = new Date(card.metadata[dateValue]);
        if (dateObject.getTime()) {
          setBeginTime((prev) => Math.min(prev, dateObject.getTime()));
          setEndTime((prev) => Math.max(prev, dateObject.getTime()));
        }
      }
    });
  });
  return (
    <div class="mx-auto flex max-w-[calc(100vw-32px)] items-center">
      <div class="w-[16px] min-[360px]:w-[32px]">
        <Show when={curCard() > 0}>
          <button onClick={() => setCurCard((prev) => prev - 1)}>
            <FiChevronLeft class="h-4 w-4 min-[360px]:h-8 min-[360px]:w-8" />
          </button>
        </Show>
        <Show when={curCard() <= 0}>
          <FiChevronLeft class="h-4 w-4 text-transparent min-[360px]:h-8 min-[360px]:w-8" />
        </Show>
      </div>
      <ScoreCard
        {...props}
        card={props.cards[curCard()]}
        counter={curCard() + 1}
        total={props.cards.length}
        begin={beginTime()}
        end={endTime()}
      />
      <div class="w-[16px] min-[360px]:w-[32px]">
        <Show when={curCard() < props.cards.length - 1}>
          <button onClick={() => setCurCard((prev) => prev + 1)}>
            <FiChevronRight class="h-4 w-4 min-[360px]:h-8 min-[360px]:w-8" />
          </button>
        </Show>
        <Show when={curCard() >= props.cards.length - 1}>
          <FiChevronRight class="h-4 w-4 text-transparent min-[360px]:h-8 min-[360px]:w-8" />
        </Show>
      </div>
    </div>
  );
};
