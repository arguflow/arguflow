import { Params, useSearchParams } from "@solidjs/router";
import { createEffect, on } from "solid-js";
import { createStore, unwrap } from "solid-js/store";
import { Filters } from "../components/FilterModal";

export interface SortByField {
  field: string;
}

export interface SortBySearchType {
  rerank_type: string;
  rerank_query: string;
}

export interface MultiQuery {
  query: string;
  weight: number;
}

export interface FulltextBoost {
  phrase?: string;
  boost_factor?: number;
}

export interface SemanticBoost {
  phrase?: string;
  distance_factor?: number;
}

export interface ScoringOptions {
  fulltext_boost?: FulltextBoost;
  semantic_boost?: SemanticBoost;
}

export function isSortByField(
  sortBy: SortByField | SortBySearchType,
): sortBy is SortByField {
  return (sortBy as SortByField).field !== undefined;
}

export function isSortBySearchType(
  sortBy: SortByField | SortBySearchType,
): sortBy is SortBySearchType {
  return (sortBy as SortBySearchType).rerank_type !== undefined;
}

export type HighlightStrategy = "v1" | "exactmatch";

export function isHighlightStrategy(
  value: string | undefined,
): value is HighlightStrategy {
  return value === "v1" || value === "exactmatch";
}

export interface SearchOptions {
  version: number;
  query: string;
  searchType: string;
  scoreThreshold: number;
  extendResults: boolean;
  slimChunks: boolean;
  groupUniqueSearch: boolean;
  correctTypos: boolean;
  oneTypoWordRangeMin: number;
  oneTypoWordRangeMax: number | null;
  twoTypoWordRangeMin: number;
  twoTypoWordRangeMax: number | null;
  prioritize_domain_specifc_words: boolean | null;
  disableOnWords: string[];
  sort_by: SortByField | SortBySearchType;
  mmr: {
    use_mmr: boolean;
    mmr_lambda?: number;
  };
  pageSize: number;
  getTotalPages: boolean;
  highlightResults: boolean;
  highlightStrategy: HighlightStrategy;
  highlightThreshold: number;
  highlightDelimiters: string[];
  highlightMaxLength: number;
  highlightMaxNum: number;
  highlightWindow: number;
  highlightPreTag: string;
  highlightPostTag: string;
  group_size: number;
  useQuoteNegatedTerms: boolean;
  removeStopWords: boolean;
  filters: Filters | null;
  multiQueries: MultiQuery[];
  audioBase64?: string;
  scoringOptions?: ScoringOptions;
  taskDefinition?: string;
}

const initalState: SearchOptions = {
  version: 0, // Variable used to subscribe to entire store.
  query: "",
  searchType: "",
  scoreThreshold: 0.0,
  extendResults: false,
  slimChunks: false,
  groupUniqueSearch: false,
  sort_by: {
    field: "",
  },
  mmr: {
    use_mmr: false,
  },
  pageSize: 10,
  getTotalPages: true,
  correctTypos: false,
  oneTypoWordRangeMin: 4,
  oneTypoWordRangeMax: 6,
  twoTypoWordRangeMin: 6,
  twoTypoWordRangeMax: null,
  prioritize_domain_specifc_words: true,
  disableOnWords: [],
  highlightResults: true,
  highlightStrategy: "exactmatch",
  highlightThreshold: 0.8,
  highlightDelimiters: ["?", ".", "!"],
  highlightMaxLength: 8,
  highlightMaxNum: 3,
  highlightWindow: 0,
  highlightPreTag: "<mark><b>",
  highlightPostTag: "</b></mark>",
  group_size: 3,
  useQuoteNegatedTerms: false,
  removeStopWords: false,
  filters: {
    must: [],
    must_not: [],
    should: [],
  } as Filters,
  multiQueries: [],
  audioBase64: "",
  scoringOptions: undefined,
};

const fromStateToParams = (state: SearchOptions): Params => {
  return {
    // DO NOT INCLUDE VERSION
    query: state.query,
    searchType: state.searchType,
    scoreThreshold: state.scoreThreshold.toString(),
    extendResults: state.extendResults.toString(),
    slimChunks: state.slimChunks.toString(),
    groupUniqueSearch: state.groupUniqueSearch.toString(),
    sort_by: JSON.stringify(state.sort_by),
    pageSize: state.pageSize.toString(),
    getTotalPages: state.getTotalPages.toString(),
    correctTypos: state.correctTypos.toString(),
    oneTypoWordRangeMin: state.oneTypoWordRangeMin.toString(),
    oneTypoWordRangeMax: state.oneTypoWordRangeMax?.toString() ?? "6",
    twoTypoWordRangeMin: state.twoTypoWordRangeMin.toString(),
    twoTypoWordRangeMax: state.twoTypoWordRangeMax?.toString() ?? "",
    mmr: JSON.stringify(state.mmr),
    prioritize_domain_specifc_words:
      state.prioritize_domain_specifc_words?.toString() ?? "",
    disableOnWords: state.disableOnWords.join(","),
    highlightStrategy: state.highlightStrategy,
    highlightResults: state.highlightResults.toString(),
    highlightThreshold: state.highlightThreshold.toString(),
    highlightDelimiters: state.highlightDelimiters.join(","),
    highlightMaxLength: state.highlightMaxLength.toString(),
    highlightMaxNum: state.highlightMaxNum.toString(),
    highlightWindow: state.highlightWindow.toString(),
    highlightPreTag: state.highlightPreTag,
    highlightPostTag: state.highlightPostTag,
    group_size: state.group_size?.toString(),
    useQuoteNegatedTerms: state.useQuoteNegatedTerms.toString(),
    removeStopWords: state.removeStopWords.toString(),
    filters: JSON.stringify(state.filters),
    multiQueries: JSON.stringify(state.multiQueries),
    scoringOptions: JSON.stringify(state.scoringOptions),
  };
};

const parseIntOrNull = (str: string | undefined) => {
  if (!str || str === "") {
    return null;
  }
  return parseInt(str);
};

const fromParamsToState = (
  params: Partial<Params>,
): Omit<SearchOptions, "version"> => {
  return {
    query: params.query ?? initalState.query,
    searchType: params.searchType ?? initalState.searchType,
    scoreThreshold: parseFloat(params.scoreThreshold ?? "0.0"),
    extendResults: params.extendResults === "true",
    slimChunks: params.slimChunks === "true",
    groupUniqueSearch: params.groupUniqueSearch === "true",
    sort_by:
      (JSON.parse(params.sort_by ?? "{}") as SortByField | SortBySearchType) ??
      initalState.sort_by,
    pageSize: parseInt(params.pageSize ?? "10"),
    getTotalPages: (params.getTotalPages ?? "true") === "true",
    mmr:
      (JSON.parse(params.mmr ?? "{}") as {
        use_mmr: boolean;
        mmr_lambda?: number;
      }) ?? initalState.mmr,
    correctTypos: (params.correctTypos ?? "false") === "true",
    oneTypoWordRangeMin: parseInt(params.oneTypoWordRangeMin ?? "4"),
    oneTypoWordRangeMax: parseIntOrNull(params.oneTypoWordRangeMax),
    twoTypoWordRangeMin: parseInt(params.oneTypoWordRangeMin ?? "6"),
    twoTypoWordRangeMax: parseIntOrNull(params.twoTypoWordRangeMax),
    prioritize_domain_specifc_words:
      (params.prioritize_domain_specifc_words ?? "true") === "true",
    disableOnWords: params.disableOnWords?.split(",") ?? [],
    highlightResults: (params.highlightResults ?? "true") === "true",
    highlightStrategy: isHighlightStrategy(params.highlightStrategy)
      ? params.highlightStrategy
      : "exactmatch",
    highlightThreshold: parseFloat(params.highlightThreshold ?? "0.8"),
    highlightDelimiters:
      params.highlightDelimiters?.split(",") ?? initalState.highlightDelimiters,
    highlightMaxLength: parseInt(params.highlightMaxLength ?? "8"),
    highlightMaxNum: parseInt(params.highlightMaxNum ?? "3"),
    highlightWindow: parseInt(params.highlightWindow ?? "0"),
    highlightPreTag: params.highlightPreTag ?? initalState.highlightPreTag,
    highlightPostTag: params.highlightPostTag ?? initalState.highlightPostTag,
    group_size: parseInt(params.group_size ?? "3"),
    useQuoteNegatedTerms: (params.useQuoteNegatedTerms ?? "false") === "true",
    removeStopWords: (params.removeStopWords ?? "false") === "true",
    filters: JSON.parse(params.filters ?? "null") as Filters | null,
    multiQueries: JSON.parse(params.multiQueries ?? "[]") as MultiQuery[],
    scoringOptions: JSON.parse(
      params.scoringOptions ?? "null",
    ) as ScoringOptions,
  };
};

export const useSearch = () => {
  const [searchParams, setSearchParams] = useSearchParams();

  const [state, setSearch] = createStore({
    ...initalState,
    ...fromParamsToState(searchParams),
  });

  const [debounced, setDebouncedState] = createStore({
    ...initalState,
    ...fromParamsToState(searchParams),
  });

  createEffect(
    on(
      () => state.version,
      () => {
        setSearchParams({
          ...fromStateToParams(unwrap(state)),
        });

        const timeout = setTimeout(() => {
          setDebouncedState({ ...unwrap(state) });
        }, 200);
        return () => clearTimeout(timeout);
      },
    ),
  );

  // @ts-expect-error args are passed to setSearch
  const proxiedSet: typeof setSearch = (
    ...args: Parameters<typeof setSearch>
  ) => {
    // @ts-expect-error args are passed to setSearch
    setSearch(...args);
    setSearch("version", (prev) => prev + 1);
  };

  return {
    debounced,
    state,
    setSearch: proxiedSet,
  };
};

export type SearchStore = ReturnType<typeof useSearch>;
