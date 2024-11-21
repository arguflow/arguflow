import React, {
  createContext,
  useContext,
  useEffect,
  useRef,
  useState,
} from "react";
import { Chunk, ChunkWithHighlights, GroupChunk } from "../types";
import {
  ChunkGroup,
  CountChunkQueryResponseBody,
  SearchChunksReqPayload,
  TrieveSDK,
} from "trieve-ts-sdk";
import {
  countChunks,
  getChunkIdsForGroup,
  groupSearchWithTrieve,
  searchWithTrieve,
} from "../trieve";
import { cached } from "../cache";

export const ALL_TAG = { tag: "all", label: "All", icon: null };

type simpleSearchReqPayload = Omit<
  SearchChunksReqPayload,
  "query" | "highlight_options"
>;
type customAutoCompleteAddOn = {
  use_autocomplete?: boolean;
};

export type currencyPosition = "before" | "after";
export type ModalTypes = "ecommerce" | "docs";
export type SearchModes = "chat" | "search";
export type searchOptions = simpleSearchReqPayload & customAutoCompleteAddOn;

export type ModalProps = {
  datasetId: string;
  apiKey: string;
  baseUrl?: string;
  onResultClick?: (chunk: Chunk) => void;
  theme?: "light" | "dark";
  searchOptions?: searchOptions;
  placeholder?: string;
  chat?: boolean;
  analytics?: boolean;
  ButtonEl?: JSX.ElementType;
  suggestedQueries?: boolean;
  defaultSearchQueries?: string[];
  defaultAiQuestions?: string[];
  brandLogoImgSrcUrl?: string;
  brandName?: string;
  problemLink?: string;
  brandColor?: string;
  openKeyCombination?: { key?: string; label?: string; ctrl?: boolean }[];
  tags?: {
    tag: string;
    label?: string;
    selected?: boolean;
    icon?: () => JSX.Element;
  }[];
  defaultSearchMode?: SearchModes;
  type?: ModalTypes;
  useGroupSearch?: boolean;
  allowSwitchingModes?: boolean;
  defaultCurrency?: string;
  currencyPosition?: currencyPosition;
  responsive?: boolean;
  open?: boolean;
  onOpenChange?: (open: boolean) => void;
  debounceMs?: number;
};

const defaultProps = {
  datasetId: "",
  apiKey: "",
  baseUrl: "https://api.trieve.ai",
  defaultSearchMode: "search" as SearchModes,
  placeholder: "Search...",
  theme: "light" as "light" | "dark",
  searchOptions: {
    use_autocomplete: true,
    search_type: "fulltext",
    typo_options: {
      correct_typos: true,
    },
  } as searchOptions,
  analytics: true,
  chat: true,
  suggestedQueries: true,
  trieve: (() => {}) as unknown as TrieveSDK,
  openKeyCombination: [{ ctrl: true }, { key: "k", label: "K" }],
  type: "docs" as ModalTypes,
  useGroupSearch: false,
  allowSwitchingModes: true,
  defaultCurrency: "$",
  currencyPosition: "before" as currencyPosition,
  responsive: false,
  debounceMs: 0,
};

const ModalContext = createContext<{
  props: ModalProps;
  trieveSDK: TrieveSDK;
  query: string;
  setQuery: React.Dispatch<React.SetStateAction<string>>;
  results: ChunkWithHighlights[] | GroupChunk[][];
  setResults: React.Dispatch<
    React.SetStateAction<ChunkWithHighlights[] | GroupChunk[][]>
  >;
  requestID: string;
  setRequestID: React.Dispatch<React.SetStateAction<string>>;
  loadingResults: boolean;
  setLoadingResults: React.Dispatch<React.SetStateAction<boolean>>;
  open: boolean;
  setOpen: React.Dispatch<React.SetStateAction<boolean>>;
  inputRef: React.RefObject<HTMLInputElement>;
  mode: string;
  setMode: React.Dispatch<React.SetStateAction<SearchModes>>;
  modalRef: React.RefObject<HTMLDivElement>;
  setContextProps: (props: ModalProps) => void;
  currentTag: string;
  setCurrentTag: React.Dispatch<React.SetStateAction<string>>;
  currentGroup: ChunkGroup | null;
  setCurrentGroup: React.Dispatch<React.SetStateAction<ChunkGroup | null>>;
  chatWithGroup: (group: ChunkGroup, betterGroupName?: string) => void;
  tagCounts: CountChunkQueryResponseBody[];
}>({
  props: defaultProps,
  trieveSDK: (() => {}) as unknown as TrieveSDK,
  query: "",
  results: [],
  loadingResults: false,
  open: false,
  inputRef: { current: null },
  modalRef: { current: null },
  mode: "search",
  setMode: () => {},
  setOpen: () => {},
  setQuery: () => {},
  setResults: () => {},
  requestID: "",
  setRequestID: () => {},
  setLoadingResults: () => {},
  setCurrentTag: () => {},
  currentTag: "all",
  currentGroup: null,
  setCurrentGroup: () => {},
  chatWithGroup: () => {},
  tagCounts: [],
  setContextProps: () => {},
});

const ModalProvider = ({
  children,
  onLoadProps,
}: {
  children: React.ReactNode;
  onLoadProps: ModalProps;
}) => {
  const [props, setProps] = useState<ModalProps>({
    ...defaultProps,
    ...onLoadProps,
  });
  const [query, setQuery] = useState("");
  const [results, setResults] = useState<
    ChunkWithHighlights[] | GroupChunk[][]
  >([]);
  const [requestID, setRequestID] = useState("");
  const [loadingResults, setLoadingResults] = useState(false);
  const [open, setOpen] = useState(props.open ?? false);
  const inputRef = useRef<HTMLInputElement>(null);
  const [mode, setMode] = useState(props.defaultSearchMode || "search");
  const modalRef = useRef<HTMLDivElement>(null);
  const [tagCounts, setTagCounts] = useState<CountChunkQueryResponseBody[]>([]);
  const [currentTag, setCurrentTag] = useState(
    props.tags?.find((t) => t.selected)?.tag || "all",
  );

  const [currentGroup, setCurrentGroup] = useState<ChunkGroup | null>(null);

  const trieve = new TrieveSDK({
    baseUrl: props.baseUrl,
    apiKey: props.apiKey,
    datasetId: props.datasetId,
  });

  const search = async (abortController: AbortController) => {
    if (!query) {
      setResults([]);
      return;
    }

    try {
      setLoadingResults(true);
      if (props.useGroupSearch) {
        const results = await groupSearchWithTrieve({
          query: query,
          searchOptions: props.searchOptions,
          trieve: trieve,
          abortController,
          ...(currentTag !== "all" && { tag: currentTag }),
          type: props.type,
        });

        const groupMap = new Map<string, GroupChunk[]>();
        results.groups.forEach((group) => {
          const title = group.chunks[0].chunk.metadata?.title;
          if (groupMap.has(title)) {
            groupMap.get(title)?.push(group);
          } else {
            groupMap.set(title, [group]);
          }
        });

        setResults(Array.from(groupMap.values()));
        setRequestID(results.requestID);
      } else {
        const results = await searchWithTrieve({
          query: query,
          searchOptions: props.searchOptions,
          trieve: trieve,
          abortController,
          ...(currentTag !== "all" && { tag: currentTag }),
          type: props.type,
        });
        setResults(results.chunks);
        setRequestID(results.requestID);
      }
    } catch (e) {
      if (
        e != "AbortError" &&
        e != "AbortError: signal is aborted without reason"
      ) {
        console.error(e);
      }
    } finally {
      setLoadingResults(false);
    }
  };

  const getTagCounts = async (abortController: AbortController) => {
    if (!query) {
      setTagCounts([]);
      return;
    }
    if (props.tags?.length) {
      try {
        const numberOfRecords = await Promise.all(
          [ALL_TAG, ...props.tags].map((tag) =>
            countChunks({
              query: query,
              trieve: trieve,
              abortController,
              ...(tag.tag !== "all" && { tag: tag.tag }),
            }),
          ),
        );
        setTagCounts(numberOfRecords);
      } catch (e) {
        if (
          e != "AbortError" &&
          e != "AbortError: signal is aborted without reason"
        ) {
          console.log(e);
          console.log(typeof e);
          console.error(e);
        }
      }
    }
  };

  const chatWithGroup = async (group: ChunkGroup, betterGroupName?: string) => {
    // TODO: normalize group name, using results
    if (betterGroupName) {
      group.name = betterGroupName;
    }

    setCurrentGroup(group);
    setMode("chat");
    // preload the chunk ids
    cached(() => {
      return getChunkIdsForGroup(group.id, trieve);
    }, `chunk-ids-${group.id}`).catch((e) => {
      console.error(e);
    });
  };

  useEffect(() => {
    setProps((p) => ({
      ...p,
      ...onLoadProps,
    }));
  }, [onLoadProps]);

  useEffect(() => {
    props.onOpenChange?.(open);
  }, [open]);

  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if (
        open &&
        e.ctrlKey &&
        e.key === "Tab" &&
        props.allowSwitchingModes !== false
      ) {
        setMode((prevMode) => (prevMode === "chat" ? "search" : "chat"));
      }
    };
    window.addEventListener("keydown", handleKeyDown);
    return () => {
      window.removeEventListener("keydown", handleKeyDown);
    };
  }, [open, props.allowSwitchingModes]);

  useEffect(() => {
    const abortController = new AbortController();

    setLoadingResults(true);
    const timeout = setTimeout(() => {
      search(abortController);
    }, props.debounceMs);

    return () => {
      clearTimeout(timeout);
      abortController.abort();
    };
  }, [query, currentTag]);

  useEffect(() => {
    const abortController = new AbortController();

    const timeout = setTimeout(() => {
      getTagCounts(abortController);
    }, props.debounceMs);

    return () => {
      clearTimeout(timeout);
      abortController.abort("AbortError");
    };
  }, [query]);

  return (
    <ModalContext.Provider
      value={{
        setContextProps: (props) =>
          setProps((p) => ({
            ...p,
            ...props,
          })),
        props,
        trieveSDK: trieve,
        query,
        setQuery,
        open,
        setOpen,
        inputRef,
        results,
        setResults,
        requestID,
        setRequestID,
        loadingResults,
        setLoadingResults,
        mode,
        setMode,
        modalRef,
        currentTag,
        setCurrentTag,
        currentGroup,
        setCurrentGroup,
        chatWithGroup,
        tagCounts,
      }}
    >
      {children}
    </ModalContext.Provider>
  );
};

function useModalState() {
  const context = useContext(ModalContext);
  if (!context) {
    throw new Error("useModalState must be used within a ModalProvider");
  }
  return context;
}

export { ModalProvider, useModalState };
