/* eslint-disable @typescript-eslint/unbound-method */
/* eslint-disable @typescript-eslint/no-unsafe-assignment */
/* eslint-disable @typescript-eslint/no-unsafe-call */
import { BiRegularSearch, BiRegularX } from "solid-icons/bi";
import {
  For,
  Match,
  Setter,
  Show,
  Switch,
  createEffect,
  createMemo,
  createSignal,
} from "solid-js";
import {
  Menu,
  MenuItem,
  Popover,
  PopoverButton,
  PopoverPanel,
} from "solid-headless";
import { FaRegularFlag, FaSolidCheck, FaSolidMicrophone } from "solid-icons/fa";
import { Filter, FilterItem } from "./FilterModal";
import { FiChevronDown, FiChevronUp } from "solid-icons/fi";
import {
  HighlightStrategy,
  isSortByField,
  isSortBySearchType,
  SearchOptions,
  SearchStore,
} from "../hooks/useSearch";
import { Tooltip } from "shared/ui";
import { BsQuestionCircle } from "solid-icons/bs";
import { AiOutlinePlus } from "solid-icons/ai";

const defaultFilter = {
  field: "",
};

const SearchForm = (props: {
  search: SearchStore;
  groupID?: string;
  openRateQueryModal: Setter<boolean>;
}) => {
  const bm25Active = import.meta.env.VITE_BM25_ACTIVE as unknown as string;
  const [tempSearchValues, setTempSearchValues] = createSignal(
    // eslint-disable-next-line solid/reactivity
    props.search.state,
  );
  const [tempFilterType, setTempFilterType] = createSignal<string>("must");
  const [mustFilters, setMustFilters] = createSignal<Filter[]>(
    // eslint-disable-next-line solid/reactivity
    tempSearchValues().filters?.must ?? [],
  );
  const [mustNotFilters, setMustNotFilters] = createSignal<Filter[]>(
    // eslint-disable-next-line solid/reactivity
    tempSearchValues().filters?.must_not ?? [],
  );
  const [shouldFilters, setShouldFilters] = createSignal<Filter[]>(
    // eslint-disable-next-line solid/reactivity
    tempSearchValues().filters?.should ?? [],
  );

  const [rerankQuery, setRerankQuery] = createSignal<string>("");

  const saveFilters = (setShowFilterModal: (filter: boolean) => void) => {
    const filters = {
      must: mustFilters(),
      must_not: mustNotFilters(),
      should: shouldFilters(),
    };
    props.search.setSearch("filters", filters);
    setShowFilterModal(false);
  };

  const default_settings = [
    { name: "Hybrid", isSelected: false, route: "hybrid" },
    {
      name: "FullText",
      isSelected: false,
      route: "fulltext",
    },
    {
      name: "Semantic",
      isSelected: false,
      route: "semantic",
    },
  ];

  if (bm25Active) {
    default_settings.push({
      name: "AutoComplete BM25",
      isSelected: false,
      route: "autocomplete-bm25",
    });
    default_settings.push({ name: "BM25", isSelected: false, route: "BM25" });
  }

  const [searchTypes, setSearchTypes] = createSignal(default_settings);
  const [sortTypes, setSortTypes] = createSignal([
    {
      name: "Timestamp",
      isSelected: false,
      value: "time_stamp",
    },
    {
      name: "Num Value",
      isSelected: false,
      value: "num_value",
    },
  ]);

  createEffect(() => {
    if (!props.search.state.groupUniqueSearch) {
      console.log("groupUniqueSearch is false");

      setSearchTypes((prev) => {
        return prev.concat([
          {
            name: "AutoComplete Semantic",
            isSelected: false,
            route: "autocomplete-semantic",
          },
          {
            name: "AutoComplete FullText",
            isSelected: false,
            route: "autocomplete-fulltext",
          },
        ]);
      });
    } else {
      console.log("groupUniqueSearch is true");
      setSearchTypes((prev) => {
        return prev.filter(
          (type) =>
            type.route !== "autocomplete-semantic" &&
            type.route !== "autocomplete-fulltext",
        );
      });
    }
  });

  const defaultRerankTypes = [
    {
      name: "Semantic",
      isSelected: false,
      value: "semantic",
    },
    {
      name: "FullText",
      isSelected: false,
      value: "fulltext",
    },
    {
      name: "Cross Encoder",
      isSelected: false,
      value: "cross_encoder",
    },
  ];

  if (bm25Active) {
    defaultRerankTypes.push({
      name: "BM25",
      isSelected: false,
      value: "bm25",
    });
  }
  const [rerankTypes, setRerankTypes] = createSignal(defaultRerankTypes);

  createEffect(() => {
    setSearchTypes((prev) => {
      return prev.map((type) => {
        if (type.route === props.search.state.searchType) {
          return { ...type, isSelected: true };
        } else {
          return { ...type, isSelected: false };
        }
      });
    });
  });

  createEffect(() => {
    setSortTypes((prev) => {
      return prev.map((type) => {
        if (isSortByField(props.search.state.sort_by)) {
          if (type.value === props.search.state.sort_by.field) {
            return { ...type, isSelected: true };
          } else {
            return { ...type, isSelected: false };
          }
        } else {
          return { ...type, isSelected: false };
        }
      });
    });
  });

  createEffect(() => {
    setRerankTypes((prev) => {
      return prev.map((type) => {
        if (isSortBySearchType(props.search.state.sort_by)) {
          if (type.value === props.search.state.sort_by.rerank_type) {
            return { ...type, isSelected: true };
          } else {
            return { ...type, isSelected: false };
          }
        } else {
          return { ...type, isSelected: false };
        }
      });
    });
    setRerankQuery(() => {
      if (isSortBySearchType(props.search.state.sort_by)) {
        return props.search.state.sort_by.rerank_query ?? "";
      } else {
        return "";
      }
    });
  });

  createEffect(() => {
    props.search.setSearch(
      "searchType",
      searchTypes().find((type) => type.isSelected)?.route ?? "hybrid",
    );
  });

  createEffect(() => {
    props.search.setSearch("sort_by", {
      field: sortTypes().find((type) => type.isSelected)?.value,
    });
  });

  createEffect(() => {
    props.search.setSearch("sort_by", {
      rerank_type: rerankTypes().find((type) => type.isSelected)?.value,
      rerank_query: rerankQuery() == "" ? undefined : rerankQuery(),
    });
  });

  const filtersLength = createMemo(() => {
    return (
      mustFilters().length + mustNotFilters().length + shouldFilters().length
    );
  });

  const [isRecording, setIsRecording] = createSignal(false);
  const [mediaRecorder, setMediaRecorder] = createSignal<MediaRecorder | null>(
    null,
  );

  const startRecording = async () => {
    try {
      const stream = await navigator.mediaDevices.getUserMedia({ audio: true });

      const recorder = new MediaRecorder(stream);
      const audioChunks: BlobPart[] = [];

      recorder.ondataavailable = (event) => {
        audioChunks.push(event.data);
      };

      recorder.onstop = () => {
        const audioBlob = new Blob(audioChunks, { type: "audio/mp3" });
        const reader = new FileReader();
        reader.readAsDataURL(audioBlob);
        reader.onloadend = () => {
          let base64data = reader.result as string;
          base64data = base64data?.split(",")[1];
          props.search.setSearch("audioBase64", base64data);
          props.search.setSearch("version", (prev) => prev + 1);
        };
      };

      setMediaRecorder(recorder);
      recorder.start();
      setIsRecording(true);
    } catch (err) {
      console.error("Error accessing microphone:", err);
      alert(
        "Error accessing microphone. Please make sure you have granted microphone permissions.",
      );
    }
  };

  const stopRecording = () => {
    if (mediaRecorder() && mediaRecorder()?.state !== "inactive") {
      mediaRecorder()?.stop();
      setIsRecording(false);
    }
  };

  const toggleRecording = () => {
    if (isRecording()) {
      stopRecording();
    } else {
      void startRecording();
    }
  };

  createEffect(() => {
    if (mustNotFilters().length > 0) {
      setTempFilterType("must not");
    } else if (shouldFilters().length > 0) {
      setTempFilterType("should");
    } else {
      setTempFilterType("must");
    }
  });

  return (
    <>
      <div class="w-full">
        <form class="w-full space-y-4 dark:text-white">
          <div class="relative flex">
            <Show when={props.search.state.multiQueries.length == 0}>
              <div
                classList={{
                  "flex w-full justify-center space-x-2 rounded-md bg-neutral-100 px-4 py-1 pr-[10px] dark:bg-neutral-700":
                    true,
                }}
              >
                <BiRegularSearch class="mt-1 h-6 w-6 fill-current" />
                <textarea
                  id="search-query-textarea"
                  classList={{
                    "scrollbar-track-rounded-md scrollbar-thumb-rounded-md mr-2 h-fit max-h-[240px] w-full resize-none whitespace-pre-wrap bg-transparent py-1 scrollbar-thin scrollbar-track-neutral-200 scrollbar-thumb-neutral-400 focus:outline-none dark:bg-neutral-700 dark:text-white dark:scrollbar-track-neutral-700 dark:scrollbar-thumb-neutral-600 text-wrap":
                      true,
                  }}
                  value={props.search.state.query}
                  onInput={(e) => {
                    props.search.setSearch("query", e.currentTarget.value);

                    e.currentTarget.style.height = "auto";
                    e.currentTarget.style.height =
                      e.currentTarget.scrollHeight + "px";

                    const searchTextarea = document.getElementById(
                      "search-query-textarea",
                    );
                    searchTextarea?.focus();
                    setTimeout(() => {
                      searchTextarea?.focus();
                    }, 50);
                    setTimeout(() => {
                      searchTextarea?.focus();
                    }, 100);
                    setTimeout(() => {
                      searchTextarea?.focus();
                    }, 200);
                    setTimeout(() => {
                      searchTextarea?.focus();
                    }, 300);
                    setTimeout(() => {
                      searchTextarea?.focus();
                    }, 400);
                    setTimeout(() => {
                      searchTextarea?.focus();
                    }, 500);
                  }}
                  onKeyDown={(e) => {
                    if (
                      ((e.ctrlKey || e.metaKey) && e.key === "Enter") ||
                      (!e.shiftKey && e.key === "Enter")
                    ) {
                      props.search.setSearch("version", (prev) => prev + 1);
                      e.preventDefault();
                      e.stopPropagation();
                    }
                  }}
                  placeholder="Search for chunks..."
                  rows={props.search.state.query.split("\n").length}
                />
                <button
                  classList={{
                    "pt-[2px]": !!props.search.state.query,
                  }}
                  onClick={(e) => {
                    e.preventDefault();
                    props.search.setSearch("query", "");
                  }}
                >
                  <BiRegularX class="h-7 w-7 fill-current" />
                </button>
                <button
                  classList={{
                    "border-l border-neutral-600 pl-[10px] dark:border-neutral-200":
                      !!props.search.state.query,
                  }}
                  type="submit"
                >
                  <BiRegularSearch class="mt-1 h-6 w-6 fill-current" />
                </button>
                <button
                  type="button"
                  classList={{
                    "border-l border-neutral-600 pl-[10px] dark:border-neutral-200 cursor-pointer pb-1":
                      true,
                    "text-red-500": isRecording(),
                  }}
                  onClick={(e) => {
                    e.preventDefault();
                    toggleRecording();
                  }}
                >
                  <FaSolidMicrophone class="mt-1 h-5 w-5 fill-current" />
                </button>
              </div>
            </Show>
            <Show when={props.search.state.multiQueries.length > 0}>
              <div class="flex w-full flex-col space-y-2">
                <For each={props.search.state.multiQueries}>
                  {(multiQuery, index) => (
                    <div
                      classList={{
                        "flex w-full justify-center space-x-2 rounded-md bg-neutral-100 px-4 py-1 pr-[10px] dark:bg-neutral-700":
                          true,
                      }}
                    >
                      <BiRegularSearch class="mt-1 h-6 w-6 fill-current" />
                      <textarea
                        id={`search-query-textarea-${index()}`}
                        classList={{
                          "scrollbar-track-rounded-md scrollbar-thumb-rounded-md mr-2 h-fit max-h-[240px] w-full resize-none whitespace-pre-wrap bg-transparent py-1 scrollbar-thin scrollbar-track-neutral-200 scrollbar-thumb-neutral-400 focus:outline-none dark:bg-neutral-700 dark:text-white dark:scrollbar-track-neutral-700 dark:scrollbar-thumb-neutral-600 text-wrap":
                            true,
                        }}
                        value={multiQuery.query}
                        onInput={(e) => {
                          props.search.setSearch(
                            "multiQueries",
                            props.search.state.multiQueries.map((query) => {
                              if (query === multiQuery) {
                                return {
                                  ...query,
                                  query: e.currentTarget.value,
                                };
                              } else {
                                return query;
                              }
                            }),
                          );

                          const searchTextarea = document.getElementById(
                            `search-query-textarea-${index()}`,
                          );

                          searchTextarea?.focus();
                          setTimeout(() => {
                            searchTextarea?.focus();
                          }, 50);
                          setTimeout(() => {
                            searchTextarea?.focus();
                          }, 100);
                          setTimeout(() => {
                            searchTextarea?.focus();
                          }, 200);
                          setTimeout(() => {
                            searchTextarea?.focus();
                          }, 300);
                          setTimeout(() => {
                            searchTextarea?.focus();
                          }, 400);
                          setTimeout(() => {
                            searchTextarea?.focus();
                          }, 500);
                          e.currentTarget.style.height = "auto";
                          e.currentTarget.style.height =
                            e.currentTarget.scrollHeight + "px";
                        }}
                        onKeyDown={(e) => {
                          if (
                            ((e.ctrlKey || e.metaKey) && e.key === "Enter") ||
                            (!e.shiftKey && e.key === "Enter")
                          ) {
                            props.search.setSearch(
                              "version",
                              (prev) => prev + 1,
                            );
                            e.preventDefault();
                            e.stopPropagation();
                          }
                        }}
                        placeholder="Search for chunks..."
                        rows={props.search.state.query.split("\n").length}
                      />
                      <input
                        id={`search-query-weight-${index()}`}
                        type="number"
                        inputmode="decimal"
                        step="0.1"
                        min="0"
                        classList={{
                          "scrollbar-track-rounded-md scrollbar-thumb-rounded-md h-fit max-h-[240px] max-w-[10vw] resize-none whitespace-pre-wrap bg-transparent py-1 scrollbar-thin scrollbar-track-neutral-200 scrollbar-thumb-neutral-400 focus:outline-none dark:bg-neutral-700 dark:text-white dark:scrollbar-track-neutral-700 dark:scrollbar-thumb-neutral-600 text-wrap border-l border-neutral-600 pl-2":
                            true,
                        }}
                        value={multiQuery.weight}
                        onChange={(e) => {
                          props.search.setSearch(
                            "multiQueries",
                            props.search.state.multiQueries.map((query) => {
                              if (query === multiQuery) {
                                return {
                                  ...query,
                                  weight: parseFloat(e.currentTarget.value),
                                };
                              } else {
                                return query;
                              }
                            }),
                          );

                          const searchTextarea = document.getElementById(
                            `search-query-weight-${index()}`,
                          );

                          searchTextarea?.focus();
                          setTimeout(() => {
                            searchTextarea?.focus();
                          }, 50);
                          setTimeout(() => {
                            searchTextarea?.focus();
                          }, 100);
                          setTimeout(() => {
                            searchTextarea?.focus();
                          }, 200);
                          setTimeout(() => {
                            searchTextarea?.focus();
                          }, 300);
                          setTimeout(() => {
                            searchTextarea?.focus();
                          }, 400);
                          setTimeout(() => {
                            searchTextarea?.focus();
                          }, 500);
                          e.currentTarget.style.height = "auto";
                          e.currentTarget.style.height =
                            e.currentTarget.scrollHeight + "px";
                        }}
                        onKeyDown={(e) => {
                          if (
                            ((e.ctrlKey || e.metaKey) && e.key === "Enter") ||
                            (!e.shiftKey && e.key === "Enter")
                          ) {
                            props.search.setSearch(
                              "version",
                              (prev) => prev + 1,
                            );
                            e.preventDefault();
                            e.stopPropagation();
                          }
                        }}
                        placeholder="Add weight..."
                      />
                      <button
                        classList={{
                          "pt-[2px]": true,
                        }}
                        onClick={(e) => {
                          e.preventDefault();
                          props.search.setSearch(
                            "multiQueries",
                            props.search.state.multiQueries.filter(
                              (query) => query !== multiQuery,
                            ),
                          );
                        }}
                      >
                        <BiRegularX class="h-7 w-7 fill-current" />
                      </button>
                    </div>
                  )}
                </For>
              </div>
            </Show>
          </div>
          <Show when={props.search.state.multiQueries.length > 0}>
            <div class="flex items-center justify-self-end">
              <div class="flex-1" />
              <button
                class="flex w-fit items-center rounded bg-neutral-100 p-1 text-sm hover:bg-neutral-100 dark:bg-neutral-700 dark:hover:bg-neutral-800"
                onClick={(e) => {
                  e.preventDefault();
                  props.search.setSearch("multiQueries", [
                    ...props.search.state.multiQueries,
                    {
                      query: "",
                      weight: 1,
                    },
                  ]);
                }}
              >
                <AiOutlinePlus class="mr-2" />
                <span>Add Query</span>
              </button>
            </div>
          </Show>
          <div class="flex flex-wrap space-x-3">
            <Popover
              defaultOpen={false}
              class="relative"
              onClose={() => {
                saveFilters(() => {});
              }}
            >
              {({ isOpen, setState }) => (
                <>
                  <PopoverButton
                    aria-label="Toggle filters"
                    type="button"
                    class="flex items-center space-x-1 pb-1 text-sm"
                  >
                    <Tooltip
                      body={
                        <div
                          classList={{
                            "rounded-full w-3 h-3 text-[8px] text-center leading-[10px]":
                              true,
                            "bg-fuchsia-500 text-white": filtersLength() > 0,
                            "bg-neutral-100 text-neutral-500":
                              filtersLength() === 0,
                          }}
                        >
                          {filtersLength()}
                        </div>
                      }
                      tooltipText={`${filtersLength()} filter(s) applied`}
                    />
                    <span>Filters</span>
                    <Switch>
                      <Match when={isOpen()}>
                        <FiChevronUp class="h-3.5 w-3.5" />
                      </Match>
                      <Match when={!isOpen()}>
                        <FiChevronDown class="h-3.5 w-3.5" />
                      </Match>
                    </Switch>
                  </PopoverButton>
                  <Show when={isOpen()}>
                    <PopoverPanel
                      tabIndex={0}
                      unmount={false}
                      class="absolute z-10 mt-2 h-fit w-fit rounded-md bg-neutral-200 p-1 shadow-lg dark:bg-neutral-700"
                    >
                      <div class="flex max-h-[50vh] min-w-[70vw] max-w-[75vw] flex-col space-y-2 overflow-auto px-2 pr-2 scrollbar-thin scrollbar-track-neutral-200 scrollbar-thumb-neutral-400 scrollbar-thumb-rounded-md dark:text-white dark:scrollbar-track-neutral-800 dark:scrollbar-thumb-neutral-600 xl:min-w-[50vw] 2xl:min-w-[40vw]">
                        <div class="flex w-full items-center space-x-2 border-b border-neutral-400 py-2 dark:border-neutral-900">
                          <label aria-label="Change Filter Type">
                            <span class="p-1">Filter Type:</span>
                          </label>
                          <select
                            class="h-fit rounded-md border border-neutral-400 bg-neutral-100 p-1 dark:border-neutral-900 dark:bg-neutral-800"
                            onChange={(e) => {
                              setTempFilterType(e.currentTarget.value);
                            }}
                            value={tempFilterType()}
                          >
                            <For each={["must", "must not", "should"]}>
                              {(filter_type) => {
                                return (
                                  <option
                                    classList={{
                                      "flex w-full items-center justify-between rounded p-1":
                                        true,
                                      "bg-neutral-300 dark:bg-neutral-900":
                                        filter_type === tempFilterType(),
                                    }}
                                  >
                                    {filter_type}
                                  </option>
                                );
                              }}
                            </For>
                          </select>
                          <button
                            type="button"
                            class="rounded-md border border-neutral-400 bg-neutral-100 p-1 dark:border-neutral-900 dark:bg-neutral-800"
                            onClick={() => {
                              const curFilterType = tempFilterType();
                              if (curFilterType === "must") {
                                setMustFilters([
                                  ...mustFilters(),
                                  defaultFilter,
                                ]);
                              }
                              if (curFilterType === "must not") {
                                setMustNotFilters([
                                  ...mustNotFilters(),
                                  defaultFilter,
                                ]);
                              }
                              if (curFilterType === "should") {
                                setShouldFilters([
                                  ...shouldFilters(),
                                  defaultFilter,
                                ]);
                              }
                            }}
                          >
                            + Add Filter
                          </button>
                          <div class="flex-1" />
                          <button
                            type="button"
                            class="rounded-md border border-neutral-400 bg-neutral-100 p-1 dark:border-neutral-900 dark:bg-neutral-800"
                            onClick={() => {
                              setMustFilters([]);
                              setMustNotFilters([]);
                              setShouldFilters([]);
                            }}
                          >
                            Reset Filters
                          </button>
                          <button
                            type="button"
                            class="rounded-md border border-neutral-400 bg-neutral-100 p-1 dark:border-neutral-900 dark:bg-neutral-800"
                            onClick={() => saveFilters(setState)}
                          >
                            Apply Filters
                          </button>
                        </div>
                        <Show when={mustFilters().length > 0}>
                          <div class="border-b border-neutral-400 py-2 dark:border-neutral-900">
                            must: [
                            <div class="flex flex-col gap-y-2">
                              <For each={mustFilters()}>
                                {(filter, index) => {
                                  const onFilterChange = (
                                    newFilter: Filter,
                                  ) => {
                                    const newFilters = mustFilters();
                                    newFilters[index()] = newFilter;
                                    setMustFilters(newFilters);
                                  };

                                  return (
                                    <div
                                      classList={{
                                        "border-b border-dotted border-neutral-400 dark:border-neutral-900":
                                          index() < mustFilters().length - 1,
                                      }}
                                    >
                                      <FilterItem
                                        initialFilter={filter}
                                        onFilterChange={onFilterChange}
                                      />
                                    </div>
                                  );
                                }}
                              </For>
                            </div>
                            ]
                          </div>
                        </Show>
                        <Show when={mustNotFilters().length > 0}>
                          <div class="border-b border-neutral-400 py-2 dark:border-neutral-900">
                            must not: [
                            <div class="flex flex-col gap-y-2">
                              <For each={mustNotFilters()}>
                                {(filter, index) => {
                                  const onFilterChange = (
                                    newFilter: Filter,
                                  ) => {
                                    const newFilters = mustNotFilters();
                                    newFilters[index()] = newFilter;
                                    setMustNotFilters(newFilters);
                                  };

                                  return (
                                    <div
                                      classList={{
                                        "border-b border-dotted border-neutral-400 dark:border-neutral-900":
                                          index() < mustNotFilters().length - 1,
                                      }}
                                    >
                                      <FilterItem
                                        initialFilter={filter}
                                        onFilterChange={onFilterChange}
                                      />
                                    </div>
                                  );
                                }}
                              </For>
                            </div>
                            ]
                          </div>
                        </Show>
                        <Show when={shouldFilters().length > 0}>
                          <div class="border-b border-neutral-400 py-2 dark:border-neutral-900">
                            should: [
                            <div class="flex flex-col gap-y-2">
                              <For each={shouldFilters()}>
                                {(filter, index) => {
                                  const onFilterChange = (
                                    newFilter: Filter,
                                  ) => {
                                    const newFilters = shouldFilters();
                                    newFilters[index()] = newFilter;
                                    setShouldFilters(newFilters);
                                  };

                                  return (
                                    <div
                                      classList={{
                                        "border-b border-dotted border-neutral-400 dark:border-neutral-900":
                                          index() < shouldFilters().length - 1,
                                      }}
                                    >
                                      <FilterItem
                                        initialFilter={filter}
                                        onFilterChange={onFilterChange}
                                      />
                                    </div>
                                  );
                                }}
                              </For>
                            </div>
                            ]
                          </div>
                        </Show>
                      </div>
                    </PopoverPanel>
                  </Show>
                </>
              )}
            </Popover>
            <Popover defaultOpen={false} class="relative">
              {({ isOpen, setState }) => (
                <>
                  <PopoverButton
                    aria-label="Toggle filters"
                    type="button"
                    class="flex items-center space-x-1 pb-1 text-sm"
                  >
                    <span>
                      Type:{" "}
                      {searchTypes().find((type) => type.isSelected)?.name ??
                        "Hybrid"}
                    </span>
                    <Switch>
                      <Match when={isOpen()}>
                        <FiChevronUp class="h-3.5 w-3.5" />
                      </Match>
                      <Match when={!isOpen()}>
                        <FiChevronDown class="h-3.5 w-3.5" />
                      </Match>
                    </Switch>
                  </PopoverButton>
                  <Show when={isOpen()}>
                    <PopoverPanel
                      unmount={false}
                      class="absolute z-10 mt-2 h-fit w-[180px] rounded-md bg-neutral-200 p-1 shadow-lg dark:bg-neutral-800"
                    >
                      <Menu class="ml-1 space-y-1">
                        <For each={searchTypes()}>
                          {(option) => {
                            if (props.groupID && option.route === "hybrid") {
                              return <></>;
                            }

                            const onClick = (e: Event) => {
                              e.preventDefault();
                              e.stopPropagation();
                              setSearchTypes((prev) => {
                                return prev.map((item) => {
                                  if (item.name === option.name) {
                                    return { ...item, isSelected: true };
                                  } else {
                                    return { ...item, isSelected: false };
                                  }
                                });
                              });
                              setState(true);
                            };
                            return (
                              <MenuItem
                                as="button"
                                classList={{
                                  "flex w-full items-center justify-between rounded p-1 focus:text-black focus:outline-none dark:hover:text-white dark:focus:text-white":
                                    true,
                                  "bg-neutral-300 dark:bg-neutral-900":
                                    option.isSelected ||
                                    (option.route == "hybrid" &&
                                      searchTypes().find(
                                        (type) => type.isSelected,
                                      )?.name == undefined),
                                }}
                                onClick={onClick}
                              >
                                <div class="flex flex-row justify-start space-x-2">
                                  <span class="text-left">{option.name}</span>
                                </div>
                                {(option.isSelected ||
                                  (option.route == "hybrid" &&
                                    searchTypes().find(
                                      (type) => type.isSelected,
                                    )?.name == undefined)) && (
                                  <span>
                                    <FaSolidCheck class="fill-current text-xl" />
                                  </span>
                                )}
                              </MenuItem>
                            );
                          }}
                        </For>
                      </Menu>
                    </PopoverPanel>
                  </Show>
                </>
              )}
            </Popover>
            <Popover defaultOpen={false} class="relative">
              {({ isOpen, setState }) => (
                <>
                  <PopoverButton
                    aria-label="Toggle filters"
                    type="button"
                    class="flex items-center space-x-1 pb-1 text-sm"
                  >
                    <span>Sort</span>
                    <Switch>
                      <Match when={isOpen()}>
                        <FiChevronUp class="h-3.5 w-3.5" />
                      </Match>
                      <Match when={!isOpen()}>
                        <FiChevronDown class="h-3.5 w-3.5" />
                      </Match>
                    </Switch>
                  </PopoverButton>
                  <Show when={isOpen()}>
                    <PopoverPanel
                      unmount={false}
                      class="absolute z-10 mt-2 h-fit w-[180px] rounded-md bg-neutral-200 p-1 shadow-lg dark:bg-neutral-800"
                    >
                      <Menu class="ml-1 space-y-1">
                        <For each={sortTypes()}>
                          {(option) => {
                            const onClick = (e: Event) => {
                              e.preventDefault();
                              e.stopPropagation();
                              setSortTypes((prev) => {
                                return prev.map((item) => {
                                  if (item.name === option.name) {
                                    return {
                                      ...item,
                                      isSelected: !item.isSelected,
                                    };
                                  } else {
                                    return {
                                      ...item,
                                      isSelected: false,
                                    };
                                  }
                                });
                              });
                              setState(true);
                            };
                            return (
                              <MenuItem
                                as="button"
                                classList={{
                                  "flex w-full items-center justify-between rounded p-1 focus:text-black focus:outline-none dark:hover:text-white dark:focus:text-white":
                                    true,
                                  "bg-neutral-300 dark:bg-neutral-900":
                                    option.isSelected,
                                }}
                                onClick={onClick}
                              >
                                <div class="flex flex-row justify-start space-x-2">
                                  <span class="text-left">{option.name}</span>
                                </div>
                                {option.isSelected && (
                                  <span>
                                    <FaSolidCheck class="fill-current text-xl" />
                                  </span>
                                )}
                              </MenuItem>
                            );
                          }}
                        </For>
                      </Menu>
                    </PopoverPanel>
                  </Show>
                </>
              )}
            </Popover>
            <Popover defaultOpen={false} class="relative">
              {({ isOpen, setState }) => (
                <>
                  <PopoverButton
                    aria-label="Toggle filters"
                    type="button"
                    class="flex items-center space-x-1 pb-1 text-sm"
                  >
                    <span>Rerank By</span>
                    <Switch>
                      <Match when={isOpen()}>
                        <FiChevronUp class="h-3.5 w-3.5" />
                      </Match>
                      <Match when={!isOpen()}>
                        <FiChevronDown class="h-3.5 w-3.5" />
                      </Match>
                    </Switch>
                  </PopoverButton>
                  <Show when={isOpen()}>
                    <PopoverPanel
                      unmount={false}
                      class="absolute z-10 mt-2 h-fit w-[180px] rounded-md bg-neutral-200 p-1 shadow-lg dark:bg-neutral-800"
                    >
                      <Menu class="ml-1 space-y-1">
                        <input
                          type="text"
                          class="max-w-[165px] rounded-md border border-neutral-400 bg-neutral-100 p-1 dark:border-neutral-900 dark:bg-neutral-800"
                          placeholder="Rerank Query"
                          onChange={(e) => {
                            setRerankQuery(e.currentTarget.value);
                          }}
                          value={rerankQuery()}
                        />
                        <For each={rerankTypes()}>
                          {(option) => {
                            const onClick = (e: Event) => {
                              e.preventDefault();
                              e.stopPropagation();
                              setRerankTypes((prev) => {
                                return prev.map((item) => {
                                  if (item.name === option.name) {
                                    return {
                                      ...item,
                                      isSelected: !item.isSelected,
                                    };
                                  } else {
                                    return {
                                      ...item,
                                      isSelected: false,
                                    };
                                  }
                                });
                              });
                              setState(true);
                            };
                            return (
                              <MenuItem
                                as="button"
                                classList={{
                                  "flex w-full items-center justify-between rounded p-1 focus:text-black focus:outline-none dark:hover:text-white dark:focus:text-white":
                                    true,
                                  "bg-neutral-300 dark:bg-neutral-900":
                                    option.isSelected,
                                }}
                                onClick={onClick}
                              >
                                <div class="flex flex-row justify-start space-x-2">
                                  <span class="text-left">{option.name}</span>
                                </div>
                                {option.isSelected && (
                                  <span>
                                    <FaSolidCheck class="fill-current text-xl" />
                                  </span>
                                )}
                              </MenuItem>
                            );
                          }}
                        </For>
                      </Menu>
                    </PopoverPanel>
                  </Show>
                </>
              )}
            </Popover>
            <Popover
              defaultOpen={false}
              class="relative"
              onClose={() => {
                const newSearchValues = tempSearchValues();
                newSearchValues.version += 1;
                newSearchValues.sort_by = props.search.state.sort_by;
                newSearchValues.searchType = props.search.state.searchType;
                newSearchValues.groupUniqueSearch =
                  props.search.state.groupUniqueSearch;
                newSearchValues.query = props.search.state.query;

                props.search.setSearch(newSearchValues);

                const searchTextarea = document.getElementById(
                  "search-query-textarea",
                );

                searchTextarea?.focus();
                setTimeout(() => {
                  searchTextarea?.focus();
                }, 50);
                setTimeout(() => {
                  searchTextarea?.focus();
                }, 100);
                setTimeout(() => {
                  searchTextarea?.focus();
                }, 200);
                setTimeout(() => {
                  searchTextarea?.focus();
                }, 300);
                setTimeout(() => {
                  searchTextarea?.focus();
                }, 400);
                setTimeout(() => {
                  searchTextarea?.focus();
                }, 500);
              }}
            >
              {({ isOpen }) => (
                <>
                  <PopoverButton
                    aria-label="Toggle options"
                    type="button"
                    class="flex items-center space-x-1 pb-1 text-sm"
                  >
                    <span>Options</span>
                    <Switch>
                      <Match when={isOpen()}>
                        <FiChevronUp class="h-3.5 w-3.5" />
                      </Match>
                      <Match when={!isOpen()}>
                        <FiChevronDown class="h-3.5 w-3.5" />
                      </Match>
                    </Switch>
                  </PopoverButton>
                  <Show when={isOpen()}>
                    <PopoverPanel
                      unmount={false}
                      tabIndex={0}
                      class="absolute z-10 mt-2 h-fit w-[80vw] rounded-md bg-neutral-200 p-1 shadow-lg dark:bg-neutral-700 sm:w-[300px] md:w-[400px]"
                    >
                      <div class="items flex flex-col space-y-1">
                        <div class="mt-1">
                          <button
                            class="rounded-md border border-neutral-400 bg-neutral-100 px-2 py-1 dark:border-neutral-900 dark:bg-neutral-800"
                            onClick={(e) => {
                              e.preventDefault();
                              setTempSearchValues((prev) => {
                                return {
                                  ...props.search.state,
                                  ...prev,
                                  scoreThreshold: 0.0,
                                  extendResults: false,
                                  slimChunks: false,
                                  sort_by: {
                                    field: "",
                                  },
                                  pageSize: 10,
                                  getTotalPages: true,
                                  highlightStrategy: "exactmatch",
                                  correctTypos: false,
                                  oneTypoWordRangeMin: 5,
                                  oneTypoWordRangeMax: 8,
                                  twoTypoWordRangeMin: 8,
                                  twoTypoWordRangeMax: null,
                                  disableOnWords: [],
                                  typoTolerance: false,
                                  prioritize_domain_specifc_words: true,
                                  highlightResults: true,
                                  highlightDelimiters: ["?", ".", "!"],
                                  highlightMaxLength: 8,
                                  highlightMaxNum: 3,
                                  highlightWindow: 0,
                                  highlightPreTag: "<mark><b>",
                                  highlightPostTag: "</b></mark>",
                                  group_size: 3,
                                  removeStopWords: false,
                                } as SearchOptions;
                              });
                            }}
                          >
                            Reset
                          </button>
                        </div>
                        <div class="px-1 pt-2 font-bold">General</div>
                        <div class="flex items-center justify-between space-x-2 p-1">
                          <label>Score Threshold (0.0 to 1.0):</label>
                          <input
                            class="w-16 rounded border border-neutral-400 p-0.5 text-black"
                            type="number"
                            step="any"
                            value={tempSearchValues().scoreThreshold}
                            onChange={(e) => {
                              setTempSearchValues((prev) => {
                                return {
                                  ...prev,
                                  scoreThreshold: parseFloat(
                                    e.currentTarget.value,
                                  ),
                                };
                              });
                            }}
                          />
                        </div>
                        <div class="flex items-center justify-between space-x-2 p-1">
                          <label>Page Size:</label>
                          <input
                            class="w-16 rounded border border-neutral-400 p-0.5 text-black"
                            type="number"
                            value={tempSearchValues().pageSize}
                            onChange={(e) => {
                              setTempSearchValues((prev) => {
                                return {
                                  ...prev,
                                  pageSize: parseInt(e.currentTarget.value),
                                };
                              });
                            }}
                          />
                        </div>
                        <Show
                          when={
                            searchTypes().find((type) => type.isSelected)
                              ?.route === "autocomplete-semantic" ||
                            searchTypes().find((type) => type.isSelected)
                              ?.route === "autocomplete-fulltext" ||
                            searchTypes().find((type) => type.isSelected)
                              ?.route === "autocomplete-bm25"
                          }
                        >
                          <div class="flex items-center justify-between space-x-2 p-1">
                            <label>Extend Results (autocomplete only):</label>
                            <input
                              class="h-4 w-4"
                              type="checkbox"
                              checked={tempSearchValues().extendResults}
                              onChange={(e) => {
                                setTempSearchValues((prev) => {
                                  return {
                                    ...prev,
                                    extendResults: e.target.checked,
                                  };
                                });
                              }}
                            />
                          </div>
                        </Show>
                        <div class="px-1 font-bold">Performance</div>
                        <div class="flex items-center justify-between space-x-2 p-1">
                          <label>Slim Chunks (Latency Improvement):</label>
                          <input
                            class="h-4 w-4"
                            type="checkbox"
                            checked={tempSearchValues().slimChunks}
                            onChange={(e) => {
                              setTempSearchValues((prev) => {
                                return {
                                  ...prev,
                                  slimChunks: e.target.checked,
                                };
                              });
                            }}
                          />
                        </div>
                        <div class="flex items-center justify-between space-x-2 p-1">
                          <label>Get Total Pages (Latency Penalty):</label>
                          <input
                            class="h-4 w-4"
                            type="checkbox"
                            checked={tempSearchValues().getTotalPages}
                            onChange={(e) => {
                              setTempSearchValues((prev) => {
                                return {
                                  ...prev,
                                  getTotalPages: e.target.checked,
                                };
                              });
                            }}
                          />
                        </div>
                        <div class="flex items-center justify-between space-x-2 p-1">
                          <label>Use MMR:</label>
                          <input
                            class="h-4 w-4"
                            type="checkbox"
                            checked={tempSearchValues().mmr.use_mmr}
                            onChange={(e) => {
                              setTempSearchValues((prev) => {
                                return {
                                  ...prev,
                                  mmr: {
                                    ...prev.mmr,
                                    use_mmr: e.target.checked,
                                  },
                                };
                              });
                            }}
                          />
                        </div>
                        <div class="flex items-center justify-between space-x-2 p-1">
                          <label>MMR Lambda:</label>
                          <input
                            class="w-16 rounded border border-neutral-400 p-0.5 text-black"
                            type="number"
                            value={tempSearchValues().mmr.mmr_lambda}
                            onChange={(e) => {
                              setTempSearchValues((prev) => {
                                return {
                                  ...prev,
                                  mmr: {
                                    ...prev.mmr,
                                    mmr_lambda: parseFloat(
                                      e.currentTarget.value,
                                    ),
                                  },
                                };
                              });
                            }}
                          />
                        </div>
                        <div class="px-1 font-bold">Search Refinement</div>
                        <div class="flex items-center justify-between space-x-2 p-1">
                          <label>Use Quote Negated Words:</label>
                          <input
                            class="h-4 w-4"
                            type="checkbox"
                            checked={tempSearchValues().useQuoteNegatedTerms}
                            onChange={(e) => {
                              setTempSearchValues((prev) => {
                                return {
                                  ...prev,
                                  useQuoteNegatedTerms: e.target.checked,
                                };
                              });
                            }}
                          />
                        </div>
                        <div class="flex items-center justify-between space-x-2 p-1">
                          <label>Remove Stop Words:</label>
                          <input
                            class="h-4 w-4"
                            type="checkbox"
                            checked={tempSearchValues().removeStopWords}
                            onChange={(e) => {
                              setTempSearchValues((prev) => {
                                return {
                                  ...prev,
                                  removeStopWords: e.target.checked,
                                };
                              });
                            }}
                          />
                        </div>
                        <div class="px-1 font-bold">Typo Tolerance</div>

                        <div class="flex items-center justify-between space-x-2 p-1">
                          <label>Typo Tolerance (Latency Penalty):</label>
                          <input
                            class="h-4 w-4"
                            type="checkbox"
                            checked={tempSearchValues().correctTypos}
                            onChange={(e) => {
                              setTempSearchValues((prev) => {
                                return {
                                  ...prev,
                                  correctTypos: e.target.checked,
                                };
                              });
                            }}
                          />
                        </div>
                        <div class="flex items-center justify-between space-x-2 p-1">
                          <label>Auto-require Domain Keywords</label>
                          <input
                            class="h-4 w-4"
                            type="checkbox"
                            checked={
                              tempSearchValues()
                                .prioritize_domain_specifc_words ?? false
                            }
                            onChange={(e) => {
                              setTempSearchValues((prev) => {
                                return {
                                  ...prev,
                                  prioritize_domain_specifc_words:
                                    e.target.checked,
                                };
                              });
                            }}
                          />
                        </div>
                        <div class="items flex justify-between space-x-2 p-1">
                          <label>One typo min word length:</label>
                          <input
                            class="w-16 rounded border border-neutral-400 p-0.5 text-black"
                            type="number"
                            step="any"
                            value={tempSearchValues().oneTypoWordRangeMin}
                            onChange={(e) => {
                              setTempSearchValues((prev) => {
                                return {
                                  ...prev,
                                  oneTypoWordRangeMin: parseInt(
                                    e.currentTarget.value,
                                  ),
                                };
                              });
                            }}
                          />
                        </div>
                        <div class="items flex justify-between space-x-2 p-1">
                          <label>One typo max word length:</label>
                          <input
                            class="w-16 rounded border border-neutral-400 p-0.5 text-black"
                            type="number"
                            step="any"
                            value={
                              tempSearchValues().oneTypoWordRangeMax?.toString() ??
                              ""
                            }
                            onChange={(e) => {
                              setTempSearchValues((prev) => {
                                return {
                                  ...prev,
                                  oneTypoWordRangeMax:
                                    e.currentTarget.value === ""
                                      ? null
                                      : parseInt(e.currentTarget.value),
                                };
                              });
                            }}
                          />
                        </div>
                        <div class="items flex justify-between space-x-2 p-1">
                          <label>Two typo min word length:</label>
                          <input
                            class="w-16 rounded border border-neutral-400 p-0.5 text-black"
                            type="number"
                            step="any"
                            value={tempSearchValues().twoTypoWordRangeMin}
                            onChange={(e) => {
                              setTempSearchValues((prev) => {
                                return {
                                  ...prev,
                                  twoTypoWordRangeMin: parseInt(
                                    e.currentTarget.value,
                                  ),
                                };
                              });
                            }}
                          />
                        </div>
                        <div class="items flex justify-between space-x-2 p-1">
                          <label>Two typo max word length:</label>
                          <input
                            class="w-16 rounded border border-neutral-400 p-0.5 text-black"
                            type="number"
                            step="any"
                            value={
                              tempSearchValues().twoTypoWordRangeMax?.toString() ??
                              ""
                            }
                            onChange={(e) => {
                              setTempSearchValues((prev) => {
                                return {
                                  ...prev,
                                  oneTypoWordRangeMax:
                                    e.currentTarget.value === ""
                                      ? null
                                      : parseInt(e.currentTarget.value),
                                };
                              });
                            }}
                          />
                        </div>
                        <div class="items flex justify-between space-x-2 p-1">
                          <label>Disable typo tolerance for words:</label>
                          <input
                            class="w-16 rounded border border-neutral-400 p-0.5 text-black"
                            type="text"
                            value={tempSearchValues().disableOnWords.join(",")}
                            onInput={(e) => {
                              if (e.currentTarget.value === " ") {
                                setTempSearchValues((prev) => {
                                  return {
                                    ...prev,
                                    disableOnWords: [" "],
                                  };
                                });
                              }

                              setTempSearchValues((prev) => {
                                return {
                                  ...prev,
                                  disableOnWords:
                                    e.currentTarget.value.split(","),
                                };
                              });
                            }}
                          />
                        </div>
                        <div class="px-1 font-bold">Highlighting</div>
                        <div class="flex items-center justify-between space-x-2 p-1">
                          <label>Highlight Results (Latency Penalty):</label>
                          <input
                            class="h-4 w-4"
                            type="checkbox"
                            checked={tempSearchValues().highlightResults}
                            onChange={(e) => {
                              setTempSearchValues((prev) => {
                                return {
                                  ...prev,
                                  highlightResults: e.target.checked,
                                };
                              });
                            }}
                          />
                        </div>
                        <div class="items flex justify-between space-x-2 p-1">
                          <label>Highlight Threshold:</label>
                          <input
                            class="w-16 rounded border border-neutral-400 p-0.5 text-black"
                            type="number"
                            step="any"
                            value={tempSearchValues().highlightThreshold}
                            onChange={(e) => {
                              setTempSearchValues((prev) => {
                                return {
                                  ...prev,
                                  highlightThreshold: parseFloat(
                                    e.currentTarget.value,
                                  ),
                                };
                              });
                            }}
                          />
                        </div>
                        <div class="items flex justify-between space-x-2 p-1">
                          <label>Highlight Delimiters (Comma Separated):</label>
                          <input
                            class="w-16 rounded border border-neutral-400 p-0.5 text-black"
                            type="text"
                            value={tempSearchValues().highlightDelimiters.join(
                              ",",
                            )}
                            onInput={(e) => {
                              if (e.currentTarget.value === " ") {
                                setTempSearchValues((prev) => {
                                  return {
                                    ...prev,
                                    highlightDelimiters: [" "],
                                  };
                                });
                              }

                              setTempSearchValues((prev) => {
                                return {
                                  ...prev,
                                  highlightDelimiters:
                                    e.currentTarget.value.split(","),
                                };
                              });
                            }}
                          />
                        </div>
                        <div class="items flex justify-between space-x-2 p-1">
                          <label>Highlight Max Length:</label>
                          <input
                            class="w-16 rounded border border-neutral-400 p-0.5 text-black"
                            type="number"
                            value={tempSearchValues().highlightMaxLength}
                            onChange={(e) => {
                              setTempSearchValues((prev) => {
                                return {
                                  ...prev,
                                  highlightMaxLength: parseInt(
                                    e.currentTarget.value,
                                  ),
                                };
                              });
                            }}
                          />
                        </div>
                        <div class="items flex justify-between space-x-2 p-1">
                          <label>Highlight Max Num:</label>
                          <input
                            class="w-16 rounded border border-neutral-400 p-0.5 text-black"
                            type="number"
                            value={tempSearchValues().highlightMaxNum}
                            onChange={(e) => {
                              setTempSearchValues((prev) => {
                                return {
                                  ...prev,
                                  highlightMaxNum: parseInt(
                                    e.currentTarget.value,
                                  ),
                                };
                              });
                            }}
                          />
                        </div>
                        <div class="items flex justify-between space-x-2 p-1">
                          <label>Highlight Window:</label>
                          <input
                            class="w-16 rounded border border-neutral-400 p-0.5 text-black"
                            type="number"
                            value={tempSearchValues().highlightWindow}
                            onChange={(e) => {
                              setTempSearchValues((prev) => {
                                return {
                                  ...prev,
                                  highlightWindow: parseInt(
                                    e.currentTarget.value,
                                  ),
                                };
                              });
                            }}
                          />
                        </div>
                        <div class="items flex justify-between space-x-2 p-1">
                          <label>Highlight Pre Tag:</label>
                          <input
                            class="w-16 rounded border border-neutral-400 p-0.5 text-black"
                            type="text"
                            value={tempSearchValues().highlightPreTag}
                            onInput={(e) => {
                              setTempSearchValues((prev) => {
                                return {
                                  ...prev,
                                  highlightPreTag: e.currentTarget.value,
                                };
                              });
                            }}
                          />
                        </div>
                        <div class="items flex justify-between space-x-2 p-1">
                          <label>Highlight Post Tag:</label>
                          <input
                            class="w-16 rounded border border-neutral-400 p-0.5 text-black"
                            type="text"
                            value={tempSearchValues().highlightPostTag}
                            onInput={(e) => {
                              setTempSearchValues((prev) => {
                                return {
                                  ...prev,
                                  highlightPostTag: e.currentTarget.value,
                                };
                              });
                            }}
                          />
                        </div>
                        <div class="flex items-center justify-between space-x-2 p-1">
                          <label>Highlight exact match</label>
                          <select
                            class="h-fit rounded-md border border-neutral-400 bg-neutral-100 p-1 dark:border-neutral-900 dark:bg-neutral-800"
                            onChange={(s) => {
                              setTempSearchValues((prev) => {
                                return {
                                  ...prev,
                                  highlightStrategy: s.target
                                    .value as HighlightStrategy,
                                };
                              });
                            }}
                            value={tempSearchValues().highlightStrategy}
                          >
                            <option value="v1">V1</option>
                            <option value="exactmatch">Exact match</option>
                          </select>
                        </div>
                        <Show when={props.search.state.groupUniqueSearch}>
                          <div class="px-1 font-bold">Group Options</div>
                          <div class="items flex justify-between space-x-2 p-1">
                            <label>Group size:</label>
                            <input
                              class="w-16 rounded border border-neutral-400 p-0.5 text-black"
                              type="number"
                              value={tempSearchValues().group_size}
                              onChange={(e) => {
                                setTempSearchValues((prev) => {
                                  return {
                                    ...prev,
                                    group_size: parseInt(e.currentTarget.value),
                                  };
                                });
                              }}
                            />
                          </div>
                        </Show>
                        <div class="px-1 font-bold">Scoring Options</div>
                        <div class="items flex justify-between space-x-2 p-1">
                          <label>Fulltext boost phrase:</label>
                          <input
                            class="w-16 rounded border border-neutral-400 p-0.5 text-black"
                            type="text"
                            value={
                              tempSearchValues().scoringOptions?.fulltext_boost
                                ?.phrase ?? ""
                            }
                            onInput={(e) => {
                              setTempSearchValues((prev) => {
                                return {
                                  ...prev,
                                  scoringOptions: {
                                    ...prev.scoringOptions,
                                    fulltext_boost: {
                                      ...prev.scoringOptions?.fulltext_boost,
                                      phrase: e.currentTarget.value,
                                    },
                                  },
                                };
                              });
                            }}
                          />
                        </div>
                        <div class="items flex justify-between space-x-2 p-1">
                          <label>Fulltext boost factor:</label>
                          <input
                            class="w-16 rounded border border-neutral-400 p-0.5 text-black"
                            type="number"
                            step="any"
                            value={
                              tempSearchValues().scoringOptions?.fulltext_boost
                                ?.boost_factor
                            }
                            onChange={(e) => {
                              setTempSearchValues((prev) => {
                                return {
                                  ...prev,
                                  scoringOptions: {
                                    ...prev.scoringOptions,
                                    fulltext_boost: {
                                      ...prev.scoringOptions?.fulltext_boost,
                                      boost_factor: parseFloat(
                                        e.currentTarget.value,
                                      ),
                                    },
                                  },
                                };
                              });
                            }}
                          />
                        </div>
                        <div class="items flex justify-between space-x-2 p-1">
                          <label>Semantic boost phrase:</label>
                          <input
                            class="w-16 rounded border border-neutral-400 p-0.5 text-black"
                            type="text"
                            value={
                              tempSearchValues().scoringOptions?.semantic_boost
                                ?.phrase ?? ""
                            }
                            onInput={(e) => {
                              setTempSearchValues((prev) => {
                                return {
                                  ...prev,
                                  scoringOptions: {
                                    ...prev.scoringOptions,
                                    semantic_boost: {
                                      ...prev.scoringOptions?.semantic_boost,
                                      phrase: e.currentTarget.value,
                                    },
                                  },
                                };
                              });
                            }}
                          />
                        </div>
                        <div class="items flex justify-between space-x-2 p-1">
                          <label>Semantic boost distance factor:</label>
                          <input
                            class="w-16 rounded border border-neutral-400 p-0.5 text-black"
                            type="number"
                            step="any"
                            value={
                              tempSearchValues().scoringOptions?.semantic_boost
                                ?.distance_factor
                            }
                            onChange={(e) => {
                              setTempSearchValues((prev) => {
                                return {
                                  ...prev,
                                  scoringOptions: {
                                    ...prev.scoringOptions,
                                    semantic_boost: {
                                      ...prev.scoringOptions?.semantic_boost,
                                      distance_factor: parseFloat(
                                        e.currentTarget.value,
                                      ),
                                    },
                                  },
                                };
                              });
                            }}
                          />
                        </div>
                      </div>
                    </PopoverPanel>
                  </Show>
                </>
              )}
            </Popover>
            <div class="flex items-center space-x-1 pb-3 text-sm">
              <Tooltip
                body={
                  <BsQuestionCircle class="h-3.5 w-3.5 rounded-full fill-current" />
                }
                tooltipText="Use multiple queries with different weights to search for chunks. Only works with Semantic Search and is not compatible with cross encoder re-ranking or highlights."
              />
              <span>Multi Query</span>
              <input
                class="h-4 w-4"
                type="checkbox"
                checked={props.search.state.multiQueries.length > 0}
                onChange={(e) => {
                  if (e.target.checked) {
                    props.search.setSearch("multiQueries", [
                      {
                        query: props.search.state.query,
                        weight: 0.5,
                      },
                    ]);
                    props.search.setSearch("searchType", "semantic");
                    props.search.setSearch("sort_by", {});
                  } else {
                    props.search.setSearch("multiQueries", []);
                  }
                }}
              />
            </div>
            <Show when={props.search.state.query !== ""}>
              <div class="flex-1" />
              <div class="flex items-center justify-self-end">
                <button
                  class="flex w-fit items-center rounded bg-neutral-100 p-1 text-sm hover:bg-neutral-100 dark:bg-neutral-700 dark:hover:bg-neutral-800"
                  onClick={(e) => {
                    e.preventDefault();
                    props.openRateQueryModal(true);
                  }}
                >
                  <FaRegularFlag class="mr-2" />
                  Rate This Search
                </button>
              </div>
            </Show>
            <Show when={!props.groupID}>
              <Show when={props.search.state.query === ""}>
                <div class="flex-1" />
              </Show>
              <div class="flex items-center space-x-2 justify-self-center">
                <label class="text-sm">Group Search</label>
                <input
                  class="h-4 w-4"
                  type="checkbox"
                  checked={props.search.state.groupUniqueSearch}
                  onChange={(e) => {
                    props.search.setSearch(
                      "groupUniqueSearch",
                      e.target.checked,
                    );
                  }}
                />
              </div>
            </Show>
            {/* New Task Definition textbox in the dashboard for AIMon reranker
            <Show
              when={
                rerankTypes().find(
                  (type) => type.value === "cross_encoder" && type.isSelected
                )
              }
            >
              <div class="flex items-center space-x-2 mb-4">
                <label class="block text-sm font-medium text-gray-700 dark:text-white">
                  Task Definition (for AIMon reranker):
                </label>
                <input
                  type="text"
                  class="w-[700px] h-10 rounded border border-neutral-400 p-2.5 text-black"
                  value={
                    tempSearchValues().taskDefinition ||
                    "Your task is to grade the relevance of context document(s) against the specified user query."
                  }
                  onInput={(e) => {
                    setTempSearchValues((prev) => ({
                      ...prev,
                      taskDefinition: e.currentTarget.value,
                    }));
                  }}
                />
              </div>
            </Show> */}
          </div>
        </form>
      </div>
    </>
  );
};

export default SearchForm;
