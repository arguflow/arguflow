/* eslint-disable @typescript-eslint/no-explicit-any */
import React, { useEffect, useMemo } from "react";
import { useState } from "react";
import {
  CheckIcon,
  ChevronDownIcon,
  ChevronUpicon,
  LoadingIcon,
  MagnifyingGlassIcon,
  PhotoIcon,
  XIcon,
} from "./icons";
import {
  InferenceFiltersFormProps,
  useModalState,
} from "../utils/hooks/modal-context";
import { toBase64 } from "./Search/UploadImage";
import {
  getPresignedUrl,
  groupSearchWithTrieve,
  uploadFile,
} from "../utils/trieve";
import { ToolFunctionParameter } from "trieve-ts-sdk";
import { getFingerprint } from "@thumbmarkjs/thumbmarkjs";
import Markdown from "react-markdown";
import { urlWordRegex } from "./Chat/ResponseMessage";
import { GroupChunk } from "../utils/types";
import { ProductGroupItem } from "./Search/ProductGroupItem";

export const ActiveFilterPills = () => {
  const { selectedSidebarFilters, setSelectedSidebarFilters } = useModalState();

  const activeFilters: {
    sectionKey: string;
    tags: string[];
  }[] = useMemo(() => {
    const filters = Object.entries(selectedSidebarFilters).map(
      ([sectionKey, tags]) => ({
        sectionKey,
        tags,
      })
    );
    return filters;
  }, [selectedSidebarFilters]);

  const numberOfSelectedFilters = useMemo(() => {
    let count = 0;
    for (const { sectionKey } of activeFilters) {
      if (sectionKey in selectedSidebarFilters) {
        count += selectedSidebarFilters[sectionKey].length;
      }
    }
    return count;
  }, [selectedSidebarFilters]);

  return (
    <div
      className="trieve-active-filter-pills-container"
      data-number-selected-filters={numberOfSelectedFilters}
    >
      <div className="trieve-all-active-filters">
        {activeFilters.map(({ sectionKey, tags }) =>
          tags.map((tag) => (
            <button
              className="trieve-active-filter-pill"
              key={tag}
              onClick={() => {
                setSelectedSidebarFilters((prev) => ({
                  ...prev,
                  [sectionKey]: prev[sectionKey].filter((t) => t !== tag),
                }));
              }}
            >
              <span>{tag}</span>
              <i className="trieve-active-filter-pill-remove-icon">
                <XIcon />
              </i>
            </button>
          ))
        )}
      </div>
      <button
        className="trieve-clear-filters-button"
        data-number-selected-filters={numberOfSelectedFilters}
        onClick={() => {
          setSelectedSidebarFilters({});
        }}
      >
        Clear all
      </button>
    </div>
  );
};

export interface AccordionProps {
  sectionKey: string;
  title: string;
  children: React.ReactNode;
  defaultOpen?: boolean;
  onToggle?: () => void;
}

export const Accordion = ({
  sectionKey,
  title,
  children,
  defaultOpen = false,
  onToggle,
}: AccordionProps) => {
  const { selectedSidebarFilters } = useModalState();
  const [open, setOpen] = useState(defaultOpen);

  const numberOfSelectedFilters = useMemo(() => {
    if (sectionKey in selectedSidebarFilters) {
      return selectedSidebarFilters[sectionKey].length;
    }
    return 0;
  }, [sectionKey, selectedSidebarFilters]);

  return (
    <div
      className="trieve-accordion-container"
      data-open={open ? "true" : "false"}
    >
      <div
        className="trieve-accordion-header"
        data-open={open ? "true" : "false"}
        onClick={() => {
          setOpen(!open);
          if (onToggle) {
            onToggle();
          }
        }}
      >
        <h3 className="trieve-accordion-title">{title}</h3>
        <div className="trieve-accordion-details">
          <span
            className="trieve-accordion-number"
            data-value={numberOfSelectedFilters}
          >
            {numberOfSelectedFilters}
          </span>
          <div className="trieve-accordion-icon-container">
            {open ? <ChevronUpicon /> : <ChevronDownIcon />}
          </div>
        </div>
      </div>
      <div
        className="trieve-accordion-content-container"
        data-open={open ? "true" : "false"}
      >
        <div className="trieve-accordion-content">{children}</div>
        <hr />
      </div>
    </div>
  );
};

export interface FilterButtonProps {
  sectionKey: string;
  filterKey: string;
  label: string;
  type: "single" | "multiple";
  description?: string;
  onClick?: () => void;
}

export const FilterButton = ({
  sectionKey,
  filterKey,
  label,
  description,
  type,
  onClick,
}: FilterButtonProps) => {
  const { selectedSidebarFilters, setSelectedSidebarFilters } = useModalState();

  const active = useMemo(() => {
    if (sectionKey in selectedSidebarFilters) {
      const selectedFilters = selectedSidebarFilters[sectionKey];
      return selectedFilters.includes(filterKey);
    }
    return false;
  }, [sectionKey, filterKey, selectedSidebarFilters]);

  const handleClick = () => {
    if (type === "single") {
      if (active) {
        setSelectedSidebarFilters({
          ...selectedSidebarFilters,
          [sectionKey]: [],
        });
      } else {
        setSelectedSidebarFilters({
          ...selectedSidebarFilters,
          [sectionKey]: [filterKey],
        });
      }
    } else {
      if (active) {
        setSelectedSidebarFilters({
          ...selectedSidebarFilters,
          [sectionKey]: selectedSidebarFilters[sectionKey].filter(
            (item) => item !== filterKey
          ),
        });
      } else {
        setSelectedSidebarFilters({
          ...selectedSidebarFilters,
          [sectionKey]: [
            ...(selectedSidebarFilters[sectionKey] || []),
            filterKey,
          ],
        });
      }
    }
    if (onClick) onClick();
  };

  return (
    <button className="trieve-filter-button-container" onClick={handleClick}>
      <div
        className={`trieve-${type}-button`}
        data-active={active ? "true" : "false"}
      >
        <div className="trieve-circle" />
        <i className="trieve-checkbox-icon">
          <CheckIcon />
        </i>
      </div>
      <label className="trieve-filter-button-label" title={description}>
        {label}
      </label>
    </button>
  );
};

export interface SearchQueryState {
  query: string;
  loading: boolean;
}

export interface InferenceFilterFormStep {
  title: string;
  description: string;
  type: "image" | "tags" | "search_results";
  placeholder?: string;
  filterSidebarSectionKey?: string;
  prompt?: string;
}

export const InferenceFiltersForm = ({ steps }: InferenceFiltersFormProps) => {
  const {
    trieveSDK,
    props,
    setSelectedSidebarFilters,
    selectedSidebarFilters,
  } = useModalState();
  const [images, setImages] = useState<Record<string, File>>({});
  const [imageUrls, setImageUrls] = useState<Record<string, string>>({});
  const [searchQueries, setSearchQueries] = useState<
    Record<string, SearchQueryState>
  >({});
  const [searchResults, setSearchResults] = useState<
    Record<string, GroupChunk[][]>
  >({});
  const [loadingStates, setLoadingStates] = useState<Record<string, string>>(
    {}
  );
  const [filterOptions, setFilterOptions] = useState<Record<string, string[]>>(
    {}
  );

  useEffect(() => {
    const toolCallAbortController = new AbortController();

    for (let i = 1; i < steps.length; i++) {
      const correspondingFilter =
        props.searchPageProps?.filterSidebarProps?.sections.find(
          (section) => section.key === steps[i].filterSidebarSectionKey
        );
      if (!correspondingFilter?.options) {
        continue;
      }

      const prevStep = steps[i - 1];
      if (prevStep.type === "image") {
        if (!images[prevStep.title]) {
          continue;
        }
        (async () => {
          setSelectedSidebarFilters((prev) => {
            return {
              ...prev,
              [steps[i].filterSidebarSectionKey ?? ""]: [],
            };
          });
          setLoadingStates((prev) => ({
            ...prev,
            [steps[i].title]: "Uploading image...",
          }));
          const imageFile = images[prevStep.title];
          const data = await toBase64(imageFile);
          const base64File = data
            .split(",")[1]
            .replace(/\+/g, "-")
            .replace(/\//g, "_")
            .replace(/=+$/, "");
          const fileId = await uploadFile(
            trieveSDK,
            imageFile.name,
            base64File
          );
          const imageUrl = await getPresignedUrl(trieveSDK, fileId);
          setImageUrls((prev) => ({
            ...prev,
            [prevStep.title]: imageUrl,
          }));

          setLoadingStates((prev) => ({
            ...prev,
            [steps[i].title]: "Detecting materials present in the image...",
          }));
          const filterParamsResp = await trieveSDK.getToolCallFunctionParams(
            {
              user_message_text: steps[i].prompt ?? "",
              image_url: imageUrl ? imageUrl : null,
              tool_function: {
                name: "get_materials_present_in_image",
                description:
                  "Decide on which materials are present in the image. Err on the side of caution and include materials that you are only somewhat sure are present.",
                parameters:
                  correspondingFilter.options?.map((tag) => {
                    return {
                      name: tag.label,
                      parameter_type: "boolean",
                      description: tag.description ?? "",
                    } as ToolFunctionParameter;
                  }) ?? [],
              },
            },
            toolCallAbortController.signal
          );
          const match_any_tags: string[] = [];
          for (const key of Object.keys(filterParamsResp.parameters ?? {})) {
            const filterParam = (filterParamsResp.parameters as any)[
              key as keyof typeof filterParamsResp.parameters
            ];
            if (typeof filterParam === "boolean" && filterParam) {
              const tag = correspondingFilter.options?.find(
                (t) => t.label === key
              )?.tag;
              if (tag) {
                match_any_tags.push(tag);
              }
            }
          }
          setFilterOptions((prev) => {
            const newFilterOptions = {
              ...prev,
              [steps[i].filterSidebarSectionKey ?? ""]: match_any_tags,
            };

            return newFilterOptions;
          });

          setLoadingStates((prev) => ({
            ...prev,
            [steps[i].title]: "idle",
          }));
        })();
      }
    }

    return () => {
      toolCallAbortController.abort();
    };
  }, [images]);

  useEffect(() => {
    const firstMessageInferenceAbortController = new AbortController();
    for (let i = 1; i < steps.length; i++) {
      if (steps[i].type === "search_results") {
        const prevFilter = steps[i - 1].filterSidebarSectionKey;
        const selectedTags = selectedSidebarFilters[prevFilter ?? ""];
        if (!selectedTags?.length) {
          continue;
        }

        (async () => {
          setLoadingStates((prev) => ({
            ...prev,
            [steps[i].title]: "Figuring out what will look good...",
          }));
          setSearchQueries((prev) => ({
            ...prev,
            [steps[i].title]: {
              query: "",
              loading: true,
            },
          }));

          const fingerprint = await getFingerprint();
          const replacementMaterialDescriptionReader =
            await trieveSDK.ragOnChunkReader(
              {
                chunk_ids: [],
                image_urls: Object.values(imageUrls).filter((url) => url),
                prev_messages: [
                  {
                    content: `${steps[i].prompt ?? ""} ${selectedTags.join(", ")}`,
                    role: "user",
                  },
                ],
                prompt: "",
                stream_response: true,
                user_id: fingerprint.toString(),
              },
              firstMessageInferenceAbortController.signal
            );
          setLoadingStates((prev) => ({
            ...prev,
            [steps[i].title]: "Generating search query...",
          }));

          let done = false;
          let textInStream = "";
          while (!done) {
            const { value, done: doneReading } =
              await replacementMaterialDescriptionReader.read();
            if (doneReading) {
              done = doneReading;
              setSearchQueries((prev) => ({
                ...prev,
                [steps[i].title]: {
                  query: textInStream,
                  loading: false,
                },
              }));
            } else if (value) {
              setLoadingStates((prev) => ({
                ...prev,
                [steps[i].title]: "idle",
              }));
              const decoder = new TextDecoder();
              const newText = decoder.decode(value);
              textInStream += newText;
              setSearchQueries((prev) => ({
                ...prev,
                [steps[i].title]: {
                  query: textInStream,
                  loading: false,
                },
              }));
            }
          }
        })();
      }
    }

    return () => {
      firstMessageInferenceAbortController.abort();
    };
  }, [selectedSidebarFilters]);

  useEffect(() => {
    for (let i = 1; i < steps.length; i++) {
      if (
        steps[i].type === "search_results" &&
        searchQueries[steps[i].title]?.loading === false &&
        searchQueries[steps[i].title]?.query.length > 0
      ) {
        (async () => {
          const results = await groupSearchWithTrieve({
            query_string: searchQueries[steps[i].title].query,
            searchOptions: props.searchOptions,
            trieve: trieveSDK,
            ...Object.values(selectedSidebarFilters).flat(),
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

          setSearchResults((prev) => ({
            ...prev,
            [steps[i].title]: Array.from(groupMap.values()),
          }));
        })();
      }
    }
  }, [searchQueries]);

  return (
    <div className="trieve-inference-filters-form">
      {steps.map((step, index) => (
        <div
          className="trieve-inference-filters-step-container"
          key={index}
          data-prev-complete={
            index == 0 ||
            images[steps[index - 1].title] ||
            selectedSidebarFilters[
              steps[index - 1].filterSidebarSectionKey ?? ""
            ]?.length
              ? "true"
              : "false"
          }
        >
          <div className="trieve-inference-filters-step-header">
            <div
              className="trieve-inference-filters-step-number"
              data-completed={images[step.title] ? "true" : "false"}
            >
              <span>{index + 1}</span>
            </div>
            <h2 className="trieve-inference-filters-step-title">
              {step.title}
            </h2>
          </div>
          <p className="trieve-inference-filters-step-description">
            {step.description}
          </p>
          <div
            className="trieve-inference-filters-step-input-container"
            data-loading-state={loadingStates[step.title] ?? "idle"}
          >
            <div
              className="trieve-image-input-container"
              data-input-field-type={step.type}
              data-image-selected={images[step.title] ? "true" : "false"}
              onDragOver={(e) => {
                e.preventDefault();
                e.stopPropagation();
              }}
              onDrop={(e) => {
                e.preventDefault();
                e.stopPropagation();
                const files = e.dataTransfer.files;
                setImages((prev) => ({
                  ...prev,
                  [step.title]: files[0],
                }));
              }}
              onClick={() => {
                const input = document.createElement("input");
                input.type = "file";
                input.accept = "image/*";
                input.multiple = true;
                input.onchange = (e) => {
                  const files = (e.target as HTMLInputElement).files;
                  if (files) {
                    setImages((prev) => ({
                      ...prev,
                      [step.title]: files[0],
                    }));
                  }
                };
                input.click();
              }}
            >
              <i
                className="trieve-image-input-icon"
                data-image-selected={images[step.title] ? "true" : "false"}
              >
                <PhotoIcon />
              </i>
              <img
                className="trieve-image-input-preview"
                src={
                  images[step.title]
                    ? URL.createObjectURL(images[step.title])
                    : ""
                }
                alt=""
                data-image-selected={images[step.title] ? "true" : "false"}
              />
              <p className="trieve-image-input-placeholder">
                {step.placeholder}
              </p>
            </div>

            <div
              className="trieve-inference-filters-step-tags"
              data-input-field-type={step.type}
            >
              {filterOptions[step.filterSidebarSectionKey ?? ""]?.map((tag) => (
                <button
                  className="trieve-inference-filters-step-tag"
                  key={tag}
                  data-active={
                    Object.keys(selectedSidebarFilters ?? {}).includes(
                      step.filterSidebarSectionKey ?? ""
                    ) &&
                    selectedSidebarFilters[
                      step.filterSidebarSectionKey ?? ""
                    ]?.includes(tag)
                      ? "true"
                      : "false"
                  }
                  onClick={() => {
                    setSelectedSidebarFilters((prev) => {
                      const selectedTags =
                        prev[step.filterSidebarSectionKey ?? ""];
                      if (selectedTags?.includes(tag)) {
                        return {
                          ...prev,
                          [step.filterSidebarSectionKey ?? ""]:
                            selectedTags.filter((t) => t !== tag),
                        };
                      }

                      return {
                        ...prev,
                        [step.filterSidebarSectionKey ?? ""]: [
                          ...(prev[step.filterSidebarSectionKey ?? ""] ?? []),
                          tag,
                        ],
                      };
                    });
                  }}
                >
                  <span>{tag}</span>
                  <i className="trieve-checkbox-icon">
                    <CheckIcon />
                  </i>
                </button>
              ))}
            </div>

            <div
              className="trieve-inference-filters-search-query"
              data-input-field-type={step.type}
            >
              <div
                data-is-empty={
                  (searchQueries[step.title]?.query.length ?? 0) === 0
                }
                className="trieve-inference-filters-search-query-text"
              >
                <div className="trieve-inference-filters-search-query-icon">
                  <MagnifyingGlassIcon />
                </div>
                <Markdown
                  className="code-markdown"
                  components={{
                    code: (codeProps) => {
                      const { children } = codeProps || {};
                      if (!children) return null;
                      return children?.toString();
                    },
                    a: (anchorProps) => {
                      const { children, href, title } = anchorProps || {};
                      if (!children) return null;
                      return (
                        <a
                          href={href}
                          target={props.openLinksInNewTab ? "_blank" : ""}
                          title={title}
                        >
                          {children?.toString()}
                        </a>
                      );
                    },
                  }}
                  key={step.title}
                >
                  {searchQueries[step.title]?.query.length > 0
                    ? searchQueries[step.title].query.replace(urlWordRegex, "")
                    : ""}
                </Markdown>
              </div>
            </div>

            <div className="trieve-inference-filters-search-results">
              {searchResults[step.title]?.map((result) => (
                <ProductGroupItem
                  key={result[0].group.id}
                  group={result}
                  index={index}
                  requestID={""}
                />
              ))}
            </div>
          </div>
          <div
            className="trieve-inference-filters-step-loading-container"
            data-loading-state={loadingStates[step.title] ?? "idle"}
          >
            <LoadingIcon className="loading" />
            <p className="trieve-loading-text">{loadingStates[step.title]}</p>
          </div>
        </div>
      ))}
    </div>
  );
};
