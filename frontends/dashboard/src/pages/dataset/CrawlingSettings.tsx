/* eslint-disable @typescript-eslint/no-unsafe-member-access */
/* eslint-disable @typescript-eslint/no-explicit-any */
/* eslint-disable @typescript-eslint/no-unsafe-assignment */
import { createMutation, createQuery } from "@tanstack/solid-query";
import { Show, useContext, createMemo } from "solid-js";
import { DatasetContext } from "../../contexts/DatasetContext";
import { useTrieve } from "../../hooks/useTrieve";
import { CrawlInterval, CrawlOptions, ScrapeOptions } from "trieve-ts-sdk";
import { createStore } from "solid-js/store";
import { MultiStringInput, Select, Tooltip } from "shared/ui";
import { toTitleCase } from "../../analytics/utils/titleCase";
import { Spacer } from "../../components/Spacer";
import { UserContext } from "../../contexts/UserContext";
import { createToast } from "../../components/ShowToasts";
import { ErrorMsg, ValidateErrors, ValidateFn } from "../../utils/validation";
import { cn } from "shared/utils";
import { FaRegularCircleQuestion } from "solid-icons/fa";

export const defaultCrawlOptions: CrawlOptions = {
  boost_titles: true,
  allow_external_links: false,
  ignore_sitemap: false,
  exclude_paths: [],
  exclude_tags: ["navbar", "footer", "aside", "nav", "form"],
  include_paths: [],
  include_tags: [],
  interval: "daily",
  limit: 1000,
  site_url: "",
  scrape_options: {
    group_variants: true,
  } as ScrapeOptions,
};

export type FlatCrawlOptions = Omit<CrawlOptions, "scrape_options"> & {
  type?: "openapi" | "shopify" | "youtube";
  openapi_schema_url?: string;
  openapi_tag?: string;
  group_variants?: boolean | null;
  tag_regexes?: string[] | null;
};

export const unflattenCrawlOptions = (
  options: FlatCrawlOptions,
): CrawlOptions => {
  if (options && options.type == "openapi") {
    if (!options.openapi_schema_url || !options.openapi_tag) {
      return {
        ...options,
        scrape_options: null,
      };
    }
    return {
      allow_external_links: options.allow_external_links,
      boost_titles: options.boost_titles,
      exclude_paths: options.exclude_paths,
      exclude_tags: options.exclude_tags,
      include_paths: options.include_paths,
      include_tags: options.include_tags,
      interval: options.interval,
      limit: options.limit,
      site_url: options.site_url,
      scrape_options: {
        type: "openapi",
        openapi_schema_url: options.openapi_schema_url,
        openapi_tag: options.openapi_tag,
      },
    };
  } else if (options && options.type == "shopify") {
    return {
      allow_external_links: options.allow_external_links,
      boost_titles: options.boost_titles,
      exclude_paths: options.exclude_paths,
      exclude_tags: options.exclude_tags,
      include_paths: options.include_paths,
      include_tags: options.include_tags,
      interval: options.interval,
      limit: options.limit,
      site_url: options.site_url,
      scrape_options: {
        type: "shopify",
        group_variants: options.group_variants,
        tag_regexes: options.tag_regexes ?? [],
      },
    };
  } else if (options && options.type == "youtube") {
    return {
      allow_external_links: options.allow_external_links,
      boost_titles: options.boost_titles,
      exclude_paths: options.exclude_paths,
      exclude_tags: options.exclude_tags,
      include_paths: options.include_paths,
      include_tags: options.include_tags,
      interval: options.interval,
      limit: options.limit,
      site_url: options.site_url,
      scrape_options: {
        type: "youtube",
      },
    };
  }
  return {
    allow_external_links: options.allow_external_links,
    boost_titles: options.boost_titles,
    exclude_paths: options.exclude_paths,
    exclude_tags: options.exclude_tags,
    include_paths: options.include_paths,
    include_tags: options.include_tags,
    interval: options.interval,
    limit: options.limit,
    site_url: options.site_url,
    scrape_options: null,
  };
};

export const flattenCrawlOptions = (
  options: CrawlOptions,
): FlatCrawlOptions => {
  if (options.scrape_options?.type == "openapi") {
    return {
      ...options,
      type: "openapi",
      openapi_schema_url: options.scrape_options.openapi_schema_url,
      openapi_tag: options.scrape_options.openapi_tag,
    };
  } else if (options.scrape_options?.type == "shopify") {
    return {
      ...options,
      type: "shopify",
      group_variants: options.scrape_options.group_variants,
      tag_regexes: options.scrape_options.tag_regexes,
    };
  } else if (options.scrape_options?.type == "youtube") {
    return {
      ...options,
      type: "youtube",
    };
  } else {
    return {
      ...options,
      type: undefined,
      openapi_schema_url: (options.scrape_options as any)?.openapi_schema_url,
      openapi_tag: (options.scrape_options as any)?.openapi_tag,
      group_variants: (options.scrape_options as any)?.group_variants,
    };
  }
};

export const CrawlingSettings = () => {
  const datasetId = useContext(DatasetContext).datasetId;
  const userContext = useContext(UserContext);
  const trieve = useTrieve();

  const crawlSettingsQuery = createQuery(() => ({
    queryKey: ["crawl-settings", datasetId()],
    queryFn: async () => {
      const result = await trieve.fetch(
        "/api/dataset/crawl_options/{dataset_id}",
        "get",
        {
          datasetId: datasetId(),
        },
      );

      return result.crawl_options ?? null;
    },
  }));

  const updateDatasetMutation = createMutation(() => ({
    mutationKey: ["crawl-settings-update", datasetId()],
    mutationFn: async (options: CrawlOptions) => {
      await trieve.fetch("/api/dataset", "put", {
        data: {
          crawl_options: options,
          dataset_id: datasetId(),
        },
        organizationId: userContext.selectedOrg().id,
      });
    },
    onSuccess() {
      createToast({
        title: "Success",
        type: "success",
        message: "Successfully updated crawl options",
      });

      void crawlSettingsQuery.refetch();
    },
    onError(e) {
      createToast({
        title: "Error",
        type: "error",
        message: `Failed to update crawl options: ${e.message}`,
        timeout: 5000,
      });
    },
  }));

  const onSave = (options: CrawlOptions) => {
    console.log("options", options);
    updateDatasetMutation.mutate(options);
  };

  return (
    <Show when={crawlSettingsQuery.isSuccess}>
      <RealCrawlingSettings
        onSave={onSave}
        mode={crawlSettingsQuery.data ? "edit" : "create"}
        initialCrawlingSettings={flattenCrawlOptions(
          crawlSettingsQuery.data || defaultCrawlOptions,
        )}
      />
    </Show>
  );
};

interface RealCrawlingSettingsProps {
  initialCrawlingSettings: FlatCrawlOptions;
  mode: "edit" | "create";
  onSave: (options: CrawlOptions) => void;
}

const Error = (props: { error: string | null | undefined }) => {
  return (
    <Show when={props.error}>
      <div class="text-sm text-red-500">{props.error}</div>
    </Show>
  );
};

export const validateFlatCrawlOptions: ValidateFn<FlatCrawlOptions> = (
  value,
) => {
  const errors: ValidateErrors<FlatCrawlOptions> = {};
  if (!value.site_url) {
    errors.site_url = "Site URL is required";
  }

  if (value.site_url && !value.site_url.startsWith("http")) {
    errors.site_url = "Invalid Site URL - http(s):// required";
  }

  if (value.type != "shopify") {
    if (!value.limit || value.limit <= 0) {
      errors.limit = "Limit must be greater than 0";
    }
    if (value.type === "openapi" && !value.openapi_schema_url) {
      errors.openapi_schema_url = "OpenAPI Schema URL is required";
    }
    if (
      value.type == "openapi" &&
      value.openapi_tag &&
      !value.openapi_schema_url
    ) {
      errors.openapi_schema_url = "OpenAPI Schema URL is required for tag";
    }
  }

  return {
    errors,
    valid: Object.values(errors).filter((v) => !!v).length === 0,
  };
};

const RealCrawlingSettings = (props: RealCrawlingSettingsProps) => {
  const [options, setOptions] = createStore(props.initialCrawlingSettings);
  const [errors, setErrors] = createStore<
    ReturnType<ValidateFn<FlatCrawlOptions>>["errors"]
  >({});

  const isShopify = createMemo(() => options.type === "shopify");
  const isOpenAPI = createMemo(() => options.type === "openapi");
  const isYoutube = createMemo(() => options.type === "youtube");

  const submit = (curOptions: FlatCrawlOptions) => {
    const validateResult = validateFlatCrawlOptions(curOptions);
    if (validateResult.valid) {
      setErrors({});
      props.onSave(unflattenCrawlOptions(curOptions));
    } else {
      setErrors(validateResult.errors);
    }
  };

  return (
    <form
      onSubmit={(e) => {
        e.preventDefault();
        submit(options);
      }}
      class="rounded border border-neutral-300 bg-white p-4 shadow"
    >
      <div class="text-lg">Crawl Options</div>

      <div class="flex w-full items-stretch justify-between gap-4 pt-2">
        <div class="grow">
          <div class="flex items-center gap-2">
            <label for="url" class="block">
              Site URL
            </label>
            <Tooltip
              tooltipText="The URL of the site to start the crawl from"
              body={<FaRegularCircleQuestion class="h-3 w-3 text-black" />}
            />
          </div>
          <input
            name="url"
            value={options.site_url || ""}
            placeholder="URL to crawl..."
            onInput={(e) => {
              setOptions("site_url", e.currentTarget.value);
            }}
            class="block w-full rounded border border-neutral-300 px-3 py-1.5 shadow-sm placeholder:text-neutral-400 focus:outline-magenta-500 sm:text-sm sm:leading-6"
          />
          <Error error={errors.site_url} />
        </div>
        <div class="min-w-[200px]">
          <Select
            options={["daily", "weekly", "monthly"] as CrawlInterval[]}
            display={(option) => toTitleCase(option)}
            onSelected={(option) => {
              setOptions("interval", option);
            }}
            class="p-1"
            selected={options.interval || "daily"}
            label="Crawl Interval"
            tooltipText="How often to crawl the site"
            tooltipDirection="left"
          />
        </div>
      </div>

      <div class="flex items-center gap-2 py-2 pt-4">
        <div class="flex items-center gap-2">
          <label class="block">Boost Titles</label>
          <Tooltip
            tooltipText="Prioritize matches on titles in the search results"
            body={<FaRegularCircleQuestion class="h-3 w-3 text-black" />}
          />
          <input
            checked={options.boost_titles || false}
            onChange={(e) => {
              setOptions("boost_titles", e.currentTarget.checked);
            }}
            class="h-3 w-3 rounded border border-neutral-300 bg-neutral-100 p-1 accent-magenta-400 dark:border-neutral-900 dark:bg-neutral-800"
            type="checkbox"
          />
        </div>

        <div class="flex items-center gap-2 pl-4">
          <label class="block">OpenAPI Spec?</label>
          <Tooltip
            tooltipText="Include an OpenAPI spec in the crawl for increased accuracy"
            body={<FaRegularCircleQuestion class="h-3 w-3 text-black" />}
          />
          <input
            onChange={(e) =>
              setOptions((prev) => {
                if (!e.currentTarget.checked) {
                  if (prev.type === "openapi") {
                    return {
                      ...prev,
                      type: undefined,
                    };
                  }
                  return {
                    ...prev,
                  };
                } else {
                  return {
                    ...prev,
                    type: "openapi",
                  };
                }
              })
            }
            checked={isOpenAPI()}
            class="h-3 w-3 rounded border border-neutral-300 bg-neutral-100 p-1 accent-magenta-400 dark:border-neutral-900 dark:bg-neutral-800"
            type="checkbox"
          />
        </div>

        <div class="flex items-center gap-2 pl-4">
          <label class="block">Shopify?</label>
          <Tooltip
            tooltipText="Check this if the site is a Shopify store"
            body={<FaRegularCircleQuestion class="h-3 w-3 text-black" />}
          />
          <input
            onChange={(e) => {
              setOptions((prev) => {
                if (!e.currentTarget.checked) {
                  if (prev.type === "shopify") {
                    return {
                      ...prev,
                      type: undefined,
                    };
                  }
                  return {
                    ...prev,
                  };
                } else {
                  return {
                    type: "shopify" as const,
                  };
                }
              });
            }}
            checked={isShopify()}
            class="h-3 w-3 rounded border border-neutral-300 bg-neutral-100 p-1 accent-magenta-400 dark:border-neutral-900 dark:bg-neutral-800"
            type="checkbox"
          />
        </div>
        <div class="flex items-center gap-2 pl-4">
          <label class="block">Youtube Channel?</label>
          <Tooltip
            tooltipText="Check this if the url is to a youtube channel"
            body={<FaRegularCircleQuestion class="h-3 w-3 text-black" />}
          />
          <input
            onChange={(e) =>
              setOptions((prev) => {
                if (!e.currentTarget.checked) {
                  if (prev.type === "youtube") {
                    return {
                      ...prev,
                      type: undefined,
                    };
                  }
                  return {
                    ...prev,
                  };
                } else {
                  return {
                    ...prev,
                    type: "youtube",
                  };
                }
              })
            }
            checked={isYoutube()}
            class="h-3 w-3 rounded border border-neutral-300 bg-neutral-100 p-1 accent-magenta-400 dark:border-neutral-900 dark:bg-neutral-800"
            type="checkbox"
          />
        </div>
      </div>

      <div class="flex items-center gap-3 py-2 pt-4">
        <div class="flex items-center gap-2">
          <label class="block">Ignore Sitemap</label>
          <Tooltip
            tooltipText="Ignore the sitemap.xml file, checkbox if the site does not have a sitemap or the sitemap is not accurate"
            body={<FaRegularCircleQuestion class="h-3 w-3 text-black" />}
          />
          <input
            class="h-3 w-3 rounded border border-neutral-300 bg-neutral-100 p-1 accent-magenta-400 dark:border-neutral-900 dark:bg-neutral-800"
            type="checkbox"
            disabled={isShopify() || isYoutube()}
            checked={options.ignore_sitemap ?? true}
            onChange={(e) => {
              setOptions("ignore_sitemap", e.currentTarget.checked);
            }}
          />
        </div>

        <div class="flex items-center gap-2">
          <label class="block">Allow External Links</label>
          <Tooltip
            tooltipText="Follow external links in the crawl. Example: if crawling the site trieve.ai, set this to true and add docs.trieve.ai to the include paths to crawl both the main site and the docs site."
            body={<FaRegularCircleQuestion class="h-3 w-3 text-black" />}
          />
          <input
            class="h-3 w-3 rounded border border-neutral-300 bg-neutral-100 p-1 accent-magenta-400 dark:border-neutral-900 dark:bg-neutral-800"
            type="checkbox"
            disabled={isShopify() || isYoutube()}
            checked={options.allow_external_links ?? false}
            onChange={(e) => {
              setOptions("allow_external_links", e.currentTarget.checked);
            }}
          />
        </div>
      </div>

      <div class="flex items-center gap-2 py-2 pt-4">
        <Show when={isShopify()}>
          <label class="block">Group Product Variants?</label>
          <input
            onChange={(e) =>
              setOptions("group_variants", e.currentTarget.checked)
            }
            checked={!!options.group_variants}
            class="h-3 w-3 rounded border border-neutral-300 bg-neutral-100 p-1 accent-magenta-400 dark:border-neutral-900 dark:bg-neutral-800"
            type="checkbox"
          />
        </Show>
      </div>
      <div class="items-center gap-2 py-2 pt-4">
        <Show when={isShopify()}>
          <div class="flex items-center gap-2">
            <div>Important Product Tags (regex)</div>
            <Tooltip
              tooltipText="Regex pattern of tags to use from the Shopify API, e.g. 'Men' to include 'Men' if it exists in a product tag."
              body={<FaRegularCircleQuestion class="h-3 w-3 text-black" />}
            />
          </div>
          <MultiStringInput
            disabled={!isShopify()}
            placeholder="Men"
            addClass="bg-magenta-100/40 px-2 text-sm rounded border border-magenta-300/40"
            addLabel="Add Product Tag"
            onChange={(value) => {
              setOptions("tag_regexes", value);
            }}
            value={options.tag_regexes || []}
          />
          <Error error={errors.tag_regexes} />
        </Show>
      </div>

      <div classList={{ "flex gap-4 pt-2": true, "opacity-40": isShopify() }}>
        <div>
          <div class="flex items-center gap-2">
            <label class="block" for="">
              Page Limit
            </label>
            <Tooltip
              tooltipText="The maximum number of pages to crawl"
              body={<FaRegularCircleQuestion class="h-3 w-3 text-black" />}
            />
          </div>
          <input
            class="block max-w-[100px] rounded border border-neutral-300 px-3 py-1.5 shadow-sm placeholder:text-neutral-400 focus:outline-magenta-500 sm:text-sm sm:leading-6"
            type="number"
            disabled={isShopify() || isYoutube()}
            value={options.limit || "0"}
            onInput={(e) => {
              setOptions("limit", parseInt(e.currentTarget.value));
            }}
          />
          <Error error={errors.limit} />
        </div>
        <Show when={options.type === "openapi"}>
          <div class="grow">
            <label class="block" for="">
              OpenAPI Schema URL
            </label>
            <input
              disabled={isShopify() || !isOpenAPI()}
              placeholder="https://example.com/openapi.json"
              value={options.openapi_schema_url || ""}
              onInput={(e) => {
                setOptions("openapi_schema_url", e.currentTarget.value);
              }}
              class="block w-full rounded border border-neutral-300 px-3 py-1.5 shadow-sm placeholder:text-neutral-400 focus:outline-magenta-500 sm:text-sm sm:leading-6"
            />
            <ErrorMsg error={errors.openapi_schema_url} />
          </div>
          <div class="grow">
            <label class="block" for="">
              OpenAPI Tag
            </label>
            <input
              disabled={isShopify() || !isOpenAPI()}
              value={options.openapi_tag || ""}
              onInput={(e) => {
                setOptions("openapi_tag", e.currentTarget.value);
              }}
              class="block w-full rounded border border-neutral-300 px-3 py-1.5 shadow-sm placeholder:text-neutral-400 focus:outline-magenta-500 sm:text-sm sm:leading-6"
            />
          </div>
        </Show>
      </div>
      <div
        class={cn(
          "grid w-full grid-cols-2 justify-stretch gap-4 pt-4 xl:grid-cols-4",
          isShopify() && "opacity-40",
        )}
      >
        <div class="">
          <div class="flex items-center gap-2">
            <label>Include URL Regex's</label>
            <Tooltip
              tooltipText="If one or more include paths are specified, only pages with URL's that match at least one of the regex patterns will be crawled"
              body={<FaRegularCircleQuestion class="h-3 w-3 text-black" />}
            />
          </div>
          <MultiStringInput
            disabled={isShopify() || isYoutube()}
            placeholder="https://example.com/include/*"
            addClass="bg-magenta-100/40 px-2 rounded text-sm border border-magenta-300/40"
            inputClass="w-full"
            addLabel="Add Path"
            onChange={(value) => {
              setOptions("include_paths", value);
            }}
            value={options.include_paths || []}
          />
          <Error error={errors.include_paths} />
        </div>
        <div class="">
          <div class="flex items-center gap-2">
            <div>Exclude URL Regex's</div>
            <Tooltip
              tooltipText="If one or more exclude paths are specified, pages with URL's that match at least one of the regex patterns will not be crawled (even if they match an include path)"
              body={<FaRegularCircleQuestion class="h-3 w-3 text-black" />}
            />
          </div>
          <MultiStringInput
            disabled={isShopify() || isYoutube()}
            placeholder="https://example.com/exclude/*"
            addClass="bg-magenta-100/40 px-2 text-sm rounded border border-magenta-300/40"
            addLabel="Add Path"
            onChange={(value) => {
              setOptions("exclude_paths", value);
            }}
            value={options.exclude_paths || []}
          />
          <Error error={errors.exclude_paths} />
        </div>
        <div class="">
          <div class="flex items-center gap-2">
            <div>Include Query Selectors</div>
            <Tooltip
              tooltipText="HTML for a page is parsed and all elements matching one or more of the include tags are assembled into a div fort the exclude query selectors to apply to. What is left is the content of the page to be indexed for search."
              body={<FaRegularCircleQuestion class="h-3 w-3 text-black" />}
            />
          </div>
          <MultiStringInput
            disabled={isShopify() || isYoutube()}
            placeholder="h1..."
            addClass="bg-magenta-100/40 text-sm px-2 rounded border border-magenta-300/40"
            addLabel="Add Selector"
            onChange={(value) => {
              setOptions("include_tags", value);
            }}
            value={options.include_tags || []}
          />
          <Error error={errors.include_tags} />
        </div>
        <div class="">
          <div class="flex items-center gap-2">
            <div>Exclude Query Selectors</div>
            <Tooltip
              tooltipText="HTML for a page is parsed and all elements matching one or more of the exclude tags are removed from the page before indexing. Exclude selectors are applied after include selectors."
              body={<FaRegularCircleQuestion class="h-3 w-3 text-black" />}
            />
          </div>
          <MultiStringInput
            disabled={isShopify() || isYoutube()}
            placeholder="button..."
            addClass="bg-magenta-100/40 px-2 text-sm rounded border border-magenta-300/40"
            addLabel="Add Selector"
            onChange={(value) => {
              setOptions("exclude_tags", value);
            }}
            value={options.exclude_tags || []}
          />
          <Error error={errors.exclude_tags} />
        </div>
        <div class="">
          <div class="flex items-center gap-2">
            <div>Heading Remove Strings</div>
            <Tooltip
              tooltipText="Once the page is parsed and separated into heading+body chunks, the heading remove strings are removed from the heading."
              body={<FaRegularCircleQuestion class="h-3 w-3 text-black" />}
            />
          </div>
          <MultiStringInput
            placeholder="#"
            addClass="bg-magenta-100/40 px-2 text-sm rounded border border-magenta-300/40"
            addLabel="Add Text"
            onChange={(value) => {
              setOptions("heading_remove_strings", value);
            }}
            value={options.heading_remove_strings || []}
          />
          <Error error={errors.heading_remove_strings} />
        </div>
        <div class="">
          <div class="flex items-center gap-2">
            <div>Body Remove Strings</div>
            <Tooltip
              tooltipText="Once the page is parsed and separated into heading+body chunks, the body remove strings are removed from the body."
              body={<FaRegularCircleQuestion class="h-3 w-3 text-black" />}
            />
          </div>
          <MultiStringInput
            placeholder="#"
            addClass="bg-magenta-100/40 px-2 text-sm rounded border border-magenta-300/40"
            addLabel="Add Text"
            onChange={(value) => {
              setOptions("body_remove_strings", value);
            }}
            value={options.body_remove_strings || []}
          />
          <Error error={errors.body_remove_strings} />
        </div>
      </div>
      <Spacer h={18} />
      <div class="mt-5 flex justify-start">
        <button class="self-start rounded-md bg-magenta-400 px-5 py-2 text-white">
          Save
        </button>
      </div>
    </form>
  );
};
