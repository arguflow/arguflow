import { createEffect, createSignal, For, Show } from "solid-js";
import { CopyButton } from "../../components/CopyButton";
import { FaRegularCircleQuestion } from "solid-icons/fa";
import { JsonInput, MultiStringInput, Select, Tooltip } from "shared/ui";
import { publicPageSearchOptionsSchema } from "../../analytics/utils/schemas/autocomplete";
import { FiExternalLink, FiPlus, FiTrash } from "solid-icons/fi";

import {
  PublicPageProvider,
  usePublicPage,
} from "../../hooks/usePublicPageSettings";
import { HeroPatterns } from "./HeroPatterns";
import { createStore } from "solid-js/store";
import { PublicPageTabMessage } from "trieve-ts-sdk";

export const PublicPageSettingsPage = () => {
  return (
    <div class="rounded border border-neutral-300 bg-white p-4 shadow">
      <div class="flex items-end justify-between pb-2">
        <div>
          <h2 id="user-details-name" class="text-xl font-medium leading-6">
            Public Page
          </h2>
          <p class="mt-1 text-sm text-neutral-600">
            Expose a public page to send your share your search to others
          </p>
        </div>
      </div>
      <PublicPageProvider>
        <PublicPageControls />
      </PublicPageProvider>
    </div>
  );
};

const PublicPageControls = () => {
  const {
    extraParams,
    setExtraParams,
    isPublic,
    publishDataset,
    unpublishDataset,
    publicUrl,
    searchOptionsError,
  } = usePublicPage();

  return (
    <>
      <Show when={!isPublic()}>
        <div class="flex items-center space-x-2">
          <button
            onClick={() => {
              void publishDataset();
            }}
            class="inline-flex justify-center rounded-md bg-magenta-500 px-3 py-2 text-sm font-semibold text-white shadow-sm hover:bg-magenta-700 focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-magenta-900"
          >
            Publish Dataset
          </button>
          <Tooltip
            tooltipText="Make a UI to display the search with our component. This is revertable"
            body={<FaRegularCircleQuestion class="h-3 w-3 text-black" />}
          />
        </div>
      </Show>
      <Show when={isPublic()}>
        <div class="mt-4 flex content-center items-center gap-1.5 gap-x-2.5">
          <span class="font-medium">Published Url:</span>{" "}
          <a class="text-magenta-400" href={publicUrl()} target="_blank">
            {publicUrl()}
          </a>
          <CopyButton size={15} text={publicUrl()} />
          <a
            class="cursor-pointer text-sm text-gray-500 hover:text-magenta-400"
            href={publicUrl()}
            target="_blank"
          >
            <FiExternalLink />
          </a>
        </div>
        <div class="mt-4 flex space-x-3">
          <div class="grow">
            <div class="flex items-center gap-1">
              <label class="block" for="">
                Brand Logo Link
              </label>
              <Tooltip
                tooltipText="URL for your brand's logo that will be displayed in the search component"
                body={<FaRegularCircleQuestion class="h-3 w-3 text-black" />}
              />
            </div>
            <input
              placeholder="https://cdn.trieve.ai/favicon.ico"
              value={extraParams.brandLogoImgSrcUrl || ""}
              onInput={(e) => {
                setExtraParams("brandLogoImgSrcUrl", e.currentTarget.value);
              }}
              class="block w-full rounded border border-neutral-300 px-3 py-1.5 shadow-sm placeholder:text-neutral-400 focus:outline-magenta-500 sm:text-sm sm:leading-6"
            />
          </div>
          <div class="grow">
            <div class="flex items-center gap-1">
              <label class="block" for="">
                Brand Name
              </label>
              <Tooltip
                tooltipText="Your brand name that will be displayed in the search component"
                body={<FaRegularCircleQuestion class="h-3 w-3 text-black" />}
              />
            </div>
            <input
              placeholder="Trieve"
              value={extraParams.brandName || ""}
              onInput={(e) => {
                setExtraParams("brandName", e.currentTarget.value);
              }}
              class="block w-full rounded border border-neutral-300 px-3 py-1.5 shadow-sm placeholder:text-neutral-400 focus:outline-magenta-500 sm:text-sm sm:leading-6"
            />
          </div>
          <div class="grow">
            <div class="flex items-center gap-1">
              <label class="block" for="">
                Color Theme
              </label>
              <Tooltip
                tooltipText="Choose between light and dark mode for the search component"
                body={<FaRegularCircleQuestion class="h-3 w-3 text-black" />}
              />
            </div>
            <Select
              display={(option) =>
                option.replace(/^\w/, (c) => c.toUpperCase())
              }
              onSelected={(option) => {
                setExtraParams("theme", option as "light" | "dark");
              }}
              class="bg-white py-1"
              selected={extraParams.theme || "light"}
              options={["light", "dark"]}
            />
          </div>
          <div class="grow">
            <div class="flex items-center gap-1">
              <label class="block" for="">
                Brand Color
              </label>
              <Tooltip
                tooltipText="Hex color code for the main accent color in the search component"
                body={<FaRegularCircleQuestion class="h-3 w-3 text-black" />}
              />
            </div>
            <input
              placeholder="#CB53EB"
              value={extraParams.brandColor || ""}
              onInput={(e) => {
                setExtraParams("brandColor", e.currentTarget.value);
              }}
              class="block w-full rounded border border-neutral-300 px-3 py-1.5 shadow-sm placeholder:text-neutral-400 focus:outline-magenta-500 sm:text-sm sm:leading-6"
            />
          </div>
        </div>

        <div class="mt-4 flex">
          <div class="flex grow">
            <div class="grow">
              <div class="flex items-center gap-1">
                <label class="block" for="">
                  Problem Link
                </label>
                <Tooltip
                  tooltipText="Contact link for users to report issues (e.g. mailto: or support URL)"
                  body={<FaRegularCircleQuestion class="h-3 w-3 text-black" />}
                />
              </div>
              <input
                placeholder="mailto:humans@trieve.ai"
                value={extraParams.problemLink || ""}
                onInput={(e) => {
                  setExtraParams("problemLink", e.currentTarget.value);
                }}
                class="block w-full rounded border border-neutral-300 px-3 py-1.5 shadow-sm placeholder:text-neutral-400 focus:outline-magenta-500 sm:text-sm sm:leading-6"
              />
            </div>
          </div>
          <div class="ml-3 grid grow grid-cols-2 items-center gap-1.5 p-1.5">
            <div class="flex gap-2">
              <div class="flex items-center gap-1">
                <label class="block" for="">
                  Responsive View
                </label>
                <Tooltip
                  tooltipText="Enable responsive layout for different screen sizes"
                  body={<FaRegularCircleQuestion class="h-3 w-3 text-black" />}
                />
              </div>
              <input
                checked={extraParams.responsive || false}
                type="checkbox"
                onInput={(e) => {
                  setExtraParams("responsive", e.currentTarget.checked);
                }}
                class="block w-4 rounded border border-neutral-300 px-3 py-1.5 shadow-sm placeholder:text-neutral-400 focus:outline-magenta-500 sm:text-sm sm:leading-6"
              />
            </div>
            <div class="flex gap-2">
              <div class="flex items-center gap-1">
                <label class="block" for="">
                  Analytics
                </label>
                <Tooltip
                  tooltipText="Collect analytics for searches on the page"
                  body={<FaRegularCircleQuestion class="h-3 w-3 text-black" />}
                />
              </div>
              <input
                checked={extraParams.analytics || false}
                type="checkbox"
                onChange={(e) => {
                  setExtraParams("analytics", e.currentTarget.checked);
                }}
                class="block w-4 rounded border border-neutral-300 px-3 py-1.5 shadow-sm placeholder:text-neutral-400 focus:outline-magenta-500 sm:text-sm sm:leading-6"
              />
            </div>
            <div class="flex gap-2">
              <div class="flex items-center gap-1">
                <label class="block" for="">
                  Enable Suggestions
                </label>
                <Tooltip
                  tooltipText="Show search suggestions as users type"
                  body={<FaRegularCircleQuestion class="h-3 w-3 text-black" />}
                />
              </div>
              <input
                checked={extraParams.suggestedQueries || true}
                type="checkbox"
                onChange={(e) => {
                  setExtraParams("suggestedQueries", e.currentTarget.checked);
                }}
                class="block w-4 rounded border border-neutral-300 px-3 py-1.5 shadow-sm placeholder:text-neutral-400 focus:outline-magenta-500 sm:text-sm sm:leading-6"
              />
            </div>
            <div class="flex gap-2">
              <div class="flex items-center gap-1">
                <label class="block" for="">
                  Enable Chat
                </label>
                <Tooltip
                  tooltipText="Enable RAG Chat in the component"
                  body={<FaRegularCircleQuestion class="h-3 w-3 text-black" />}
                />
              </div>
              <input
                checked={extraParams.chat || true}
                type="checkbox"
                onChange={(e) => {
                  setExtraParams("chat", e.currentTarget.checked);
                }}
                class="block w-4 rounded border border-neutral-300 px-3 py-1.5 shadow-sm placeholder:text-neutral-400 focus:outline-magenta-500 sm:text-sm sm:leading-6"
              />
            </div>
            <div class="flex gap-2">
              <div class="flex items-center gap-1">
                <label class="block" for="">
                  Ecommerce Mode
                </label>
                <Tooltip
                  tooltipText="Use the component in ecommerce mode"
                  body={<FaRegularCircleQuestion class="h-3 w-3 text-black" />}
                />
              </div>
              <input
                checked={extraParams.type === "ecommerce" || false}
                type="checkbox"
                onChange={(e) => {
                  setExtraParams(
                    "type",
                    e.currentTarget.checked ? "ecommerce" : "docs",
                  );
                }}
                class="block w-4 rounded border border-neutral-300 px-3 py-1.5 shadow-sm placeholder:text-neutral-400 focus:outline-magenta-500 sm:text-sm sm:leading-6"
              />
            </div>
            <div class="flex gap-2">
              <div class="flex items-center gap-1">
                <label class="block" for="">
                  Use Grouping
                </label>
                <Tooltip
                  tooltipText="Use search over groups instead of chunk-level search"
                  body={<FaRegularCircleQuestion class="h-3 w-3 text-black" />}
                />
              </div>
              <input
                checked={extraParams.useGroupSearch || false}
                type="checkbox"
                onChange={(e) => {
                  setExtraParams("useGroupSearch", e.currentTarget.checked);
                }}
                class="block w-4 rounded border border-neutral-300 px-3 py-1.5 shadow-sm placeholder:text-neutral-400 focus:outline-magenta-500 sm:text-sm sm:leading-6"
              />
            </div>
          </div>
        </div>
        <SearchOptions />
        <div class="mt-4 grid grid-cols-2 gap-4">
          <div class="grow">
            <div class="flex items-center gap-1">
              <label class="block" for="">
                Default Search Queries
              </label>
              <Tooltip
                tooltipText="Example search queries to show users"
                body={<FaRegularCircleQuestion class="h-3 w-3 text-black" />}
              />
            </div>
            <MultiStringInput
              placeholder={`What is ${
                extraParams["brandName"] || "Trieve"
              }?...`}
              value={extraParams.defaultSearchQueries || []}
              onChange={(e) => {
                setExtraParams("defaultSearchQueries", e);
              }}
              addLabel="Add Example"
              addClass="text-sm"
              inputClass="block w-full rounded border border-neutral-300 px-3 py-1.5 shadow-sm placeholder:text-neutral-400 focus:outline-magenta-500 sm:text-sm sm:leading-6"
            />
          </div>
          <div class="grow">
            <div class="flex items-center gap-1">
              <label class="block" for="">
                Default AI Questions
              </label>
              <Tooltip
                tooltipText="Example AI questions to show in the RAG chat"
                body={<FaRegularCircleQuestion class="h-3 w-3 text-black" />}
              />
            </div>
            <MultiStringInput
              placeholder={`What is ${
                extraParams["brandName"] || "Trieve"
              }?...`}
              value={extraParams.defaultAiQuestions || []}
              onChange={(e) => {
                setExtraParams("defaultAiQuestions", e);
              }}
              addLabel="Add Example"
              addClass="text-sm"
              inputClass="block w-full rounded border border-neutral-300 px-3 py-1.5 shadow-sm placeholder:text-neutral-400 focus:outline-magenta-500 sm:text-sm sm:leading-6"
            />
          </div>
          <div class="grow">
            <div class="flex items-center gap-1">
              <label class="block">Placeholder Text</label>
              <Tooltip
                tooltipText="Text shown in the search box before user input"
                body={<FaRegularCircleQuestion class="h-3 w-3 text-black" />}
              />
            </div>
            <input
              placeholder="Search..."
              value={extraParams.placeholder || ""}
              onInput={(e) => {
                setExtraParams("placeholder", e.currentTarget.value);
              }}
              class="block w-full rounded border border-neutral-300 px-3 py-1.5 shadow-sm placeholder:text-neutral-400 focus:outline-magenta-500 sm:text-sm sm:leading-6"
            />
          </div>
          <div class="grow">
            <div class="flex items-center gap-1">
              <label class="block">Hero Pattern</label>
              <Tooltip
                tooltipText="Choose a hero pattern for the search component"
                body={<FaRegularCircleQuestion class="h-3 w-3 text-black" />}
              />
            </div>
            <Select
              display={(option) => option}
              onSelected={(option) => {
                setExtraParams("heroPattern", "heroPatternName", option);
              }}
              class="bg-white py-1"
              selected={extraParams.heroPattern?.heroPatternName || "Blank"}
              options={Object.keys(HeroPatterns)}
            />
          </div>
        </div>
        <Show when={extraParams["heroPattern"]?.heroPatternName !== "Blank"}>
          <div class="flex flex-row items-center justify-start gap-4 pt-4">
            <div class="">
              <label class="block" for="">
                Foreground Color
              </label>
              <input
                type="color"
                onChange={(e) => {
                  setExtraParams(
                    "heroPattern",
                    "foregroundColor",
                    e.currentTarget.value,
                  );
                }}
                value={extraParams.heroPattern?.foregroundColor || "#ffffff"}
              />
            </div>
            <div class="">
              <label class="block" for="">
                Foreground Opacity
              </label>
              <input
                type="range"
                min="0"
                max="100"
                onChange={(e) => {
                  setExtraParams(
                    "heroPattern",
                    "foregroundOpacity",
                    parseInt(e.currentTarget.value) / 100,
                  );
                }}
                value={
                  (extraParams.heroPattern?.foregroundOpacity || 0.5) * 100
                }
              />
            </div>
            <div class="">
              <Show
                when={
                  extraParams.heroPattern?.heroPatternName !== "Blank" &&
                  extraParams.heroPattern?.heroPatternName !== "Solid"
                }
              >
                <label class="block" for="">
                  Background Color
                </label>
                <input
                  type="color"
                  onChange={(e) => {
                    setExtraParams(
                      "heroPattern",
                      "backgroundColor",
                      e.currentTarget.value,
                    );
                  }}
                  value={extraParams.heroPattern?.backgroundColor || "#ffffff"}
                />
              </Show>
            </div>
          </div>
        </Show>
        <details class="mb-4 mt-4">
          <summary class="cursor-pointer text-sm font-medium">
            Advanced Settings
          </summary>
          <div class="mt-4 space-y-4">
            <div class="grid grid-cols-2 gap-4">
              <div class="grow">
                <div class="flex items-center gap-1">
                  <label class="block" for="">
                    Default Currency
                  </label>
                  <Tooltip
                    tooltipText="Set the default currency for pricing display"
                    body={
                      <FaRegularCircleQuestion class="h-3 w-3 text-black" />
                    }
                  />
                </div>
                <input
                  placeholder="USD"
                  value={extraParams.defaultCurrency || ""}
                  onInput={(e) => {
                    setExtraParams("defaultCurrency", e.currentTarget.value);
                  }}
                  class="block w-full rounded border border-neutral-300 px-3 py-1.5 shadow-sm placeholder:text-neutral-400 focus:outline-magenta-500 sm:text-sm sm:leading-6"
                />
              </div>
              <div class="grow">
                <div class="flex items-center gap-1">
                  <label class="block" for="">
                    Currency Position
                  </label>
                  <Tooltip
                    tooltipText="Position of currency symbol (prefix/suffix)"
                    body={
                      <FaRegularCircleQuestion class="h-3 w-3 text-black" />
                    }
                  />
                </div>
                <Select
                  display={(option) => option}
                  onSelected={(option) => {
                    setExtraParams(
                      "currencyPosition",
                      option as "prefix" | "suffix",
                    );
                  }}
                  class="bg-white py-1"
                  selected={extraParams.currencyPosition || "prefix"}
                  options={["prefix", "suffix"]}
                />
              </div>
            </div>

            <div class="grid grid-cols-2 gap-4">
              <div class="grow">
                <div class="flex items-center gap-1">
                  <label class="block" for="">
                    Default Search Mode
                  </label>
                  <Tooltip
                    tooltipText="Set the initial search mode"
                    body={
                      <FaRegularCircleQuestion class="h-3 w-3 text-black" />
                    }
                  />
                </div>
                <Select
                  display={(option) => option}
                  onSelected={(option) => {
                    setExtraParams("defaultSearchMode", option);
                  }}
                  class="bg-white py-1"
                  selected={extraParams.defaultSearchMode || "search"}
                  options={["search", "chat"]}
                />
              </div>

              <div class="grow">
                <div class="flex items-center gap-1">
                  <label class="block" for="">
                    Debounce (ms)
                  </label>
                  <Tooltip
                    tooltipText="Delay before search triggers after typing"
                    body={
                      <FaRegularCircleQuestion class="h-3 w-3 text-black" />
                    }
                  />
                </div>
                <input
                  type="number"
                  placeholder="300"
                  value={extraParams.debounceMs || 300}
                  onInput={(e) => {
                    setExtraParams(
                      "debounceMs",
                      parseInt(e.currentTarget.value),
                    );
                  }}
                  class="block w-full rounded border border-neutral-300 px-3 py-1.5 shadow-sm placeholder:text-neutral-400 focus:outline-magenta-500 sm:text-sm sm:leading-6"
                />
              </div>
            </div>

            <div class="grid grid-cols-2 gap-4">
              <div class="flex gap-2">
                <div class="flex items-center gap-1">
                  <label class="block" for="">
                    Allow Switching Modes
                  </label>
                  <Tooltip
                    tooltipText="Enable users to switch between search modes"
                    body={
                      <FaRegularCircleQuestion class="h-3 w-3 text-black" />
                    }
                  />
                </div>
                <input
                  type="checkbox"
                  checked={extraParams.allowSwitchingModes || false}
                  onChange={(e) => {
                    setExtraParams(
                      "allowSwitchingModes",
                      e.currentTarget.checked,
                    );
                  }}
                  class="block w-4 rounded border border-neutral-300 px-3 py-1.5 shadow-sm placeholder:text-neutral-400 focus:outline-magenta-500 sm:text-sm sm:leading-6"
                />
              </div>

              <div class="flex gap-2">
                <div class="flex items-center gap-1">
                  <label class="block" for="">
                    Use Group Search
                  </label>
                  <Tooltip
                    tooltipText="Enable grouped search results"
                    body={
                      <FaRegularCircleQuestion class="h-3 w-3 text-black" />
                    }
                  />
                </div>
                <input
                  type="checkbox"
                  checked={extraParams.useGroupSearch || false}
                  onChange={(e) => {
                    setExtraParams("useGroupSearch", e.currentTarget.checked);
                  }}
                  class="block w-4 rounded border border-neutral-300 px-3 py-1.5 shadow-sm placeholder:text-neutral-400 focus:outline-magenta-500 sm:text-sm sm:leading-6"
                />
              </div>
            </div>
          </div>
        </details>

        <TabOptions />

        <div class="space-x-1.5 pt-8">
          <button
            class="inline-flex justify-center rounded-md bg-magenta-500 px-3 py-2 text-sm font-semibold text-white shadow-sm hover:bg-magenta-700 focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-magenta-900 disabled:opacity-40"
            onClick={() => {
              void publishDataset();
            }}
            disabled={searchOptionsError() !== null}
          >
            Save
          </button>
          <button
            class="inline-flex justify-center rounded-md border-2 border-magenta-500 px-3 py-2 text-sm font-semibold text-magenta-500 shadow-sm hover:bg-magenta-600 hover:text-white focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-magenta-900"
            onClick={() => {
              void unpublishDataset();
            }}
          >
            Make Private
          </button>
        </div>
      </Show>
    </>
  );
};

export const TabOptions = () => {
  const { extraParams: params } = usePublicPage();

  // We know params.tabMessages is an array because of effect in hook
  // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
  const [messages, setMessages] = createStore(params.tabMessages!);

  const [selectedTabIndex, setSelectedTabIndex] = createSignal<number | null>(
    null,
  );

  createEffect(() => {
    if (messages.length > 0 && selectedTabIndex() === null) {
      setSelectedTabIndex(0);
    }
  });

  const TabConfig = (props: {
    index: number;
    message: PublicPageTabMessage;
  }) => {
    const [nameRequiredWarning, setNameRequiredWarning] = createSignal(false);
    return (
      <>
        <button
          onClick={() => {
            setMessages([
              ...messages.slice(0, props.index),
              ...messages.slice(props.index + 1),
            ]);
            setSelectedTabIndex(null);
          }}
          class="absolute right-2 top-2 flex items-center gap-2 rounded border border-neutral-200 bg-neutral-100 p-1 text-sm font-medium text-neutral-500 hover:bg-neutral-200"
        >
          <FiTrash />
          Delete Tab
        </button>
        <div class="flex gap-6">
          <div>
            <label class="block">Tab Name</label>
            <input
              onFocusOut={(e) => {
                if (e.currentTarget.value === "") {
                  setNameRequiredWarning(true);
                }
              }}
              placeholder={`Tab ${props.index + 1}`}
              class="block w-full max-w-md rounded border border-neutral-300 px-3 py-1.5 shadow-sm placeholder:text-neutral-400 focus:outline-magenta-500 sm:text-sm sm:leading-6"
              value={props.message.title || ""}
              onInput={(e) => {
                setMessages(props.index, {
                  ...props.message,
                  title: e.currentTarget.value,
                });
              }}
            />
            <Show when={nameRequiredWarning() && props.message.title === ""}>
              <div class="text-sm text-red-500">Tab name is required</div>
            </Show>
          </div>
          <div class="flex items-end gap-2">
            <label>Show Component Code</label>
            <input
              type="checkbox"
              class="-translate-y-1"
              checked={props.message.showComponentCode || false}
              onChange={(e) => {
                setMessages(props.index, {
                  ...props.message,
                  showComponentCode: e.currentTarget.checked,
                });
              }}
            />
          </div>
        </div>
        <label class="block pt-4" for="">
          Message HTML
          <div class="text-xs text-neutral-500">
            This is the HTML that will be displayed on the public page under
            that tab
          </div>
        </label>
        <HtmlEditor
          value={props.message.tabInnerHtml || ""}
          onValueChange={(value) => {
            setMessages(props.index, {
              ...props.message,
              tabInnerHtml: value,
            });
          }}
        />
      </>
    );
  };

  return (
    <details open={messages.length > 0}>
      <summary class="cursor-pointer text-sm font-medium">Tab Messages</summary>
      <div class="flex items-end gap-2 overflow-y-auto pt-2">
        <For each={messages}>
          {(message, index) => (
            <div class="flex flex-row gap-2">
              <button
                onClick={() => {
                  setSelectedTabIndex(index);
                }}
                classList={{
                  "bg-neutral-200/70 border-neutral-200 border hover:bg-neutral-200 p-2 px-4 rounded-t-md":
                    true,
                  "!bg-magenta-100/50 border-transparent hover:bg-magenta-100/80 text-magenta-900":
                    index() === selectedTabIndex(),
                }}
              >
                {message.title || `Tab ${index() + 1}`}
              </button>
            </div>
          )}
        </For>
        <button
          onClick={() => {
            setMessages(messages.length, {
              title: "",
              tabInnerHtml: "",
              showComponentCode: false,
            });
            setSelectedTabIndex(messages.length - 1);
          }}
          classList={{
            "ml-4 flex items-center gap-2 border border-neutral-300 hover:bg-neutral-200 py-1 bg-neutral-100 p-2":
              true,
            "border-b-transparent": selectedTabIndex() !== null,
          }}
        >
          <FiPlus />
          Add Tab
        </button>
      </div>
      {/* eslint-disable-next-line @typescript-eslint/no-non-null-assertion */}
      <Show when={selectedTabIndex() != null && messages[selectedTabIndex()!]}>
        <div class="relative border border-neutral-200 p-4">
          <TabConfig
            /* eslint-disable-next-line @typescript-eslint/no-non-null-assertion */
            index={selectedTabIndex()!}
            /* eslint-disable-next-line @typescript-eslint/no-non-null-assertion */
            message={messages[selectedTabIndex()!]}
          />
        </div>
      </Show>
    </details>
  );
};

export const SearchOptions = () => {
  const {
    extraParams,
    setExtraParams,
    searchOptionsError,
    setSearchOptionsError,
  } = usePublicPage();
  return (
    <div class="p-2">
      <div class="flex items-baseline justify-between">
        <div>Search Options</div>
        <a
          href="https://ts-sdk.trieve.ai/types/types_gen.SearchChunksReqPayload.html"
          target="_blank"
          class="text-sm opacity-65"
        >
          View Schema
        </a>
      </div>
      <JsonInput
        theme="light"
        onValueChange={(value) => {
          const result = publicPageSearchOptionsSchema.safeParse(value);

          if (result.success) {
            setExtraParams("searchOptions", result.data);
            setSearchOptionsError(null);
          } else {
            setSearchOptionsError(
              result.error.errors.at(0)?.message || "Invalid Search Options",
            );
          }
        }}
        value={() => {
          return extraParams?.searchOptions || {};
        }}
        onError={(message) => {
          setSearchOptionsError(message);
        }}
      />
      <Show when={searchOptionsError()}>
        <div class="text-red-500">{searchOptionsError()}</div>
      </Show>
    </div>
  );
};

// Text area switches between preview and input
const HtmlEditor = (props: {
  value: string;
  onValueChange: (value: string) => void;
}) => {
  return (
    <textarea
      class="w-full rounded border border-neutral-300 px-3 py-1.5 font-mono shadow-sm placeholder:text-neutral-400 focus:outline-magenta-500 sm:text-sm sm:leading-6"
      rows={6}
      value={props.value}
      onInput={(e) => {
        props.onValueChange(e.currentTarget.value);
      }}
    />
  );
};
