/* eslint-disable @typescript-eslint/no-explicit-any */
import { TrieveModalSearch } from "../../../src/index";
import "../../../dist/index.css";
import { useState } from "react";
import { createFileRoute } from "@tanstack/react-router";

export const Route = createFileRoute("/ecommerce")({
  component: ECommerce,
});

export default function ECommerce() {
  const baseUrl = import.meta.env.VITE_API_BASE_URL;
  const datasetId = import.meta.env.VITE_DATASET_ID;
  const apiKey = import.meta.env.VITE_API_KEY;
  const brandName = import.meta.env.VITE_BRAND_NAME;
  const brandLogoSrcUrl = import.meta.env.VITE_BRAND_LOGO_SRC_URL;
  const brandColor = import.meta.env.VITE_ACCENT_COLOR;
  const problemLink = import.meta.env.VITE_PROBLEM_LINK;
  const useGroupSearch = import.meta.env.VITE_USE_GROUP_SEARCH == "true";
  const showFloatingButton = import.meta.env.VITE_SHOW_FLOATING_BTN == "true";
  const floatingButtonPosition = import.meta.env.VITE_FLOATING_BTN_POSITION;
  const floatingSearchIconPosition = import.meta.env
    .VITE_FLOATING_SEARCH_ICON_POSITION;
  const showFloatingSearchIcon =
    import.meta.env.VITE_SHOW_FLOATING_SEARCH_ICON == "true";
  const showFloatingInput = import.meta.env.VITE_SHOW_FLOATING_INPUT == "true";
  const usePagefind = import.meta.env.VITE_USE_PAGEFIND == "true";
  const defaultSearchQueries: string[] = (
    import.meta.env.VITE_DEFAULT_SEARCH_QUERIES ?? ""
  ).split(",");
  const defaultTags: any[] = JSON.parse(
    import.meta.env.VITE_DEFAULT_TAGS ?? "[]",
  );
  const defaultSearchMode =
    import.meta.env.VITE_DEFAULT_SEARCH_MODE ?? "search";
  const defaultAIQuestions = (
    import.meta.env.VITE_DEFAULT_AI_QUESTIONS ?? ""
  ).split(",");
  const inline = import.meta.env.VITE_INLINE == "true";
  const showResultHighlights =
    import.meta.env.VITE_SHOW_RESULT_HIGHLIGHTS == "true";
  const inlineCarousel = import.meta.env.VITE_INLINE_CAROUSEL == "true";

  const [theme, setTheme] = useState<"light" | "dark">("light");
  const [component, setComponent] = useState(0);

  return (
    <>
      <div
        className={`p-12 flex flex-col items-center justify-center w-screen h-screen relative ${
          theme === "dark" ? "bg-zinc-900 text-zinc-50" : ""
        }`}
      >
        <div className="absolute top-6 right-6">
          <ul>
            <li key="theme">
              <button
                onClick={() => setTheme(theme === "light" ? "dark" : "light")}
              >
                {theme === "light" ? (
                  <span>
                    <i className="fa-regular fa-sun"></i>
                  </span>
                ) : (
                  <span>
                    <i className="fa-regular fa-moon"></i>
                  </span>
                )}
              </button>
            </li>
          </ul>
        </div>
        {component === 0 ? (
          <>
            <h2 className="font-bold text-center py-8">
              Search Modal Component{" "}
            </h2>

            <TrieveModalSearch
              type="ecommerce"
              defaultSearchMode={defaultSearchMode}
              apiKey={apiKey}
              baseUrl={baseUrl}
              datasetId={datasetId}
              problemLink={problemLink}
              theme={theme}
              brandLogoImgSrcUrl={brandLogoSrcUrl}
              brandName={brandName}
              brandColor={brandColor}
              allowSwitchingModes={true}
              useGroupSearch={useGroupSearch}
              responsive={false}
              currencyPosition="before"
              searchOptions={{
                use_autocomplete: false,
                search_type: "fulltext",
              }}
              buttonTriggers={[
                {
                  selector: ".random-trigger-location",
                  mode: "chat",
                },
              ]}
              usePagefind={usePagefind}
              cssRelease="none"
              defaultSearchQueries={defaultSearchQueries}
              defaultAiQuestions={defaultAIQuestions}
              tags={defaultTags}
              floatingButtonPosition={floatingButtonPosition}
              showFloatingButton={showFloatingButton}
              debounceMs={10}
              floatingSearchIconPosition={floatingSearchIconPosition}
              showFloatingSearchIcon={showFloatingSearchIcon}
              inlineCarousel={inlineCarousel}
              showFloatingInput={showFloatingInput}
              inline={inline}
              showResultHighlights={showResultHighlights}
            />
          </>
        ) : (
          <>
            <h2 className="tv-font-bold tv-text-center tv-py-8">
              Search Results Component
            </h2>
            <h2 className="tv-font-bold tv-text-center tv-py-8">
              This was removed, see
              https://github.com/devflowinc/trieve/pull/2613
            </h2>
          </>
        )}

        <ul className="tv-absolute tv-top-1/2 -tv-translate-y-1/2 tv-w-full">
          {component > 0 ? (
            <li className="tv-left-6 tv-absolute">
              <button onClick={() => setComponent(0)}>
                <i className="fa-solid fa-chevron-left"></i>
              </button>
            </li>
          ) : (
            <li className="tv-right-6 tv-absolute">
              <button onClick={() => setComponent(1)}>
                <i className="fa-solid fa-chevron-right"></i>
              </button>
            </li>
          )}
        </ul>
      </div>
    </>
  );
}
