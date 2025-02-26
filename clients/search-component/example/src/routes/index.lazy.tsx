import { TrieveModalSearch } from "../../../src/index";
import "../../../dist/index.css";
import { useState } from "react";
import { createLazyFileRoute } from "@tanstack/react-router";

export const Route = createLazyFileRoute("/")({
  component: Home,
});

export default function Home() {
  const baseUrl = import.meta.env.VITE_API_BASE_URL;
  const datasetId = import.meta.env.VITE_DATASET_ID;
  const apiKey = import.meta.env.VITE_API_KEY;
  const brandName = import.meta.env.VITE_BRAND_NAME;
  const brandLogoSrcUrl = import.meta.env.VITE_BRAND_LOGO_SRC_URL;
  const brandColor = import.meta.env.VITE_ACCENT_COLOR;
  const brandFontFamily = import.meta.env.VITE_BRAND_FONT_FAMILY;
  const problemLink = import.meta.env.VITE_PROBLEM_LINK;
  const useGroupSearch = import.meta.env.VITE_USE_GROUP_SEARCH == "true";
  const usePagefind = import.meta.env.VITE_USE_PAGEFIND == "true";
  const showFloatingButton = import.meta.env.VITE_SHOW_FLOATING_BTN == "true";
  const floatingButtonPosition = import.meta.env.VITE_FLOATING_BTN_POSITION;
  const floatingSearchIconPosition = import.meta.env
    .VITE_FLOATING_SEARCH_ICON_POSITION;
  const showFloatingSearchIcon =
    import.meta.env.VITE_SHOW_FLOATING_SEARCH_ICON == "true";
  const showFloatingInput = import.meta.env.VITE_SHOW_FLOATING_INPUT == "true";
  const defaultSearchQueries: string[] = (
    import.meta.env.VITE_DEFAULT_SEARCH_QUERIES ?? ""
  ).split(",");
  const defaultSearchMode =
    import.meta.env.VITE_DEFAULT_SEARCH_MODE ?? "search";
  const showResultHighlights =
    import.meta.env.VITE_SHOW_RESULT_HIGHLIGHTS == "true";

  const defaultAiQuestions = (
    import.meta.env.VITE_DEFAULT_AI_QUESTIONS ??
    "What is Trieve?,How to perform autocomplete search?,How do I install the TS SDK?"
  ).split(",");
  const inlineCarousel = import.meta.env.VITE_INLINE_CAROUSEL == "true";

  const [theme, setTheme] = useState<"light" | "dark">("light");

  return (
    <>
      <div
        className={`p-12 flex flex-col items-center justify-center w-screen h-screen relative ${
          theme === "dark" ? "bg-black text-zinc-50" : ""
        }`}>
        <div className="absolute top-6 right-6">
          <ul>
            <li key="theme">
              <button
                onClick={() => setTheme(theme === "light" ? "dark" : "light")}>
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
        <h2 className="font-bold text-center py-8">Search Modal Component </h2>

        <TrieveModalSearch
          debounceMs={50}
          defaultSearchMode={defaultSearchMode}
          apiKey={apiKey}
          baseUrl={baseUrl}
          datasetId={datasetId}
          problemLink={problemLink}
          type="docs"
          defaultSearchQueries={defaultSearchQueries}
          theme={theme}
          showResultHighlights={showResultHighlights}
          buttonTriggers={[
            {
              selector: ".random-trigger-location",
              mode: "chat",
            },
          ]}
          useGroupSearch={useGroupSearch}
          usePagefind={usePagefind}
          defaultAiQuestions={defaultAiQuestions}
          brandLogoImgSrcUrl={brandLogoSrcUrl}
          brandName={brandName}
          brandColor={brandColor}
          brandFontFamily={brandFontFamily}
          allowSwitchingModes={true}
          responsive={false}
          cssRelease="none"
          searchOptions={{
            use_autocomplete: false,
            search_type: "fulltext",
            score_threshold: 0.1,
          }}
          floatingButtonPosition={floatingButtonPosition}
          floatingSearchIconPosition={floatingSearchIconPosition}
          showFloatingButton={showFloatingButton}
          showFloatingSearchIcon={showFloatingSearchIcon}
          inlineCarousel={inlineCarousel}
          showFloatingInput={showFloatingInput}
          inline={false}
        />
      </div>
    </>
  );
}
