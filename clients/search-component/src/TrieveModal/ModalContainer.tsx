import React, { Suspense } from "react";
import { useModalState } from "../utils/hooks/modal-context";
import { useAtom } from "jotai";
import { PdfViewer, pdfViewState } from "./PdfView/PdfViewer";
import { ChatModeSwitch } from "./ModeSwitch";
import { cn } from "../utils/styles";
import { Footer } from "./Footer";
import { useSizing } from "../utils/hooks/useSizing";
import SearchMode from "./Search/SearchMode";
import ChatMode from "./Chat/ChatMode";

export const ModalContainer = () => {
  const { mode, props, open } = useModalState();
  const [fullscreenPdfState] = useAtom(pdfViewState);

  const componentClass = useSizing();

  return (
    <div
      id="trieve-search-modal"
      className={cn(
        "tv-flex tv-flex-col tv-items-stretch",
        "tv-scroll-smooth tv-rounded-lg focus:tv-outline-none tv-overflow-hidden tv-text-base tv-text-zinc-950 tv-bg-white",
        `${mode === "chat" ? "chat-modal-mobile" : ""}${
          props.theme === "dark" ? " dark tv-dark" : ""
        }${
          props.inline
            ? " trieve-inline-modal tv-trieve-inline-modal"
            : " trieve-popup-modal"
        } ${props.type}`.trim(),
        !props.inline && "md:tv-resize-y",
        props.inline && "tv-border-2",
        componentClass
      )}
      style={{
        zIndex: (props.zIndex ?? 1000) + 1,
        display: open || props.inline ? "flex" : "none",
      }}
    >
      {!props.inline && !fullscreenPdfState && <ChatModeSwitch />}
      <Suspense>
        {mode === "search" && !fullscreenPdfState && <SearchMode />}
      </Suspense>
      {mode === "chat" && !fullscreenPdfState && <ChatMode />}
      {fullscreenPdfState && <PdfViewer {...fullscreenPdfState} />}
      <Footer />
    </div>
  );
};
