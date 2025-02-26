/* eslint-disable @typescript-eslint/no-explicit-any */
import React, { useEffect, startTransition, useCallback } from "react";

import {
  ModalProps,
  ModalProvider,
  useModalState,
} from "../utils/hooks/modal-context";
import { useKeyboardNavigation } from "../utils/hooks/useKeyboardNavigation";
import { OpenModalButton } from "./OpenModalButton";
import { ChatProvider, useChatState } from "../utils/hooks/chat-context";
import r2wc from "@r2wc/react-to-web-component";
import { setClickTriggers } from "../utils/hooks/setClickTriggers";
import { ChunkGroup } from "trieve-ts-sdk";
import { FloatingActionButton } from "./FloatingActionButton";
import { FloatingSearchIcon } from "./FloatingSearchIcon";
import { FloatingSearchInput } from "./FloatingSearchInput";
import { ModalContainer } from "./ModalContainer";

const Modal = () => {
  useKeyboardNavigation();
  const { open, setOpen, setMode, setQuery, props } = useModalState();
  const { askQuestion, chatWithGroup, cancelGroupChat, clearConversation } =
    useChatState();

  const onViewportResize = useCallback(() => {
    const viewportHeight = window.visualViewport?.height;
    if (props.inline) {
      return;
    }

    const trieveSearchModal = document.querySelector(
      "#trieve-search-modal"
    ) as HTMLElement;

    const chatModalWrapper = document.querySelector(".chat-modal-wrapper");

    if ((window.visualViewport?.width ?? 1000) <= 640) {
      if (!props.inline) {
        if (trieveSearchModal) {
          (trieveSearchModal as HTMLElement).style.maxHeight =
            `calc(${viewportHeight}px - 48px)`;
        }
      }
    }

    if (chatModalWrapper) {
      chatModalWrapper.scrollTo({
        top: chatModalWrapper.scrollHeight,
        behavior: "smooth",
      });
    }
  }, [open]);

  useEffect(() => {
    onViewportResize();
    window.addEventListener("resize", onViewportResize);

    return () => {
      window.removeEventListener("resize", onViewportResize);
    };
  }, [open]);

  useEffect(() => {
    if (!(Object as any).hasOwn) {
      (Object as any).hasOwn = (obj: any, prop: any) =>
        Object.prototype.hasOwnProperty.call(obj, prop);
    }
  });

  useEffect(() => {
    setClickTriggers(setOpen, setMode, props);
  }, []);

  const chatWithGroupListener: EventListener = useCallback((e: Event) => {
    try {
      const customEvent = e as CustomEvent<{
        message?: string;
        group: ChunkGroup;
        betterGroupName?: string;
      }>;
      if (customEvent.detail.group && !props.inline) {
        setOpen(true);
        if (customEvent.detail.betterGroupName) {
          customEvent.detail.group.name = customEvent.detail.betterGroupName;
        }
        clearConversation();
        chatWithGroup(
          customEvent.detail.group,
          customEvent.detail.betterGroupName
        );
        if (customEvent.detail.message) {
          askQuestion(customEvent.detail.message, customEvent.detail.group);
        }
      }
    } catch (e) {
      console.log("error on event listener for group chat", e);
    }
  }, []);

  const openWithTextListener: EventListener = useCallback((e: Event) => {
    try {
      const customEvent = e as CustomEvent<{
        text: string;
      }>;

      const defaultMode = props.defaultSearchMode || "search";
      if (props.inline) return;

      if (defaultMode === "chat") {
        setOpen(true);
        setMode("chat");
        cancelGroupChat();

        askQuestion(customEvent.detail.text);
      } else {
        setOpen(true);
        setMode("search");
        setQuery(customEvent.detail.text);
      }
    } catch (e) {
      console.log("error on event listener for group chat", e);
    }
  }, []);

  const closeModalListener: EventListener = useCallback(() => {
    try {
      setOpen(false);
    } catch (e) {
      console.log("error on event listener for closing modal", e);
    }
  }, []);

  const openModalListener: EventListener = useCallback(() => {
    try {
      const defaultMode = props.defaultSearchMode || "search";
      if (props.inline) return;

      if (defaultMode === "chat") {
        startTransition(() => {
          setMode("chat");
          cancelGroupChat();
          setOpen(true);
        });
      } else {
        startTransition(() => {
          setOpen(true);
          setMode("search");
        });
      }
    } catch (e) {
      console.log("error on event listener for opening modal", e);
    }
  }, []);

  useEffect(() => {
    const script = document.createElement("script");
    script.src =
      "https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.7.1/js/all.min.js";
    script.setAttribute("data-auto-replace-svg", "");

    document.head.appendChild(script);

    if (!props.ignoreEventListeners) {
      window.addEventListener(
        "trieve-start-chat-with-group",
        chatWithGroupListener
      );
      window.addEventListener("trieve-open-with-text", openWithTextListener);

      window.addEventListener("trieve-open-modal", openModalListener);

      window.addEventListener("trieve-close-modal", closeModalListener);
    }

    return () => {
      if (!props.ignoreEventListeners) {
        window.removeEventListener(
          "trieve-start-chat-with-group",
          chatWithGroupListener
        );

        window.addEventListener("trieve-open-modal", openModalListener);

        window.removeEventListener(
          "trieve-open-with-text",
          openWithTextListener
        );

        window.addEventListener("trieve-close-modal", closeModalListener);
      }
    };
  }, []);

  useEffect(() => {
    document.documentElement.style.setProperty(
      "--tv-prop-brand-color",
      props.brandColor ?? "#CB53EB"
    );

    if (props.theme === "dark") {
      document.documentElement.style.setProperty(
        "--tv-prop-scrollbar-thumb-color",
        "var(--tv-zinc-700)"
      );
    } else {
      document.documentElement.style.setProperty(
        "--tv-prop-scrollbar-thumb-color",
        "var(--tv-zinc-300)"
      );
    }

    document.documentElement.style.setProperty(
      "--tv-prop-brand-font-family",
      props.brandFontFamily ??
        `Maven Pro, ui-sans-serif, system-ui, sans-serif,
    "Apple Color Emoji", "Segoe UI Emoji", "Segoe UI Symbol", "Noto Color Emoji"`
    );
  }, [props.brandColor, props.brandFontFamily]);

  return (
    <>
      {!props.inline && !props.hideOpenButton && (
        <OpenModalButton
          setOpen={() => {
            startTransition(() => {
              setOpen(true);
              setMode(props.defaultSearchMode || "search");
            });
          }}
        />
      )}
      <>
        {!props.inline && !props.hideOverlay && open && (
          <div
            onClick={() => {
              setOpen(false);
            }}
            id="trieve-search-modal-overlay"
            className="tv-bg-black/60 tv-w-screen tv-fixed tv-inset-0 tv-h-screen tv-animate-overlayShow tv-backdrop-blur-sm tv-block"
            style={{ zIndex: props.zIndex ?? 1000 }}
          ></div>
        )}
        <ModalContainer />
      </>
      {props.showFloatingSearchIcon && <FloatingSearchIcon />}
      {props.showFloatingButton && <FloatingActionButton />}
      {props.showFloatingInput && <FloatingSearchInput />}
    </>
  );
};

export const initTrieveModalSearch = (props: ModalProps) => {
  const ModalSearchWC = r2wc(() => <TrieveModalSearch {...props} />);

  if (!customElements.get("trieve-modal-search")) {
    customElements.define("trieve-modal-search", ModalSearchWC);
  }
};

export const TrieveModalSearch = (props: ModalProps) => {
  return (
    <ModalProvider onLoadProps={props}>
      <ChatProvider>
        <Modal />
      </ChatProvider>
    </ModalProvider>
  );
};
