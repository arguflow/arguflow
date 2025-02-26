import React, { startTransition } from "react";
import { useModalState } from "../utils/hooks/modal-context";
import { SparklesIcon } from "./icons";

export const FloatingActionButton = () => {
  const { props, setOpen, setMode } = useModalState();

  const setButtonPosition = (position: string) => {
    switch (position) {
      case "top-left":
        return { top: "20px", left: "20px" };
      case "top-right":
        return { top: "20px", right: "20px" };
      case "bottom-left":
        return { bottom: "20px", left: "20px" };
      case "bottom-right":
        return { bottom: "20px", right: "20px" };
      default:
        return { bottom: "20px", right: "20px" };
    }
  };

  return (
    <button
      onClick={() => {
        startTransition(() => {
          setOpen(true);
          setMode("chat");
        });
      }}
      className={`floating-action-button${props.theme === "dark" ? " dark" : ""}`}
      style={{
        ...setButtonPosition(props.floatingButtonPosition || "bottom-right"),
        zIndex: (props.zIndex ?? 1000) - 1,
      }}
    >
      {props.brandLogoImgSrcUrl ? (
        <img src={props.brandLogoImgSrcUrl} alt="Brand Logo" />
      ) : (
        <SparklesIcon width={20} height={20} />
      )}
      Ask AI
    </button>
  );
};
