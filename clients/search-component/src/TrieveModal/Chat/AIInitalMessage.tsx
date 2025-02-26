import React from "react";
import { useModalState } from "../../utils/hooks/modal-context";
import { SparklesIcon } from "../icons";

export const AIInitialMessage = () => {
  const { props } = useModalState();
  if (!props.initialAiMessage) return null;

  return (
    <div
      style={{
        display: props.initialAiMessage ? "flex" : "none",
      }}
    >
      <span className="ai-avatar assistant">
        {props.brandLogoImgSrcUrl ? (
          <img
            src={props.brandLogoImgSrcUrl}
            alt={props.brandName || "Brand logo"}
          />
        ) : (
          <SparklesIcon />
        )}
      </span>
      <span
        className="content tv-w-fit"
        dangerouslySetInnerHTML={{ __html: props.initialAiMessage }}
      ></span>
    </div>
  );
};
