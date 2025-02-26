import React from "react";
import { useChatState } from "../../utils/hooks/chat-context";
import { useModalState } from "../../utils/hooks/modal-context";

export const InlineChatHeader = ({
  resetHeight,
}: {
  resetHeight: () => void;
}) => {
  const { props } = useModalState();
  const { messages, clearConversation, isDoneReading, stopGeneratingMessage } =
    useChatState();

  if (!props.inline) {
    return null;
  }

  if (!messages.length) {
    return null;
  }
  return (
    <div
      className={`tv-text-xs tv-rounded-md !tv-bg-transparent tv-flex !hover:bg-tv-zinc-200 tv-px-2 tv-justify-end tv-items-center tv-p-2 tv-gap-0.5 tv-cursor-pointer ${props.type}`}
      onClick={() => {
        if (isDoneReading) {
          resetHeight();
          clearConversation();
        } else {
          stopGeneratingMessage();
        }
      }}
      style={{ display: props.inline && messages.length ? "flex" : "none" }}
    >
      <svg
        className="close-icon"
        xmlns="http://www.w3.org/2000/svg"
        width="24"
        height="24"
        viewBox="0 0 24 24"
        fill="none"
        stroke="currentColor"
        strokeWidth="2"
        strokeLinecap="round"
        strokeLinejoin="round"
      >
        <path stroke="none" d="M0 0h24v24H0z" fill="none" />
        <line x1="18" y1="6" x2="6" y2="18" />
        <line x1="6" y1="6" x2="18" y2="18" />
      </svg>
      <span>{isDoneReading ? "Clear" : "Stop"} </span>
    </div>
  );
};
