import React, { Suspense, useRef } from "react";
import { useModalState } from "../../utils/hooks/modal-context";
import { useChatState } from "../../utils/hooks/chat-context";
import { ResponseMessage } from "./ResponseMessage";
import { useIntersectionObserver } from "react-intersection-observer-hook";
import { SuggestedQuestions } from "./SuggestedQuestions";
import { FollowupQueries } from "./FollowupQueries";
import { UserMessage } from "./UserMessage";
import { InlineChatHeader } from "./InlineChatHeader";
import { ChatInput } from "./ChatInput";
import { useChatHeight } from "../../utils/hooks/useChatHeight";

export const ChatMode = () => {
  const { modalRef } = useModalState();
  const { messages } = useChatState();

  const actualChatRef = useRef<HTMLDivElement>(null);
  const { minHeight, resetHeight } = useChatHeight(actualChatRef);

  const [ref, { entry }] = useIntersectionObserver();
  const isOnScreen = entry && entry.isIntersecting;

  return (
    <Suspense>
      <InlineChatHeader resetHeight={resetHeight} />
      <div
        ref={modalRef}
        className="chat-modal-wrapper tv-justify-items-stretch tv-flex-grow tv-pt-3 tv-pb-2 tv-px-2 tv-relative tv-overflow-y-auto tv-flex"
      >
        <ChatRuler minHeight={minHeight} />
        <div
          className="tv-flex-col tv-h-full tv-grow tv-flex tv-gap-4"
          ref={actualChatRef}
        >
          <SuggestedQuestions /> {/* Only shows with zero messages */}
          {messages.map((message, i) => {
            if (message.type === "user") {
              return <UserMessage key={i} message={message} idx={i} />;
            } else {
              return <ResponseMessage key={i} message={message} idx={i} />;
            }
          })}
          <FollowupQueries />
          <div
            ref={ref}
            className="tv-z-50 tv-mx-4 tv-w-4 tv-min-h-1 tv-h-1"
          ></div>
        </div>
      </div>
      <ChatInput showShadow={!isOnScreen} />
    </Suspense>
  );
};

// sits on the left side of chat in a flexbox to enforce the minimum height and control scroll
const ChatRuler = ({ minHeight }: { minHeight: number }) => {
  return (
    <div
      style={{
        minHeight,
      }}
    ></div>
  );
};

export default ChatMode;
