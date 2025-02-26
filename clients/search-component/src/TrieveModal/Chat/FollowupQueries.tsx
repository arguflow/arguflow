import React from "react";
import { useChatState } from "../../utils/hooks/chat-context";
import { useFollowupQuestions } from "../../utils/hooks/useFollowupQuestions";
import { SparklesIcon } from "../icons";
import { useAutoAnimate } from "@formkit/auto-animate/react";

export const FollowupQueries = () => {
  const { isDoneReading, askQuestion } = useChatState();

  const { suggestedQuestions, isLoadingSuggestedQueries } =
    useFollowupQuestions();

  const [parent] = useAutoAnimate();

  if (!isDoneReading) {
    return null;
  }

  return (
    <div ref={parent} className="followup-questions">
      {suggestedQuestions?.map((q) => (
        <button
          onClick={() => {
            askQuestion(q);
          }}
          key={q}
          className={`followup-question ${
            isLoadingSuggestedQueries ? "loading" : ""
          }`}
        >
          <SparklesIcon className="followup-icon" />
          {q}
        </button>
      ))}
    </div>
  );
};
