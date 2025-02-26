import { useEffect, useMemo, useState } from "react";
import { getSuggestedQuestions } from "../trieve";
import { useModalState } from "./modal-context";
import { useChatState } from "./chat-context";

export const useFollowupQuestions = () => {
  const { trieveSDK, currentGroup, props } = useModalState();
  const { messages, isDoneReading } = useChatState();
  const [isLoading, setIsLoading] = useState(false);
  const [suggestedQuestions, setSuggestedQuestions] = useState<
    Record<string, string[]>
  >({});

  const getFollowUpQuestions = async () => {
    setIsLoading(true);
    const prevUserMessages =
      messages
        .filter((msg) => {
          return msg.type == "user";
        }) ?? messages;

    const prevChunks =
      messages
        .filter((msg) => {
          return msg.type == "system";
        }).slice(-1)[0].additional ?? messages.slice(-1)[0].additional;

    const prevMessage = prevUserMessages.slice(-1)[0];

    if (!prevMessage) {
      setIsLoading(false);
      return;
    }

    const queries = await getSuggestedQuestions({
      trieve: trieveSDK,
      query: prevMessage.text,
      count: props.numberOfSuggestions ?? 3,
      groupTrackingId: props.inline
        ? (props.groupTrackingId ?? currentGroup?.tracking_id)
        : currentGroup?.tracking_id,
      is_followup: true,
      prevUserMessages: prevUserMessages.map((message) => message.text),
      chunks: prevChunks,
      props,
    });
    setSuggestedQuestions((prev) => ({
      ...prev,
      [prevMessage.text]: queries.queries.map((q) => {
        return q.replace(/^[\d.-]+\s*/, "").trim();
      }),
    }));
    setIsLoading(false);
  };

  useEffect(() => {
    if (!isDoneReading) {
      return;
    }
    setIsLoading(true);
    const abortController = new AbortController();

    const timeoutId = setTimeout(async () => {
      getFollowUpQuestions();
    });

    return () => {
      clearTimeout(timeoutId);
      abortController.abort("fetch aborted");
    };
  }, [messages, isDoneReading]);

  const filteredSuggestedQuestions = useMemo(() => {
    const prevMessage =
      messages
        .filter((msg) => {
          return msg.type == "user";
        })
        .slice(-1)[0] ?? messages.slice(-1)[0];

    if (!prevMessage) {
      return [];
    }

    return prevMessage.text in suggestedQuestions
      ? suggestedQuestions[prevMessage.text]
      : [];
  }, [messages, suggestedQuestions]);

  return {
    suggestedQuestions: filteredSuggestedQuestions,
    isLoadingSuggestedQueries: isLoading,
  };
};
