/* eslint-disable @typescript-eslint/no-explicit-any */
import React, { createContext, useContext, useRef, useState } from "react";
import { useModalState } from "./modal-context";
import { Chunk } from "../types";
import { getFingerprint } from "@thumbmarkjs/thumbmarkjs";
import { useEffect } from "react";
import { trackViews } from "../trieve";
import { ChunkFilter, ChunkGroup, ToolFunctionParameter } from "trieve-ts-sdk";
import { defaultHighlightOptions } from "../highlight";

const scrollToBottomOfChatModalWrapper = () => {
  const chatModal = document.querySelector(".chat-modal-wrapper");
  if (chatModal) {
    chatModal.scrollTo({
      top: chatModal.scrollHeight,
      behavior: "smooth",
    });
  }
};

export type Messages = {
  queryId: string | null;
  type: string;
  text: string;
  additional: Chunk[] | null;
}[];

const ChatContext = createContext<{
  askQuestion: (question?: string, group?: ChunkGroup) => Promise<void>;
  isLoading: boolean;
  messages: Messages;
  currentQuestion: string;
  setCurrentQuestion: React.Dispatch<React.SetStateAction<string>>;
  stopGeneratingMessage: () => void;
  clearConversation: () => void;
  switchToChatAndAskQuestion: (query: string) => Promise<void>;
  cancelGroupChat: () => void;
  chatWithGroup: (group: ChunkGroup, betterGroupName?: string) => void;
  isDoneReading?: boolean;
  rateChatCompletion: (isPositive: boolean, queryId: string | null) => void;
}>({
  askQuestion: async () => {},
  currentQuestion: "",
  isLoading: false,
  messages: [],
  setCurrentQuestion: () => {},
  cancelGroupChat: () => {},
  clearConversation: () => {},
  chatWithGroup: () => {},
  switchToChatAndAskQuestion: async () => {},
  stopGeneratingMessage: () => {},
  rateChatCompletion: () => {},
});

function ChatProvider({ children }: { children: React.ReactNode }) {
  const {
    query,
    trieveSDK,
    mode,
    setMode,
    setCurrentGroup,
    imageUrl,
    setImageUrl,
    audioBase64,
    setAudioBase64,
  } = useModalState();
  const [currentQuestion, setCurrentQuestion] = useState(query);
  const [currentTopic, setCurrentTopic] = useState("");
  const called = useRef(false);
  const [messages, setMessages] = useState<Messages>([]);
  const [isLoading, setIsLoading] = useState(false);
  const chatMessageAbortController = useRef<AbortController>(
    new AbortController(),
  );
  const [isDoneReading, setIsDoneReading] = useState(true);

  const createTopic = async ({ question }: { question: string }) => {
    if (!currentTopic) {
      called.current = true;
      setIsLoading(true);
      setCurrentQuestion("");
      const fingerprint = await getFingerprint();
      const topic = await trieveSDK.createTopic({
        name: currentQuestion,
        owner_id: fingerprint.toString(),
      });
      setCurrentTopic(topic.id);
      createQuestion({ id: topic.id, question: question });
    }
  };

  const clearConversation = () => {
    setCurrentTopic("");
    setMessages([]);
  };

  const { selectedTags, currentGroup, props } = useModalState();

  useEffect(() => {
    if (props.groupTrackingId) {
      trieveSDK
        .getGroupByTrackingId({
          trackingId: props.groupTrackingId,
        })
        .then((fetchedGroup) => {
          if (fetchedGroup) {
            chatWithGroup(fetchedGroup, props.cleanGroupName);
          }
        })
        .catch((e) => {
          console.error(e);
        });
    }
  }, []);

  useEffect(() => {
    if (mode == "chat" && audioBase64 && audioBase64 != "") {
      askQuestion(" ");
    }
  }, [audioBase64, mode]);

  const handleReader = async (
    reader: ReadableStreamDefaultReader<Uint8Array>,
    queryId: string | null,
  ) => {
    setIsLoading(true);
    setIsDoneReading(false);
    let done = false;
    let calledAnalytics = false;
    let textInStream = "";
    let state: "READING_TEXT" | "READING_LABEL" | "READING_URL" =
      "READING_TEXT";
    let linkBuffer = "";
    let outputBuffer = "";

    while (!done) {
      const { value, done: doneReading } = await reader.read();
      if (doneReading) {
        done = doneReading;
        setIsDoneReading(true);
      } else if (value) {
        const decoder = new TextDecoder();
        const newText = decoder.decode(value);
        textInStream += newText;

        let text: string = "";
        let jsonData: string = "";

        if (textInStream.includes("||")) {
          [jsonData, text] = textInStream.split("||");
        }

        let json;
        try {
          json = JSON.parse(jsonData);
        } catch {
          json = null;
        }

        if (json && props.analytics && !calledAnalytics) {
          calledAnalytics = true;
          const ecommerceChunks = (json as unknown as Chunk[]).filter(
            (chunk) =>
              (chunk.metadata.heading ||
                chunk.metadata.title ||
                chunk.metadata.page_title) &&
              chunk.link &&
              chunk.image_urls?.length &&
              chunk.num_value,
          );
          if (ecommerceChunks && queryId) {
            trackViews({
              trieve: trieveSDK,
              requestID: queryId,
              type: "rag",
              items: ecommerceChunks.map((chunk) => {
                return chunk.id ?? "";
              }),
            });
          }
        }

        outputBuffer = "";
        linkBuffer = "";
        state = "READING_TEXT";
        for (let i = 0; i < text.length; i++) {
          const char = text[i];

          switch (state) {
            case "READING_TEXT":
              if (char === "[") {
                state = "READING_LABEL";
                linkBuffer = "[";
              } else {
                outputBuffer += char;
              }
              break;

            case "READING_LABEL":
              linkBuffer += char;
              if (char === "]") {
                state = "READING_URL";
              }
              break;

            case "READING_URL":
              linkBuffer += char;
              if (char === ")") {
                state = "READING_TEXT";
                outputBuffer += linkBuffer;
                linkBuffer = "";
              }
              break;
          }
        }

        setMessages((m) => [
          ...m.slice(0, -1),
          {
            type: "system",
            text: outputBuffer,
            additional: json ? json : null,
            queryId,
          },
        ]);
      }
    }
  };

  const createQuestion = async ({
    id,
    question,
    group,
  }: {
    id?: string;
    question?: string;
    group?: ChunkGroup;
  }) => {
    setIsLoading(true);
    let curAudioBase64 = audioBase64;
    let questionProp = question;
    const curGroup = group || currentGroup;
    let transcribedQuery: string | null = null;

    // Use group search
    let filters: ChunkFilter | null = {
      must: null,
      must_not: null,
      should: null,
    };

    if (selectedTags.length > 0) {
      filters.should = [];
      filters.should?.push({
        field: "tag_set",
        match_any: selectedTags.map((t) => t.tag),
      });
    }

    if (curGroup) {
      if (!filters.should) {
        filters.should = [];
      }
      filters.should?.push({
        field: "group_ids",
        match_all: [curGroup.id],
      });
    }

    if (props.chatFilters) {
      if (props.chatFilters.must) {
        if (!filters.must) {
          filters.must = [];
        }
        filters.must?.push(...props.chatFilters.must);
      }
      if (props.chatFilters.must_not) {
        if (!filters.must_not) {
          filters.must_not = [];
        }
        filters.must_not?.push(...props.chatFilters.must_not);
      }
      if (props.chatFilters.should) {
        if (!filters.should) {
          filters.should = [];
        }
        filters.should?.push(...props.chatFilters.should);
      }
    }

    let stoppedGeneratingMessage = false;

    if (!curGroup && (props.tags?.length ?? 0) > 0) {
      let filterParamsRetries = 0;
      while (filterParamsRetries < 3) {
        filterParamsRetries++;
        chatMessageAbortController.current = new AbortController();
        const toolCallTimeout = setTimeout(
          () => {
            console.error(
              "getToolCallFunctionParams timeout on retry: ",
              filterParamsRetries,
            );
            chatMessageAbortController.current.abort();
          },
          imageUrl || curAudioBase64 ? 20000 : 10000,
        );

        try {
          const filterParamsResp = await trieveSDK.getToolCallFunctionParams(
            {
              user_message_text:
                questionProp || currentQuestion
                  ? `Get filters from the following messages: ${messages
                      .slice(0, -1)
                      .filter((message) => {
                        return message.type == "user";
                      })
                      .map(
                        (message) => `\n\n${message.text}`,
                      )} \n\n ${questionProp || currentQuestion}`
                  : null,
              image_url: imageUrl ? imageUrl : null,
              audio_input: curAudioBase64 ? curAudioBase64 : null,
              tool_function: {
                name: "get_filters",
                description:
                  "Decide on which filters to apply to available catalog being used within the knowledge base to respond. Always get filters.",
                parameters:
                  props.tags?.map((tag) => {
                    return {
                      name: tag.label,
                      parameter_type: "boolean",
                      description: tag.description ?? "",
                    } as ToolFunctionParameter;
                  }) ?? [],
              },
            },
            chatMessageAbortController.current.signal,
            (headers: Record<string, string>) => {
              if (headers["x-tr-query"] && curAudioBase64) {
                transcribedQuery = headers["x-tr-query"];
              }
            },
          );

          if (transcribedQuery && curAudioBase64) {
            questionProp = transcribedQuery;
            setAudioBase64("");
            curAudioBase64 = undefined;
            setMessages((m) => {
              return [
                ...m.slice(0, -2),
                {
                  type: "user",
                  text: transcribedQuery ?? "",
                  additional: null,
                  queryId: null,
                  imageUrl: imageUrl ? imageUrl : null,
                },
                {
                  type: "system",
                  text: "Loading...",
                  additional: null,
                  queryId: null,
                },
              ];
            });
          }

          const match_any_tags = [];
          for (const key of Object.keys(filterParamsResp.parameters ?? {})) {
            const filterParam = (filterParamsResp.parameters as any)[
              key as keyof typeof filterParamsResp.parameters
            ];
            if (typeof filterParam === "boolean" && filterParam) {
              const tag = props.tags?.find((t) => t.label === key)?.tag;
              if (tag) {
                match_any_tags.push(tag);
              }
            }
          }

          if (match_any_tags.length > 0) {
            if (!filters.should) {
              filters.should = [];
            }
            filters.should.push({
              field: "tag_set",
              match_any: match_any_tags,
            });
          }

          clearTimeout(toolCallTimeout);

          break;
        } catch (e) {
          console.error("error getting getToolCallFunctionParams", e);
          clearTimeout(toolCallTimeout);
          if (e && typeof e == "string" && e === "Stopped generating message") {
            stoppedGeneratingMessage = true;
            break;
          }
        }
      }
    }

    if (
      filters.must == null &&
      filters.must_not == null &&
      filters.should == null
    ) {
      filters = null;
    }

    let messageReaderRetries = 0;
    let {
      reader,
      queryId,
    }: {
      reader: ReadableStreamDefaultReader<Uint8Array> | null;
      queryId: string | null;
    } = { reader: null, queryId: null };

    while (!stoppedGeneratingMessage && messageReaderRetries < 5) {
      messageReaderRetries++;
      chatMessageAbortController.current = new AbortController();
      const createMessageTimeout = setTimeout(
        () => {
          console.error(
            "createMessageReaderWithQueryId timeout on retry: ",
            messageReaderRetries,
          );
          chatMessageAbortController.current.abort();
        },
        imageUrl || curAudioBase64 ? 20000 : 10000,
      );

      try {
        const result = await trieveSDK.createMessageReaderWithQueryId(
          {
            topic_id: id || currentTopic,
            new_message_content: questionProp || currentQuestion,
            audio_input:
              curAudioBase64 && curAudioBase64?.length > 0
                ? curAudioBase64
                : undefined,
            image_urls: imageUrl ? [imageUrl] : [],
            llm_options: {
              completion_first: false,
            },
            concat_user_messages_query: true,
            user_id: `${props.componentName}`,
            page_size: props.searchOptions?.page_size ?? 8,
            score_threshold: props.searchOptions?.score_threshold || null,
            use_group_search: props.useGroupSearch,
            filters: filters,
            highlight_options: {
              ...defaultHighlightOptions,
              highlight_delimiters: ["?", ",", ".", "!", "\n"],
              highlight_window: props.type === "ecommerce" ? 5 : 10,
              highlight_results: true,
            },
            only_include_docs_used: true,
          },
          chatMessageAbortController.current.signal,
          (headers: Record<string, string>) => {
            if (headers["x-tr-query"] && curAudioBase64) {
              transcribedQuery = headers["x-tr-query"];
            }
          },
        );
        reader = result.reader;
        queryId = result.queryId;

        clearTimeout(createMessageTimeout);

        break;
      } catch (e) {
        console.error("error getting createMessageReaderWithQueryId", e);
        clearTimeout(createMessageTimeout);
        if (e && typeof e == "string" && e === "Stopped generating message") {
          stoppedGeneratingMessage = true;
          break;
        }
      }
    }

    if (transcribedQuery && curAudioBase64) {
      setAudioBase64("");
      curAudioBase64 = undefined;
      setMessages((m) => [
        ...m.slice(0, -2),
        {
          type: "user",
          text: transcribedQuery ?? "",
          additional: null,
          queryId: null,
          imageUrl: imageUrl ? imageUrl : null,
        },
        {
          type: "system",
          text: "Loading...",
          additional: null,
          queryId: null,
        },
      ]);
    }

    if (reader) handleReader(reader, queryId);

    if (imageUrl) {
      setImageUrl("");
    }
    if (audioBase64) {
      setAudioBase64("");
    }
  };

  const chatWithGroup = (group: ChunkGroup, betterGroupName?: string) => {
    if (betterGroupName) {
      group.name = betterGroupName;
    }
    clearConversation();
    setCurrentGroup(group);
    setMode("chat");
  };

  const stopGeneratingMessage = () => {
    chatMessageAbortController.current.abort("Stopped generating message");
    chatMessageAbortController.current = new AbortController();
    setIsDoneReading(true);
    setIsLoading(false);

    if (messages.at(-1)?.text === "Loading...") {
      setMessages((messages) => [
        ...messages.slice(0, -1),
        messages[messages.length - 1],
      ]);
    }
  };

  const cancelGroupChat = () => {
    setCurrentGroup(null);
    clearConversation();
  };

  const askQuestion = async (
    question?: string,
    group?: ChunkGroup,
    retry?: boolean,
  ) => {
    const questionProp = question;
    setIsDoneReading(false);
    setCurrentQuestion("");

    if (props.groupTrackingId) {
      const fetchedGroup = await trieveSDK.getGroupByTrackingId({
        trackingId: props.groupTrackingId,
      });
      if (fetchedGroup) {
        group = {
          created_at: fetchedGroup.created_at,
          updated_at: fetchedGroup.updated_at,
          dataset_id: fetchedGroup.dataset_id,
          description: fetchedGroup.description,
          id: fetchedGroup.id,
          metadata: fetchedGroup.metadata,
          name: props.cleanGroupName ? props.cleanGroupName : fetchedGroup.name,
          tag_set: fetchedGroup.tag_set,
        } as ChunkGroup;
      }
    }

    if (!currentGroup && group) {
      chatWithGroup(group);
      setCurrentGroup(group);
    }

    if (!audioBase64) {
      if (question == undefined || question == null || question == "") {
        question = props.defaultImageQuestion;
      }

      if (!retry) {
        setMessages((m) => [
          ...m,
          {
            type: "user",
            text: questionProp || currentQuestion,
            additional: null,
            queryId: null,
            imageUrl: imageUrl ? imageUrl : null,
          },
          {
            type: "system",
            text: "Loading...",
            additional: null,
            queryId: null,
          },
        ]);
      }
    } else {
      if (!retry) {
        setMessages((m) => [
          ...m,
          {
            type: "user",
            text: "Loading...",
            additional: null,
            queryId: null,
            imageUrl: imageUrl ? imageUrl : null,
          },
          {
            type: "system",
            text: "Loading...",
            additional: null,
            queryId: null,
          },
        ]);
      }
    }
    scrollToBottomOfChatModalWrapper();

    if (!currentTopic) {
      await createTopic({ question: questionProp || currentQuestion });
    } else {
      await createQuestion({
        question: questionProp || currentQuestion,
        group,
      });
    }
  };

  const switchToChatAndAskQuestion = async (query: string) => {
    setMode("chat");
    await askQuestion(query);
  };

  const rateChatCompletion = async (
    isPositive: boolean,
    queryId: string | null,
  ) => {
    if (queryId) {
      trieveSDK.rateRagQuery({
        rating: isPositive ? 1 : 0,
        query_id: queryId,
        metadata: {
          component_name: props.componentName,
        },
      });
    }
  };

  return (
    <ChatContext.Provider
      value={{
        askQuestion,
        isLoading,
        cancelGroupChat,
        messages,
        currentQuestion,
        chatWithGroup,
        setCurrentQuestion,
        switchToChatAndAskQuestion,
        clearConversation,
        stopGeneratingMessage,
        isDoneReading,
        rateChatCompletion,
      }}>
      {children}
    </ChatContext.Provider>
  );
}

function useChatState() {
  const context = useContext(ChatContext);
  if (!context) {
    throw new Error("useChatState must be used within a ChatProvider");
  }
  return context;
}

export { ChatProvider, useChatState };
