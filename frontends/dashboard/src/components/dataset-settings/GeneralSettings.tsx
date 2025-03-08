import { Tooltip } from "shared/ui";
import { AiOutlineInfoCircle } from "solid-icons/ai";
import { For, Accessor } from "solid-js";
import { DatasetConfig } from "./LegacySettingsWrapper";
import {
  availableDistanceMetrics,
  availableEmbeddingModels,
  availableRerankerModels,
} from "shared/types";

export const GeneralServerSettings = (props: {
  serverConfig: Accessor<DatasetConfig>;
  setServerConfig: (config: (prev: DatasetConfig) => DatasetConfig) => void;
}) => {
  return (
    <form class="flex flex-col gap-3">
      {/* General LLM Settings */}

      {/* Embedding Settings */}
      <div
        class="rounded-md border shadow sm:overflow-hidden"
        id="embedding-settings"
      >
        <div class="rounded-md bg-white px-4 py-6 sm:p-6">
          <div>
            <h2 id="user-details-name" class="text-xl font-medium leading-6">
              Embedding Settings
            </h2>
            <p class="mt-1 text-sm text-neutral-600">
              Configure the embedding model and query parameters.
            </p>
          </div>

          <div class="mt-6 grid grid-cols-4 gap-6">
            <div class="col-span-4 space-y-1 sm:col-span-2">
              <div class="flex items-center">
                <label
                  for="embeddingSize"
                  class="mr-2 block text-sm font-medium leading-6"
                >
                  Embedding Model
                </label>
                <Tooltip
                  body={<AiOutlineInfoCircle />}
                  tooltipText="Embedding Model is only editable on creation"
                />
              </div>
              <select
                id="embeddingSize"
                aria-readonly
                title="Embedding Model is only editable on creation"
                disabled
                name="embeddingSize"
                class="col-span-2 block w-full cursor-not-allowed rounded-md border-[0.5px] border-neutral-300 bg-white px-3 py-1.5 shadow-sm placeholder:text-neutral-400 focus:outline-magenta-500 sm:text-sm sm:leading-6"
                value={
                  availableEmbeddingModels.find(
                    (model) =>
                      model.id === props.serverConfig().EMBEDDING_MODEL_NAME,
                  )?.name ?? availableEmbeddingModels[0].name
                }
              >
                <For each={availableEmbeddingModels}>
                  {(model) => <option value={model.name}>{model.name}</option>}
                </For>
              </select>
            </div>

            <div class="col-span-4 space-y-1 sm:col-span-2">
              <div class="flex items-center">
                <label
                  for="embeddingSize"
                  class="mr-2 block text-sm font-medium leading-6"
                >
                  Embedding Query Prefix
                </label>
                <Tooltip
                  body={<AiOutlineInfoCircle />}
                  tooltipText="For some embedding models, the training data includes query prefixes. The default for Jina is 'Search for: '. You can experiment with different values."
                />
              </div>
              <input
                type="text"
                name="embeddingQueryPrefix"
                id="embeddingQueryPrefix"
                class="block w-full rounded-md border-[0.5px] border-neutral-300 px-3 py-1.5 shadow-sm placeholder:text-neutral-400 focus:outline-magenta-500 sm:text-sm sm:leading-6"
                value={props.serverConfig().EMBEDDING_QUERY_PREFIX || ""}
                onInput={(e) =>
                  props.setServerConfig((prev) => {
                    return {
                      ...prev,
                      EMBEDDING_QUERY_PREFIX: e.currentTarget.value,
                    };
                  })
                }
              />
            </div>

            <div class="col-span-4 space-y-1 sm:col-span-2">
              <div class="flex items-center">
                <label
                  for="embeddingSize"
                  class="mr-2 block text-sm font-medium leading-6"
                >
                  Reranker Model
                </label>
                <Tooltip
                  body={<AiOutlineInfoCircle />}
                  tooltipText="Reranker Model for re-ranking search results."
                />
              </div>
              <select
                id="embeddingSize"
                aria-readonly
                title="Embedding Model is only editable on creation"
                name="embeddingSize"
                class="col-span-2 block w-full rounded-md border-[0.5px] border-neutral-300 bg-white px-3 py-1.5 shadow-sm placeholder:text-neutral-400 focus:outline-magenta-500 sm:text-sm sm:leading-6"
                value={
                  availableRerankerModels.find(
                    (metric) =>
                      metric.id === props.serverConfig().RERANKER_MODEL_NAME,
                  )?.name ?? availableRerankerModels[0].name
                }
                onChange={(e) => {
                  const selectedModel = availableRerankerModels.find(
                    (model) => model.name === e.currentTarget.value,
                  );

                  const url = selectedModel?.url ?? "";

                  props.setServerConfig((prev) => {
                    return {
                      ...prev,
                      RERANKER_MODEL_NAME: selectedModel?.id,
                      RERANKER_BASE_URL: url,
                    };
                  });
                }}
              >
                <For each={availableRerankerModels}>
                  {(metric) => (
                    <option value={metric.name}>{metric.name}</option>
                  )}
                </For>
              </select>
            </div>
            <div class="col-span-4 sm:col-span-2">
              <div class="flex flex-row items-center gap-2">
                <label
                  for="rerankerApiKey"
                  class="block text-sm font-medium leading-6"
                >
                  API Key (for either the AIMon reranker or the Cohere reranker)
                </label>
                <Tooltip
                  body={<AiOutlineInfoCircle />}
                  tooltipText="Sets the API key for either the AIMon reranker or the Cohere reranker, whichever is selected."
                />
              </div>
              <input
                type="text"
                name="rerankerApiKey"
                id="linesBeforeShowMore"
                class="block w-full rounded-md border-[0.5px] border-neutral-300 px-3 py-1.5 shadow-sm placeholder:text-neutral-400 focus:outline-magenta-500 sm:text-sm sm:leading-6"
                value={props.serverConfig().RERANKER_API_KEY ?? ""}
                onChange={(e) =>
                  props.setServerConfig((prev) => {
                    return {
                      ...prev,
                      RERANKER_API_KEY: e.currentTarget.value,
                    };
                  })
                }
              />
            </div>

            <div class="col-span-4 space-y-1 sm:col-span-2">
              <div class="flex items-center">
                <label
                  for="embeddingSize"
                  class="mr-2 block text-sm font-medium leading-6"
                >
                  Distance Metric
                </label>
                <Tooltip
                  body={<AiOutlineInfoCircle />}
                  tooltipText="Distance Metric is only editable on creation"
                />
              </div>
              <select
                id="embeddingSize"
                aria-readonly
                title="Embedding Model is only editable on creation"
                disabled
                name="embeddingSize"
                class="col-span-2 block w-full cursor-not-allowed rounded-md border-[0.5px] border-neutral-300 bg-white px-3 py-1.5 shadow-sm placeholder:text-neutral-400 focus:outline-magenta-500 sm:text-sm sm:leading-6"
                value={
                  availableDistanceMetrics.find(
                    (metric) =>
                      metric.id === props.serverConfig().DISTANCE_METRIC,
                  )?.name ?? availableEmbeddingModels[0].name
                }
              >
                <For each={availableDistanceMetrics}>
                  {(metric) => (
                    <option value={metric.name}>{metric.name}</option>
                  )}
                </For>
              </select>
            </div>
            {/* Task Definition textbox: Only if RERANKER_MODEL_NAME is "aimon-rerank" */}
            {props.serverConfig().RERANKER_MODEL_NAME === "aimon-rerank" && (
              <div class="col-span-4 space-y-1 sm:col-span-2">
                <div class="flex items-center">
                  <label
                    for="taskDefinition"
                    class="mr-2 block text-sm font-medium leading-6"
                  >
                    Task Definition (for AIMon reranker)
                  </label>
                  <Tooltip
                    body={<AiOutlineInfoCircle />}
                    tooltipText="Task definition can be used to specify the domain of the context documents for AIMon reranker."
                  />
                </div>
                <textarea
                  name="taskDefinition"
                  id="taskDefinition"
                  class="block w-full rounded-md border-[0.5px] border-neutral-300 px-3 py-1.5 shadow-sm placeholder:text-neutral-400 focus:outline-magenta-500 sm:text-sm sm:leading-6"
                  value={
                    props.serverConfig().TASK_DEFINITION ||
                    "Your task is to grade the relevance of context document(s) against the specified user query."
                  }
                  onInput={(e) =>
                    props.setServerConfig((prev) => {
                      return {
                        ...prev,
                        TASK_DEFINITION: e.currentTarget.value,
                      };
                    })
                  }
                  placeholder="Task definition can be used to specify the domain of the context documents for AIMon reranker."
                />
              </div>
            )}

          </div>
        </div>
      </div>

      {/* Additional Options */}
      <div
        class="rounded-md border shadow sm:overflow-hidden"
        id="additional-options"
      >
        <div class="rounded-md bg-white px-4 py-6 sm:p-6">
          <div>
            <h2 id="user-details-name" class="text-xl font-medium leading-6">
              Additional Options
            </h2>
            <p class="mt-1 text-sm text-neutral-600">
              Fine-tune server and model settings.
            </p>
          </div>

          <div class="mt-6 grid grid-cols-4 gap-6">
            <div class="col-span-4 sm:col-span-2">
              <div class="flex flex-row items-center gap-2">
                <label
                  for="maxLimit"
                  class="block text-sm font-medium leading-6"
                >
                  Max Count Limit
                </label>
                <Tooltip
                  body={<AiOutlineInfoCircle />}
                  tooltipText="Sets the maximum limit when counting chunks, applies to search and count routes in the API to prevent DDOS attacks on the server."
                />
              </div>
              <input
                type="number"
                name="maxLimit"
                id="linesBeforeShowMore"
                class="block w-full rounded-md border-[0.5px] border-neutral-300 px-3 py-1.5 shadow-sm placeholder:text-neutral-400 focus:outline-magenta-500 sm:text-sm sm:leading-6"
                value={props.serverConfig().MAX_LIMIT ?? 0}
                onChange={(e) =>
                  props.setServerConfig((prev) => {
                    return {
                      ...prev,
                      MAX_LIMIT: e.currentTarget.valueAsNumber,
                    };
                  })
                }
              />
            </div>

            <div class="col-span-4 flex items-center space-x-2 sm:col-span-2">
              <input
                type="checkbox"
                name="semanticEnabled"
                id="semanticEnabled"
                checked={!!props.serverConfig().SEMANTIC_ENABLED}
                onInput={(e) =>
                  props.setServerConfig((prev) => {
                    return {
                      ...prev,
                      SEMANTIC_ENABLED: e.currentTarget.checked,
                    };
                  })
                }
              />
              <label
                for="semanticEnabled"
                class="block text-sm font-medium leading-6"
              >
                Semantic Enabled
              </label>
            </div>

            <div class="col-span-4 flex items-center space-x-2 sm:col-span-2">
              <input
                type="checkbox"
                name="disableAnalytics"
                id="disableAnalytics"
                checked={!!props.serverConfig().DISABLE_ANALYTICS}
                onInput={(e) =>
                  props.setServerConfig((prev) => {
                    return {
                      ...prev,
                      DISABLE_ANALYTICS: e.currentTarget.checked,
                    };
                  })
                }
              />
              <label
                for="disableAnalytics"
                class="block text-sm font-medium leading-6"
              >
                Disable Analytics
              </label>
            </div>

            <div class="col-span-4 flex items-center space-x-2 sm:col-span-2">
              <input
                type="checkbox"
                name="lockDataset"
                id="lockDataset"
                checked={!!props.serverConfig().LOCKED}
                onInput={(e) =>
                  props.setServerConfig((prev) => {
                    return {
                      ...prev,
                      LOCKED: e.currentTarget.checked,
                    };
                  })
                }
              />
              <label for="lockDataset" class="block text-sm font-medium">
                Lock dataset
              </label>
            </div>

            <div class="col-span-4 flex items-center space-x-2 sm:col-span-2">
              <input
                type="checkbox"
                name="fullTextEnabled"
                id="fullTextEnabled"
                checked={!!props.serverConfig().FULLTEXT_ENABLED}
                onInput={(e) =>
                  props.setServerConfig((prev) => {
                    return {
                      ...prev,
                      FULLTEXT_ENABLED: e.currentTarget.checked,
                    };
                  })
                }
              />
              <label
                for="fullTextEnabled"
                class="block text-sm font-medium leading-6"
              >
                Fulltext Enabled
              </label>
            </div>

            <div class="col-span-4 flex items-center space-x-2 sm:col-span-2">
              <input
                type="checkbox"
                name="indexedOnly"
                id="indexedOnly"
                checked={!!props.serverConfig().INDEXED_ONLY}
                onInput={(e) =>
                  props.setServerConfig((prev) => {
                    return {
                      ...prev,
                      INDEXED_ONLY: e.currentTarget.checked,
                    };
                  })
                }
              />
              <div class="flex items-center">
                <label
                  for="embeddingSize"
                  class="mr-2 block text-sm font-medium leading-6"
                >
                  Indexed Only
                </label>
                <Tooltip
                  body={<AiOutlineInfoCircle />}
                  tooltipText="If enabled, only indexed documents will be returned in search results."
                />
              </div>
            </div>

            <div class="col-span-4 flex items-center space-x-2 sm:col-span-2">
              <input
                type="checkbox"
                name="qdrantOnly"
                id="qdrantOnly"
                checked={!!props.serverConfig().QDRANT_ONLY}
                onInput={(e) =>
                  props.setServerConfig((prev) => {
                    return {
                      ...prev,
                      QDRANT_ONLY: e.currentTarget.checked,
                    };
                  })
                }
              />
              <div class="flex items-center">
                <label
                  for="embeddingSize"
                  class="mr-2 block text-sm font-medium leading-6"
                >
                  Qdrant Only
                </label>
                <Tooltip
                  body={<AiOutlineInfoCircle />}
                  tooltipText="If enabled, chunks will only be stored in Qdrant."
                />
              </div>
            </div>

            <div class="col-span-4 flex items-center space-x-2 sm:col-span-2">
              <input
                type="checkbox"
                name="enablePagefind"
                id="enablePagefind"
                checked={!!props.serverConfig().PAGEFIND_ENABLED}
                onInput={(e) =>
                  props.setServerConfig((prev) => {
                    return {
                      ...prev,
                      PAGEFIND_ENABLED: e.currentTarget.checked,
                    };
                  })
                }
              />
              <div class="flex items-center">
                <label
                  for="enablePagefind"
                  class="mr-2 block text-sm font-medium leading-6"
                >
                  Pagefind Index Enabled
                </label>
                <Tooltip
                  body={<AiOutlineInfoCircle />}
                  tooltipText="If enabled, chunks will only be stored in Qdrant."
                />
              </div>
            </div>
          </div>
        </div>
      </div>
    </form>
  );
};
