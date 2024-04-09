import { For, Show, createEffect, createSignal, useContext } from "solid-js";
import { DatasetContext } from "../../../contexts/DatasetContext";
import {
  ClientEnvsConfiguration,
  ServerEnvsConfiguration,
  availableEmbeddingModels,
  isComboboxValues,
} from "../../../types/apiTypes";
import { UserContext } from "../../../contexts/UserContext";
import { createToast } from "../../../components/ShowToasts";
import { AiOutlineInfoCircle } from "solid-icons/ai";
import { useNavigate } from "@solidjs/router";

export const defaultClientEnvsConfiguration: ClientEnvsConfiguration = {
  CREATE_CHUNK_FEATURE: true,
  DOCUMENT_UPLOAD_FEATURE: true,
  SEARCH_QUERIES: "",
  FRONTMATTER_VALS: "",
  LINES_BEFORE_SHOW_MORE: 0,
  DATE_RANGE_VALUE: "",
  FILTER_ITEMS: [],
  SUGGESTED_QUERIES: "",
  SHOW_GITHUB_STARS: false,
  IMAGE_RANGE_START_KEY: "",
  IMAGE_RANGE_END_KEY: "",
  FILE_NAME_KEY: "",
};

export const defaultServerEnvsConfiguration: ServerEnvsConfiguration = {
  LLM_BASE_URL: "",
  LLM_DEFAULT_MODEL: "",
  EMBEDDING_BASE_URL: "https://embedding.trieve.ai",
  RAG_PROMPT: "",
  EMBEDDING_SIZE: 768,
  N_RETRIEVALS_TO_INCLUDE: 8,
  DUPLICATE_DISTANCE_THRESHOLD: 1.1,
  DOCUMENT_UPLOAD_FEATURE: true,
  DOCUMENT_DOWNLOAD_FEATURE: true,
  COLLISIONS_ENABLED: false,
  FULLTEXT_ENABLED: true,
  QDRANT_COLLECTION_NAME: null,
};

export const FrontendSettingsForm = () => {
  const datasetContext = useContext(DatasetContext);
  const userContext = useContext(UserContext);

  const [clientConfig, setClientConfig] = createSignal<ClientEnvsConfiguration>(
    datasetContext.dataset?.()?.client_configuration ??
      defaultClientEnvsConfiguration,
  );

  const [name, setName] = createSignal<string>(
    datasetContext.dataset?.()?.name ?? "",
  );

  createEffect(() => {
    setName(datasetContext.dataset?.()?.name ?? "");
  });

  createEffect(() => {
    setClientConfig(
      datasetContext.dataset?.()?.client_configuration ??
        defaultClientEnvsConfiguration,
    );
  });

  const [saved, setSaved] = createSignal<boolean>(false);

  const onSave = () => {
    void fetch(`${import.meta.env.VITE_API_HOST}/dataset`, {
      method: "PUT",
      headers: {
        "Content-Type": "application/json",
        "TR-Organization": userContext.selectedOrganizationId?.() as string,
      },
      credentials: "include",
      body: JSON.stringify({
        dataset_id: datasetContext.dataset?.()?.id,
        dataset_name: name(),
        client_configuration: clientConfig(),
      }),
    })
      .then(() => {
        setSaved(true);
        void new Promise((r) => setTimeout(r, 1000)).then(() =>
          setSaved(false),
        );
      })
      .catch((err) => {
        console.log(err);
      });
  };

  return (
    <form>
      <div class="shadow sm:overflow-hidden sm:rounded-md">
        <div class="bg-white px-4 py-6 sm:p-6">
          <div>
            <h2 id="user-details-name" class="text-lg font-medium leading-6">
              Frontend Settings
            </h2>
            <p class="mt-1 text-sm text-neutral-600">
              Update settings for how the frontend behaves.
            </p>
          </div>
          <div class="mt-6 grid grid-cols-4 gap-6">
            <div class="col-span-4 sm:col-span-2">
              <label
                for="datasetName"
                class="block text-sm font-medium leading-6"
              >
                Dataset Name
              </label>
              <input
                type="text"
                name="datasetName"
                id="datasetName"
                class="mt-2 block w-full rounded-md border-[0.5px] border-neutral-300 px-3 py-1.5 shadow-sm placeholder:text-neutral-400 focus:outline-magenta-500 sm:text-sm sm:leading-6"
                value={name()}
                onInput={(e) => setName(e.target.value)}
              />
            </div>

            <div class="col-span-4 sm:col-span-2">
              <label
                for="linesBeforeShowMore"
                class="block text-sm font-medium leading-6"
              >
                Lines before show more
              </label>
              <input
                type="number"
                name="linesBeforeShowMore"
                id="linesBeforeShowMore"
                class="mt-2 block w-full rounded-md border-[0.5px] border-neutral-300 px-3 py-1.5 shadow-sm placeholder:text-neutral-400 focus:outline-magenta-500 sm:text-sm sm:leading-6"
                value={clientConfig().LINES_BEFORE_SHOW_MORE}
                onInput={(e) =>
                  setClientConfig((prev) => {
                    return {
                      ...prev,
                      LINES_BEFORE_SHOW_MORE: e.target.valueAsNumber,
                    };
                  })
                }
              />
            </div>

            <div class="col-span-4 flex items-center space-x-2 sm:col-span-2">
              <input
                type="checkbox"
                name="documentUploadFeatureClient"
                id="documentUploadFeatureClient"
                checked={clientConfig().DOCUMENT_UPLOAD_FEATURE}
                onInput={(e) =>
                  setClientConfig((prev) => {
                    return {
                      ...prev,
                      DOCUMENT_UPLOAD_FEATURE: e.currentTarget.checked,
                    };
                  })
                }
              />
              <label
                for="documentUploadFeature"
                class="block text-sm font-medium"
              >
                Document upload feature
              </label>
            </div>

            <div class="col-span-4 flex items-center space-x-2 sm:col-span-2">
              <input
                type="checkbox"
                name="documentUploadFeatureClient"
                id="documentUploadFeatureClient"
                checked={clientConfig().CREATE_CHUNK_FEATURE}
                onInput={(e) =>
                  setClientConfig((prev) => {
                    return {
                      ...prev,
                      CREATE_CHUNK_FEATURE: e.currentTarget.checked,
                    };
                  })
                }
              />
              <label
                for="documentUploadFeature"
                class="block text-sm font-medium"
              >
                Create chunk feature
              </label>
            </div>

            <div class="col-span-4 sm:col-span-2">
              <label
                for="dateRangeValue"
                class="block text-sm font-medium leading-6"
              >
                Date range value
              </label>
              <input
                type="text"
                name="dateRangeValue"
                id="dateRangeValue"
                class="mt-2 block w-full rounded-md border-[0.5px] border-neutral-300 px-3 py-1.5 shadow-sm placeholder:text-neutral-400 focus:outline-magenta-500 sm:text-sm sm:leading-6"
                value={clientConfig().DATE_RANGE_VALUE}
                onInput={(e) =>
                  setClientConfig((prev) => {
                    return {
                      ...prev,
                      DATE_RANGE_VALUE: e.target.value,
                    };
                  })
                }
              />
            </div>

            <div class="col-span-4">
              <label
                for="searchQueries"
                class="block text-sm font-medium leading-6"
              >
                Search Queries
              </label>
              <input
                type="text"
                name="searchQueries"
                id="searchQueries"
                class="mt-2 block w-full rounded-md border-[0.5px] border-neutral-300 px-3 py-1.5 shadow-sm placeholder:text-neutral-400 focus:outline-magenta-500 sm:text-sm sm:leading-6"
                value={clientConfig().SEARCH_QUERIES}
                onInput={(e) =>
                  setClientConfig((prev) => {
                    return {
                      ...prev,
                      SEARCH_QUERIES: e.target.value,
                    };
                  })
                }
              />
            </div>

            <div class="col-span-4">
              <label
                for="frontmatterVals"
                class="block text-sm font-medium leading-6"
              >
                Frontmatter Values
              </label>
              <input
                type="text"
                name="frontmatterVals"
                id="frontmatterVals"
                class="mt-2 block w-full rounded-md border-[0.5px] border-neutral-300 px-3 py-1.5 shadow-sm placeholder:text-neutral-400 focus:outline-magenta-500 sm:text-sm sm:leading-6"
                value={clientConfig().FRONTMATTER_VALS}
                onInput={(e) =>
                  setClientConfig((prev) => {
                    return {
                      ...prev,
                      FRONTMATTER_VALS: e.target.value,
                    };
                  })
                }
              />
            </div>
            {/** TODO: change to a modal to set filters and generate json from that */}
            <div class="col-span-4">
              <label
                for="filterItems"
                class="block text-sm font-medium leading-6"
              >
                Filter Items
              </label>
              <input
                type="text"
                name="filterItems"
                id="filterItems"
                class="mt-2 block w-full rounded-md border-[0.5px] border-neutral-300 px-3 py-1.5 shadow-sm placeholder:text-neutral-400 focus:outline-magenta-500 sm:text-sm sm:leading-6"
                value={JSON.stringify(clientConfig().FILTER_ITEMS)}
                onInput={(e) => {
                  const value = JSON.parse(e.target.value) as object[];
                  if (isComboboxValues(value)) {
                    setClientConfig((prev) => {
                      return {
                        ...prev,
                        FILTER_ITEMS: value,
                      };
                    });
                  }
                }}
              />
            </div>

            <div class="col-span-4">
              <label
                for="suggestedQueries"
                class="block text-sm font-medium leading-6"
              >
                Suggested Queries
              </label>
              <input
                type="text"
                name="suggestedQueries"
                id="suggestedQueries"
                class="mt-2 block w-full rounded-md border-[0.5px] border-neutral-300 px-3 py-1.5 shadow-sm placeholder:text-neutral-400 focus:outline-magenta-500 sm:text-sm sm:leading-6"
                value={clientConfig().SUGGESTED_QUERIES}
                onInput={(e) =>
                  setClientConfig((prev) => {
                    return {
                      ...prev,
                      SUGGESTED_QUERIES: e.target.value,
                    };
                  })
                }
              />
            </div>

            <div class="col-span-4">
              <label
                for="imageRangeStartKey"
                class="block text-sm font-medium leading-6"
              >
                Image Range Start Key
              </label>
              <input
                type="text"
                name="imageRangeStartKey"
                id="imageRangeStartKey"
                class="mt-2 block w-full rounded-md border-[0.5px] border-neutral-300 px-3 py-1.5 shadow-sm placeholder:text-neutral-400 focus:outline-magenta-500 sm:text-sm sm:leading-6"
                value={clientConfig().IMAGE_RANGE_START_KEY}
                onInput={(e) =>
                  setClientConfig((prev) => {
                    return {
                      ...prev,
                      IMAGE_RANGE_START_KEY: e.target.value,
                    };
                  })
                }
              />
            </div>

            <div class="col-span-4">
              <label
                for="imageRangeEndKey"
                class="block text-sm font-medium leading-6"
              >
                Image Range End Key
              </label>
              <input
                type="text"
                name="imageRangeEndKey"
                id="imageRangeEndKey"
                class="mt-2 block w-full rounded-md border-[0.5px] border-neutral-300 px-3 py-1.5 shadow-sm placeholder:text-neutral-400 focus:outline-magenta-500 sm:text-sm sm:leading-6"
                value={clientConfig().IMAGE_RANGE_END_KEY}
                onInput={(e) =>
                  setClientConfig((prev) => {
                    return {
                      ...prev,
                      IMAGE_RANGE_END_KEY: e.target.value,
                    };
                  })
                }
              />
            </div>

            <div class="col-span-4">
              <label
                for="fileNameKey"
                class="block text-sm font-medium leading-6"
              >
                File Name Key
              </label>
              <input
                type="text"
                name="suggestedQueries"
                id="suggestedQueries"
                class="mt-2 block w-full rounded-md border-[0.5px] border-neutral-300 px-3 py-1.5 shadow-sm placeholder:text-neutral-400 focus:outline-magenta-500 sm:text-sm sm:leading-6"
                value={clientConfig().FILE_NAME_KEY}
                onInput={(e) =>
                  setClientConfig((prev) => {
                    return {
                      ...prev,
                      FILE_NAME_KEY: e.target.value,
                    };
                  })
                }
              />
            </div>
          </div>
        </div>
        <div class="bg-neutral-50 px-4 py-3 text-right sm:px-6">
          <button
            onClick={(e) => {
              e.preventDefault();
              onSave();
            }}
            class="inline-flex justify-center rounded-md bg-magenta-500 px-3 py-2 font-semibold text-white shadow-sm hover:bg-magenta-500 focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-magenta-600 disabled:bg-magenta-200"
          >
            Save
          </button>
          <Show when={saved()}>
            <span class="ml-3 text-sm">Saved!</span>
          </Show>
        </div>
      </div>
    </form>
  );
};

export const ServerSettingsForm = () => {
  const datasetContext = useContext(DatasetContext);
  const [serverConfig, setServerConfig] = createSignal<ServerEnvsConfiguration>(
    datasetContext.dataset?.()?.server_configuration ??
      defaultServerEnvsConfiguration,
  );

  createEffect(() => {
    setServerConfig(
      datasetContext.dataset?.()?.server_configuration ??
        defaultServerEnvsConfiguration,
    );
  });

  const [saved, setSaved] = createSignal<boolean>(false);

  const onSave = () => {
    const datasetId = datasetContext.dataset?.()?.id;
    if (!datasetId) return;

    void fetch(`${import.meta.env.VITE_API_HOST}/dataset`, {
      method: "PUT",
      headers: {
        "Content-Type": "application/json",
        "TR-Dataset": datasetId,
      },
      credentials: "include",
      body: JSON.stringify({
        dataset_id: datasetContext.dataset?.()?.id,
        server_configuration: serverConfig(),
      }),
    })
      .then(() => {
        setSaved(true);
        void new Promise((r) => setTimeout(r, 1000)).then(() =>
          setSaved(false),
        );
      })
      .catch((err) => {
        console.log(err);
      });
  };

  return (
    <form>
      <div class="shadow sm:overflow-hidden sm:rounded-md">
        <div class="bg-white px-4 py-6 sm:p-6">
          <div>
            <h2 id="user-details-name" class="text-lg font-medium leading-6">
              Server Settings
            </h2>
            <p class="mt-1 text-sm text-neutral-600">
              Update settings for how the server behaves.
            </p>
          </div>

          <div class="mt-6 grid grid-cols-4 gap-6">
            <div class="col-span-4 sm:col-span-2">
              <label
                for="llmAPIURL"
                class="block text-sm font-medium leading-6"
              >
                LLM API URL
              </label>
              <input
                type="text"
                name="llmAPIURL"
                id="llmAPIURL"
                class="mt-2 block w-full rounded-md border-[0.5px] border-neutral-300 px-3 py-1.5 shadow-sm placeholder:text-neutral-400 focus:outline-magenta-500 sm:text-sm sm:leading-6"
                value={serverConfig().LLM_BASE_URL}
                onInput={(e) =>
                  setServerConfig((prev) => {
                    return {
                      ...prev,
                      LLM_BASE_URL: e.currentTarget.value,
                    };
                  })
                }
              />
            </div>

            <div class="col-span-4 sm:col-span-2">
              <label
                for="llmAPIURL"
                class="block text-sm font-medium leading-6"
              >
                LLM Default Model
              </label>
              <input
                type="text"
                name="llmDefaultModel"
                id="llmDefaultModel"
                class="mt-2 block w-full rounded-md border-[0.5px] border-neutral-300 px-3 py-1.5 shadow-sm placeholder:text-neutral-400 focus:outline-magenta-500 sm:text-sm sm:leading-6"
                value={serverConfig().LLM_DEFAULT_MODEL}
                onInput={(e) =>
                  setServerConfig((prev) => {
                    return {
                      ...prev,
                      LLM_DEFAULT_MODEL: e.currentTarget.value,
                    };
                  })
                }
              />
            </div>

            <div class="col-span-4 sm:col-span-2">
              <label
                for="nRetrivialsToInclude"
                class="block text-sm font-medium leading-6"
              >
                N Retrivials To Include (RAG-inference)
              </label>
              <input
                type="number"
                name="nRetrivialsToInclude"
                id="linesBeforeShowMore"
                class="mt-2 block w-full rounded-md border-[0.5px] border-neutral-300 px-3 py-1.5 shadow-sm placeholder:text-neutral-400 focus:outline-magenta-500 sm:text-sm sm:leading-6"
                value={serverConfig().N_RETRIEVALS_TO_INCLUDE}
                onInput={(e) =>
                  setServerConfig((prev) => {
                    return {
                      ...prev,
                      N_RETRIEVALS_TO_INCLUDE: e.currentTarget.valueAsNumber,
                    };
                  })
                }
              />
            </div>

            <div class="col-span-4 sm:col-span-2">
              <label
                for="duplicateThreshold"
                class="block text-sm font-medium leading-6"
              >
                Duplicate Threshold
              </label>
              <input
                type="number"
                step={0.1}
                name="duplicateThreshold"
                id="linesBeforeShowMore"
                class="mt-2 block w-full rounded-md border-[0.5px] border-neutral-300 px-3 py-1.5 shadow-sm placeholder:text-neutral-400 focus:outline-magenta-500 sm:text-sm sm:leading-6"
                value={serverConfig().DUPLICATE_DISTANCE_THRESHOLD}
                onInput={(e) =>
                  setServerConfig((prev) => {
                    return {
                      ...prev,
                      DUPLICATE_DISTANCE_THRESHOLD:
                        e.currentTarget.valueAsNumber,
                    };
                  })
                }
              />
            </div>

            <div class="col-span-4 sm:col-span-2">
              <label
                for="ragPrompt"
                class="block text-sm font-medium leading-6"
              >
                RAG Prompt
              </label>
              <textarea
                value={serverConfig().RAG_PROMPT}
                onInput={(e) =>
                  setServerConfig((prev) => {
                    return {
                      ...prev,
                      RAG_PROMPT: e.currentTarget.value,
                    };
                  })
                }
                rows="4"
                name="ragPrompt"
                id="ragPrompt"
                class="mt-2 block w-full rounded-md border-[0.5px] border-neutral-300 px-3 py-1.5 shadow-sm placeholder:text-neutral-400 focus:outline-magenta-500 sm:text-sm sm:leading-6"
              />
            </div>

            <div class="col-span-4 space-y-1 sm:col-span-2">
              <AiOutlineInfoCircle
                class="h-5 w-5 text-neutral-400"
                title="Embedding Model is only editable on creation"
              />
              <select
                id="embeddingSize"
                aria-readonly
                title="Embedding Model is only editable on creation"
                disabled
                name="embeddingSize"
                class="col-span-2 block w-full rounded-md border-[0.5px] border-neutral-300 bg-white px-3 py-1.5 shadow-sm placeholder:text-neutral-400 focus:outline-magenta-500 sm:text-sm sm:leading-6"
                value={
                  availableEmbeddingModels.find(
                    (model) =>
                      model.dimension === serverConfig().EMBEDDING_SIZE,
                  )?.name ?? availableEmbeddingModels[0].name
                }
              >
                <For each={availableEmbeddingModels}>
                  {(model) => <option value={model.name}>{model.name}</option>}
                </For>
              </select>
            </div>

            <div class="col-span-4 flex items-center space-x-2 sm:col-span-2">
              <input
                type="checkbox"
                name="collisionsEnabled"
                id="collisionsEnabled"
                checked={serverConfig().COLLISIONS_ENABLED}
                onInput={(e) =>
                  setServerConfig((prev) => {
                    return {
                      ...prev,
                      COLLISIONS_ENABLED: e.currentTarget.checked,
                    };
                  })
                }
              />
              <label
                for="collisionsEnabled"
                class="block text-sm font-medium leading-6"
              >
                Collisions Enabled
              </label>
            </div>

            <div class="col-span-4 flex items-center space-x-2 sm:col-span-2">
              <input
                type="checkbox"
                name="fullTextEnabled"
                id="fullTextEnabled"
                checked={serverConfig().FULLTEXT_ENABLED}
                onInput={(e) =>
                  setServerConfig((prev) => {
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
                name="documentUploadFeature"
                id="documentUploadFeature"
                checked={serverConfig().DOCUMENT_UPLOAD_FEATURE}
                onInput={(e) =>
                  setServerConfig((prev) => {
                    return {
                      ...prev,
                      DOCUMENT_UPLOAD_FEATURE: e.currentTarget.checked,
                    };
                  })
                }
              />
              <label
                for="documentUploadFeature"
                class="block text-sm font-medium"
              >
                Document upload feature
              </label>
            </div>

            <div class="col-span-4 flex items-center space-x-2 sm:col-span-2">
              <input
                type="checkbox"
                name="documentDownloadFeature"
                id="documentDownloadFeature"
                checked={serverConfig().DOCUMENT_DOWNLOAD_FEATURE}
                onInput={(e) =>
                  setServerConfig((prev) => {
                    return {
                      ...prev,
                      DOCUMENT_DOWNLOAD_FEATURE: e.currentTarget.checked,
                    };
                  })
                }
              />
              <label
                for="documentDownloadFeature"
                class="block text-sm font-medium leading-6"
              >
                Document download feature
              </label>
            </div>
          </div>
        </div>
        <div class="bg-neutral-50 px-4 py-3 text-right sm:px-6">
          <button
            onClick={(e) => {
              e.preventDefault();
              onSave();
            }}
            class="inline-flex justify-center rounded-md bg-magenta-500 px-3 py-2 font-semibold text-white shadow-sm hover:bg-magenta-500 focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-magenta-600 disabled:bg-magenta-200"
          >
            Save
          </button>
          <Show when={saved()}>
            <span class="ml-3 text-sm">Saved!</span>
          </Show>
        </div>
      </div>
    </form>
  );
};

export const DangerZoneForm = () => {
  const datasetContext = useContext(DatasetContext);

  const navigate = useNavigate();

  const deleteDataset = () => {
    const dataset_id = datasetContext.dataset?.()?.id;
    const organization_id = datasetContext.dataset?.()?.organization_id;
    if (!dataset_id) return;
    if (!organization_id) return;

    const confirmBox = confirm(
      "Deleting this dataset will remove all chunks which are contained within it. Are you sure you want to delete?",
    );
    if (!confirmBox) return;

    fetch(`${import.meta.env.VITE_API_HOST}/dataset`, {
      method: "DELETE",
      headers: {
        "Content-Type": "application/json",
        "TR-Organization": organization_id,
      },
      credentials: "include",
      body: JSON.stringify({
        dataset_id: dataset_id,
      }),
    })
      .then((res) => {
        if (res.ok) {
          navigate("/dashboard/overview");
          createToast({
            title: "Success",
            message: "Dataset deleted successfully!",
            type: "success",
          });
        }
      })
      .catch(() => {
        createToast({
          title: "Error",
          message: "Error deleting dataset!",
          type: "error",
        });
      });
  };

  return (
    <form class="border-4 border-red-500">
      <div class="shadow sm:overflow-hidden sm:rounded-md ">
        <div class="space-y-3 bg-white px-4 py-6 sm:p-6">
          <div>
            <h2 id="user-details-name" class="text-lg font-medium leading-6">
              Danger Zone
            </h2>
            <p class="mt-1 text-sm text-neutral-600">
              These settings are for advanced users only. Changing these
              settings can break the app.
            </p>
          </div>

          <button
            onClick={() => {
              deleteDataset();
            }}
            class="pointer:cursor w-fit rounded-md border border-red-500 px-4 py-2 text-red-500 hover:bg-red-500 hover:text-white focus:outline-magenta-500"
          >
            DELETE DATASET
          </button>
        </div>
      </div>
    </form>
  );
};

export const DatasetSettingsPage = () => {
  return (
    <div class="grid grid-cols-1 gap-3 lg:grid-cols-2">
      <div>
        <FrontendSettingsForm />
      </div>
      <div>
        <ServerSettingsForm />
      </div>
      <div>
        <DangerZoneForm />
      </div>
    </div>
  );
};
