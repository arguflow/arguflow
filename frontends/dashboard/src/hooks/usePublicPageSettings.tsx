import { createSignal, createEffect, useContext, createMemo } from "solid-js";
import { createStore } from "solid-js/store";
import { Dataset, PublicPageParameters } from "trieve-ts-sdk";
import { createQuery } from "@tanstack/solid-query";
import { DatasetContext } from "../contexts/DatasetContext";
import { UserContext } from "../contexts/UserContext";
import { useTrieve } from "./useTrieve";
import { createToast } from "../components/ShowToasts";
import { ApiRoutes } from "../components/Routes";
import { HeroPatterns } from "../pages/dataset/HeroPatterns";
import { createInitializedContext } from "../utils/initialize";

export type DatasetWithPublicPage = Dataset & {
  server_configuration: {
    PUBLIC_DATASET?: {
      extra_params: PublicPageParameters;
      enabled: boolean;
    };
  };
};

export const { use: usePublicPage, provider: PublicPageProvider } =
  createInitializedContext("public-page-settings", () => {
    const [extraParams, setExtraParams] = createStore<PublicPageParameters>({});
    const [searchOptionsError, setSearchOptionsError] = createSignal<
      string | null
    >(null);

    const [isPublic, setisPublic] = createSignal<boolean>(false);
    const [hasLoaded, setHasLoaded] = createSignal(false);

    const { datasetId } = useContext(DatasetContext);
    const { selectedOrg } = useContext(UserContext);

    const trieve = useTrieve();

    createEffect(() => {
      void (
        trieve.fetch<"eject">("/api/dataset/{dataset_id}", "get", {
          datasetId: datasetId(),
        }) as Promise<DatasetWithPublicPage>
      ).then((dataset) => {
        setisPublic(!!dataset.server_configuration?.PUBLIC_DATASET?.enabled);
        setExtraParams(
          dataset?.server_configuration?.PUBLIC_DATASET?.extra_params || {},
        );

        setHasLoaded(true);
      });
    });

    const crawlSettingsQuery = createQuery(() => ({
      queryKey: ["crawl-settings", datasetId()],
      queryFn: async () => {
        const result = await trieve.fetch(
          "/api/dataset/crawl_options/{dataset_id}",
          "get",
          {
            datasetId: datasetId(),
          },
        );
        return result.crawl_options ?? null;
      },
    }));

    // If the useGroupSearch has not been manually set,
    // set to true if shopify scraping is enabled
    createEffect(() => {
      if (
        crawlSettingsQuery.data &&
        crawlSettingsQuery.data.scrape_options?.type === "shopify"
      ) {
        if (
          extraParams.useGroupSearch === null ||
          extraParams.useGroupSearch === undefined
        ) {
          setExtraParams("useGroupSearch", true);
        }
      }
    });

    // manually set the array for rolemessages to simplify logic
    // context blocks until it's set
    createEffect(() => {
      if (
        extraParams.tabMessages === undefined ||
        extraParams.tabMessages === null
      ) {
        setExtraParams("tabMessages", []);
      }
    });

    // Selecting blank as the hero pattern should reset everything else
    // Selecting another pattern builds the svg field
    createEffect(() => {
      const pattern = extraParams.heroPattern?.heroPatternName;
      const foreground = extraParams.heroPattern?.foregroundColor;
      if (hasLoaded()) {
        if (pattern == "Blank" || !pattern) {
          setExtraParams("heroPattern", {
            heroPatternName: "Blank",
            heroPatternSvg: "",
            foregroundColor: "#ffffff",
            foregroundOpacity: 0.5,
            backgroundColor: "#f3f3f3",
          });
        } else if (pattern == "Solid") {
          setExtraParams("heroPattern", (prev) => ({
            ...prev,
            backgroundColor: foreground,
          }));
        } else {
          setExtraParams("heroPattern", (prev) => ({
            ...prev,
            heroPatternSvg: HeroPatterns[pattern](
              prev?.foregroundColor || "#ffffff",
              prev?.foregroundOpacity || 0.5,
            ),
          }));
        }
      }
    });

    const unpublishDataset = async () => {
      await trieve.fetch("/api/dataset", "put", {
        organizationId: selectedOrg().id,
        data: {
          dataset_id: datasetId(),
          server_configuration: {
            PUBLIC_DATASET: {
              enabled: false,
            },
          },
        },
      });

      createToast({
        type: "info",
        title: `Made dataset ${datasetId()} private`,
      });

      setisPublic(false);
    };

    const publishDataset = async () => {
      const name = `${datasetId()}-pregenerated-search-component`;
      if (!isPublic()) {
        const response = await trieve.fetch(
          "/api/organization/api_key",
          "post",
          {
            data: {
              name: name,
              role: 0,
              dataset_ids: [datasetId()],
              scopes: ApiRoutes["Search Component Routes"],
            },
            organizationId: selectedOrg().id,
          },
        );

        await trieve.fetch("/api/dataset", "put", {
          organizationId: selectedOrg().id,
          data: {
            dataset_id: datasetId(),
            server_configuration: {
              PUBLIC_DATASET: {
                enabled: true,
                // @ts-expect-error Object literal may only specify known properties, and 'api_key' does not exist in type 'PublicDatasetOptions'. [2353]
                api_key: response.api_key,
                extra_params: {
                  ...extraParams,
                },
              },
            },
          },
        });

        createToast({
          type: "info",
          title: `Created API key for ${datasetId()} named ${name}`,
        });
      } else {
        await trieve.fetch("/api/dataset", "put", {
          organizationId: selectedOrg().id,
          data: {
            dataset_id: datasetId(),
            server_configuration: {
              PUBLIC_DATASET: {
                enabled: true,
                extra_params: {
                  ...extraParams,
                },
              },
            },
          },
        });

        createToast({
          type: "info",
          title: `Updated Public settings for ${name}`,
        });
      }

      setExtraParams(extraParams);
      setisPublic(true);
    };

    const apiHost = import.meta.env.VITE_API_HOST as unknown as string;
    const publicUrl = createMemo(() => {
      return `${apiHost.slice(0, -4)}/public_page/${datasetId()}`;
    });

    return {
      extraParams,
      setExtraParams,
      searchOptionsError,
      setSearchOptionsError,
      isPublic,
      publicUrl,
      unpublishDataset,
      publishDataset,
      get ready() {
        return hasLoaded() && !!extraParams.tabMessages;
      },
    };
  });
