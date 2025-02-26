import { useEffect, useState } from "react";
import { RecommendationsConfig } from "./Recommendations";
import { GetRecommendedGroupsData } from "trieve-ts-sdk";

export type RecommendationsChunk = {
  score: number;
  chunk: {
    id: string;
    link: string;
    created_at: string;
    chunk_html: string;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    metadata: Record<string, any>;
    image_urls: string[];
  };
};

type GroupAndChunkPair = {
  group: {
    name: string;
    description: string;
    id: string;
    tracking_id: string;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    metadata: Record<string, any>;
  };
  chunks: RecommendationsChunk[];
};

type RecommendationsResponse = {
  results: GroupAndChunkPair[];
};

export const useRecommendations = (config: RecommendationsConfig) => {
  const [status, setStatus] = useState<"loading" | "error" | "success">(
    "loading",
  );

  const [results, setResults] = useState<RecommendationsChunk[]>([]);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    setStatus("loading");

    const getData = async () => {
      try {
        if (config.useGroupSearch ?? true) {
          const response = await fetch(
            config.baseUrl + "/api/chunk_group/recommend",
            {
              method: "POST",
              body: JSON.stringify({
                positive_group_tracking_ids: [config.productId],
                limit: config.maxResults,
                filters: config.filter,
              } satisfies GetRecommendedGroupsData["requestBody"]),
              headers: {
                "TR-Dataset": config.datasetId,
                Authorization: config.apiKey,
                "Content-Type": "application/json",
              },
            },
          );

          if (!response.ok) {
            setStatus("error");

            try {
              const error = (await response.json()) as {
                message: string;
              };
              setError(error.message);
            } catch {
              setError("Something went wrong");
            }

            return;
          }

          const data = (await response.json()) as RecommendationsResponse;
          console.log(data);
          setResults(data.results.map((c) => c.chunks.at(0)!));
          setStatus("success");
        } else {
          const response = await fetch(
            config.baseUrl + "/api/chunk/recommend",
            {
              method: "POST",
              body: JSON.stringify({
                positive_tracking_ids: [config.productId],
                limit: config.maxResults,
                filters: config.filter,
              }),
              headers: {
                "TR-Dataset": config.datasetId,
                Authorization: config.apiKey,
                "Content-Type": "application/json",
              },
            },
          );

          if (!response.ok) {
            setStatus("error");

            try {
              const error = (await response.json()) as {
                message: string;
              };
              setError(error.message);
            } catch {
              setError("Something went wrong");
            }

            return;
          }

          const data = (await response.json()) as {
            chunks: RecommendationsChunk[];
          };
          setResults(data.chunks);
          setStatus("success");
        }
      } catch {
        setStatus("error");
        setError("Something went wrong");
      }
    };

    getData();
  }, [config.productId]);

  return { status, results, error };
};
