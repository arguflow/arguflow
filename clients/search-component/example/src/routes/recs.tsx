import { createFileRoute } from "@tanstack/react-router";
import { Recommendations } from "../../../src/recommendations/Recommendations.tsx";

export const Route = createFileRoute("/recs")({
  component: RouteComponent,
});

function RouteComponent() {
  const baseUrl = import.meta.env.VITE_API_BASE_URL;
  const datasetId = import.meta.env.VITE_DATASET_ID;
  const apiKey = import.meta.env.VITE_API_KEY;
  return (
    <div className="grid">
      <Recommendations
        theme={{
          padding: "sm",
          mode: "light",
        }}
        productId="44895730368689"
        useGroupSearch={false}
        filter={{
          must: [
            {
              field: "tag_set",
              match_all: ["gender_mens"],
            },
          ],
        }}
        apiKey={apiKey}
        baseUrl={baseUrl}
        datasetId={datasetId}
        title="Similar Items"
      />
    </div>
  );
}
