import { SearchQueryEvent } from "shared/types";
import { FullScreenModal, JsonInput } from "shared/ui";
import { IoCode } from "solid-icons/io";
import { createMemo, createSignal, Show } from "solid-js";
import { z } from "zod";

interface ResultCardProps {
  result: SearchQueryEvent["results"][0];
}

const usefulMetadataSchema = z.object({
  id: z.string(),
  chunk_html: z.string(),
  tracking_id: z.string().optional().nullable(),
  weight: z.number().optional().nullable(),
  created_at: z.string().optional().nullable(),
});

export const ResultCard = (props: ResultCardProps) => {
  const metadata = createMemo(() => {
    const parseResult = usefulMetadataSchema.safeParse(
      props?.result?.metadata?.at(0),
    );
    if (parseResult.success) {
      console.log("THE PARSED METADATA: ", parseResult.data);
      return parseResult.data;
    } else {
      console.error("THE PARSED METADATA: ", parseResult.error);
      return null;
    }
  });

  const [showingJson, setShowingJson] = createSignal(false);

  return (
    <Show when={props.result}>
      <div class="rounded border border-neutral-200 p-2">
        <button
          onClick={() => setShowingJson(!showingJson())}
          class="text-left"
        >
          <div class="flex items-center justify-between gap-2 text-sm">
            <span class="font-medium">{metadata()?.id}</span>

            <IoCode />
          </div>
          <Show when={props?.result?.score}>
            <div class="text-xs font-normal opacity-60">
              Score: {props?.result?.score?.toFixed(5)}
            </div>
          </Show>
          <Show when={metadata()}>
            {(metadata) => (
              <div class="line-clamp-1 font-mono text-xs text-zinc-600">
                {metadata().chunk_html}
              </div>
            )}
          </Show>
        </button>
        <FullScreenModal
          title="Metadata"
          class="max-h-[80vh] max-w-[80vw] overflow-y-auto p-3"
          show={showingJson}
          setShow={setShowingJson}
        >
          <JsonInput
            value={() => props.result.metadata[0]}
            class="min-w-[60vw]"
            readonly
          />
        </FullScreenModal>
      </div>
    </Show>
  );
};
