/* eslint-disable @typescript-eslint/no-unsafe-call */
/* eslint-disable @typescript-eslint/no-unsafe-return */
/* eslint-disable @typescript-eslint/no-unsafe-member-access */
/* eslint-disable @typescript-eslint/no-unsafe-assignment */
import { BiSolidUserRectangle } from "solid-icons/bi";
import { AiFillRobot } from "solid-icons/ai";
import { Accessor, For, Show, createEffect, createSignal } from "solid-js";
import type { ScoreChunkDTO } from "../../utils/apiTypes";
import ScoreChunk, { sanitzerOptions } from "../ScoreChunk";
import sanitizeHtml from "sanitize-html";
import { useCtrClickForChunk } from "../../hooks/useCtrAnalytics";

export interface AfMessageProps {
  role: "user" | "assistant" | "system";
  content: string;
  streamingCompletion: Accessor<boolean>;
  chunks: Accessor<ScoreChunkDTO[]>;
  order: number;
}

export const AfMessage = (props: AfMessageProps) => {
  const [selectedIds, setSelectedIds] = createSignal<string[]>([]);
  const { registerClickForChunk } = useCtrClickForChunk();
  const [content, setContent] = createSignal<string>("");

  createEffect(() => {
    setContent(props.content);
  });

  createEffect(() => {
    if (props.streamingCompletion()) return;

    setContent(props.content);
  });

  return (
    <>
      <Show when={props.role !== "system"}>
        <div
          classList={{
            "dark:text-white md:px-6 w-full px-4 py-4 flex items-start": true,
            "bg-neutral-200 dark:bg-zinc-700": props.role === "assistant",
            "bg-neutral-50 dark:bg-zinc-800": props.role === "user",
          }}
        >
          <div class="w-full space-y-2 md:flex md:flex-row md:space-x-2 md:space-y-0 lg:space-x-4">
            {props.role === "user" ? (
              <BiSolidUserRectangle class="fill-current" />
            ) : (
              <AiFillRobot class="fill-current" />
            )}
            <div class="flex flex-col items-start gap-y-8 lg:grid lg:grid-cols-3 lg:flex-row lg:gap-4">
              <div class="col-span-2 whitespace-pre-line text-neutral-800 dark:text-neutral-50">
                <div
                  // eslint-disable-next-line solid/no-innerhtml
                  innerHTML={sanitizeHtml(content(), sanitzerOptions)}
                />
              </div>
              <Show when={!props.content}>
                <div class="col-span-2 w-full whitespace-pre-line">
                  <div class="flex w-full flex-col items-center justify-center">
                    <div class="h-5 w-5 animate-spin rounded-full border-b-2 border-t-2 border-fuchsia-300" />
                  </div>
                </div>
              </Show>
              <Show when={props.role == "assistant"}>
                <div class="max-h-[600px] w-full flex-col space-y-3 overflow-scroll overflow-x-hidden scrollbar-thin scrollbar-track-neutral-200 dark:scrollbar-track-zinc-700">
                  <For each={props.chunks()}>
                    {(chunk, idx) => (
                      <ScoreChunk
                        group={undefined}
                        allGroupsList={[]}
                        chunk={chunk.chunk}
                        score={0}
                        showExpand={!props.streamingCompletion()}
                        counter={(idx() + 1).toString()}
                        order={props.order.toString()}
                        selectedIds={selectedIds}
                        setSelectedIds={setSelectedIds}
                        chat={true}
                        registerClickForChunk={({ id, eventType }) =>
                          registerClickForChunk({
                            id: id,
                            eventType: eventType,
                            position: 0,
                          })
                        }
                      />
                    )}
                  </For>
                </div>
              </Show>
            </div>
          </div>
        </div>
      </Show>
    </>
  );
};
