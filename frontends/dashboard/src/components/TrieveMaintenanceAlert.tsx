import { FaSolidTriangleExclamation } from "solid-icons/fa";

export const TrieveMaintenanceAlert = () => {
  return (
    <div>
      <div
        class={`flex flex-row items-center justify-between rounded-lg border-2 border-yellow-500 bg-transparent bg-white p-4`}
      >
        <div
          class={`flex flex-row items-center justify-center gap-3 text-yellow-600`}
        >
          <FaSolidTriangleExclamation />
          <span class="text-sm font-semibold">
            Trieve is currently undergoing maintenance. Chunk creation and
            updates are delayed but will be applied once maintenance is
            complete.
          </span>
        </div>
      </div>
    </div>
  );
};
