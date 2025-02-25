import { startTransition } from "react";
import { ModalProps, SearchModes, useModalState } from "./modal-context";

export const setClickTriggers = (
  setOpen: (open: boolean) => void,
  setMode: React.Dispatch<React.SetStateAction<SearchModes>>,
  props: ModalProps,
) => {
  const removeAllClickListeners = (selector: string): Element | null => {
    const element: Element | null = document.querySelector(selector);
    if (!element) return null;
    // Vue click attributes
    element.removeAttribute("@click.prevent");
    element.removeAttribute("@click");
    // @ts-expect-error Property 'href' does not exist on type 'Element'. [2339]
    element.href = "#";

    const newElement = element.cloneNode(true);
    element?.parentNode?.replaceChild(newElement, element);
    return newElement as unknown as Element;
  };

  props.buttonTriggers?.forEach((trigger) => {
    let element: Element | null = document.querySelector(trigger.selector);
    if (trigger.removeListeners ?? true) {
      element = removeAllClickListeners(trigger.selector);
    }

    if (element) {
      const { trieveSDK } = useModalState();
      trieveSDK.sendAnalyticsEvent({
        event_name: `${props.componentName}_click`,
        event_type: "click",
        clicked_items: {
          chunk_id: `${props.componentName}`,
          position: 0,
        },
      });

      element.addEventListener("click", () => {
        startTransition(() => {
          setMode(trigger.mode);
          setOpen(true);
        });
      });
    }
  });
};
