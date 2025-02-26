import { cva } from "cva";
import { useModalState } from "./modal-context";

const popupClass = cva(
  [
    "tv-max-h-[calc(100vh-48px)]",
    "tv-overflow-x-auto",
    "mobile-only:tv-min-w-[calc(100vw-32px)]", // mobile
    "mobile-only:tv-max-w-[calc(100vw-32px)]", // mobile
    "tv-top-[16px]", // mobile
    "tv-shadow-2xl tv-fixed",
    "tv-px-4",
    "tv-left-[50%]",
    "-tv-translate-x-[50%]",
    "md:tv-min-w-auto",
    "md:tv-w-[90vw]",
    "md:!tv-min-w-auto",
    "md:!tv-max-w-[1440px]",
    "md:!tv-max-h-[calc(100vh-64px)]",
  ],
  {
    variants: {
      type: {
        ecommerce: [],
        docs: [],
        pdf: [],
      },
      mode: {
        search: [],
        chat: [],
      },
    },
  }
);

const inlineClass = cva(
  [
    "tv-max-h-[40vh]",
    "sm:tv-max-w-[800px]",
    "!tv-min-w-full tv-max-w-sm tv-w-full",
    "tv-px-2",
  ],
  {
    variants: {
      type: {
        ecommerce: [
          "tv-top-1 tv-w-[95vw] tv-min-w-full tv-rounded-lg",
          "tv-px-0 tv-pt-0 tv-max-w-full",
        ],
        docs: [],
        pdf: [],
      },
    },
  }
);

export const useSizing = () => {
  const { props, mode } = useModalState();

  if (props.inline) {
    return inlineClass({ type: props.type });
  } else {
    return popupClass({
      type: props.type,
      mode: mode as "search" | "chat",
    });
  }
};
