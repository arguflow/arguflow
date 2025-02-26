import React, { CSSProperties } from "react";
import {
  RecommendationsChunk,
  useRecommendations,
} from "./use-recommendations";
import { cva } from "cva";
import { ChunkFilter } from "trieve-ts-sdk";
export interface Theme {
  mode?: "light" | "dark";
  rounded?: "sm" | "md" | "lg" | "none";
  font?: string;
  padding?: "sm" | "md" | "lg" | "xl";
  shadow?: "sm" | "md" | "lg" | "none";
  border?: string;
  containerClassName?: string;
  itemClassName?: string;
  containerStyles?: CSSProperties;
  itemStyles?: StyleSheet;
}

export interface RecommendationsConfig {
  theme?: Theme;
  datasetId: string;
  productId: string;
  apiKey: string;
  baseUrl?: string;
  overflowScroll?: boolean;
  maxResults?: number;
  title?: string;
  orientation?: "horizontal" | "vertical";
  cssRelease?: string;
  filter?: ChunkFilter;
  useGroupSearch?: boolean;
}

const outerClass = cva(["tv-flex", "tv-gap-2"], {
  variants: {
    orientation: {
      horizontal: "tv-flex-row",
      vertical: "tv-flex-col",
    },
    overflowScroll: {
      true: "tv-overflow-auto",
    },
  },
  defaultVariants: {
    orientation: "horizontal",
    overflowScroll: true,
  },
});

const itemClass = cva(
  [
    "tv-transition-colors",
    "tv-justify-between tv-flex tv-flex-col",
    "tv-gap-2",
  ],
  {
    variants: {
      orientation: {
        horizontal: "tv-flex-row",
        vertical: "tv-flex-col tv-max-w-[200px] tv-items-center",
      },
      padding: {
        sm: "tv-p-2",
        md: "tv-px-4 tv-py-3",
        lg: "tv-px-6 tv-py-5",
        xl: "tv-px-8 tv-py-6",
      },
      rounded: {
        none: "tv-rounded-none",
        sm: "tv-rounded-sm",
        md: "tv-rounded-md",
        lg: "tv-rounded-lg",
      },
      mode: {
        light: "tv-bg-neutral-50 hover:tv-bg-neutral-100",
        dark: "tv-bg-neutral-800 tv-text-white",
      },
      shadow: {
        none: "tv-shadow-none",
        sm: "tv-shadow-sm",
        md: "tv-shadow-md",
        lg: "tv-shadow-lg",
      },
    },
    compoundVariants: [
      {
        orientation: "vertical",
        mode: "light",
        className: "tv-bg-transparent hover:tv-bg-auto",
      },
    ],

    defaultVariants: {
      orientation: "horizontal",
      mode: "light",
      rounded: "none",
      shadow: "none",
      padding: "sm",
    },
  },
);

const imageClass = cva(["tv-max-w-[100px]"], {
  variants: {
    rounded: {
      none: "tv-rounded-none",
      sm: "tv-rounded-sm",
      md: "tv-rounded-md",
      lg: "tv-rounded-lg",
    },
  },
  defaultVariants: {
    rounded: "none",
  },
});

const titleClass = cva(["tv-text-lg"], {
  variants: {
    mode: {
      light: "tv-text-neutral-900",
      dark: "tv-text-neutral-50",
    },
  },
});

export const Recommendations = (config: RecommendationsConfig) => {
  const { results } = useRecommendations(config);

  return (
    <>
      {config.title && (
        <div
          className={titleClass({
            mode: config.theme?.mode,
            className: ["trieve-recommendations-title"],
          })}
          style={{
            fontFamily: config.theme?.font || "inherit",
          }}>
          {config.title}
        </div>
      )}
      <div
        className={outerClass({
          orientation: config.orientation,
          className: [
            "trieve-recommendations-container",
            config.theme?.containerClassName,
          ],
        })}
        style={{
          fontFamily: config.theme?.font || "inherit",
          ...config.theme?.containerStyles,
        }}>
        {results?.map((r) => (
          <RecommendationsItem key={r.chunk.id} item={r} config={config} />
        ))}
      </div>
    </>
  );
};

const RecommendationsItem = ({
  item,
  config,
}: {
  item: RecommendationsChunk;
  config: RecommendationsConfig;
}) => {
  const getPrice = () => {
    const priceText = item?.chunk?.metadata?.variants?.at(0)?.price;
    if (priceText === "$0.00") {
      return null;
    }
  };

  return (
    <a
      href={item.chunk.link}
      className={itemClass({
        orientation: config.orientation,
        mode: config.theme?.mode,
        rounded: config.theme?.rounded,
        shadow: config.theme?.shadow,
        padding: config.theme?.padding,
        className: [
          "trieve-recommendations-container",
          config.theme?.itemClassName,
        ],
      })}
      style={{
        border: config.theme?.border ? `1px solid ${config.theme?.border}` : "",
        ...config.theme?.itemStyles,
      }}>
      <img
        className={imageClass({
          ...config.theme,
          className: "trieve-recommendations-image",
        })}
        src={item.chunk.metadata.images[0].src}
      />
      <div>
        <div>{item.chunk.metadata.title}</div>
        {getPrice() && (
          <div className="tv-text-sm tv-font-medium tv-text-neutral-500">
            ${getPrice()}
          </div>
        )}
      </div>
    </a>
  );
};
