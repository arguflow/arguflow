import React, { useEffect, useState } from "react";
import { useModalState } from "../utils/hooks/modal-context";
import { ChunkWithHighlights } from "../utils/types";
import { CheckIcon, ShoppingCart } from "./icons";

interface Props {
  item: ChunkWithHighlights;
}

export const AddToCartButton = ({ item }: Props) => {
  const { props } = useModalState();
  const [quantityInCart, setQuantityInCart] = useState(0);

  useEffect(() => {
    if (props.getCartQuantity && item.chunk.tracking_id) {
      const quantity = props.getCartQuantity(item.chunk.tracking_id);
      if (typeof quantity === "number") {
        setQuantityInCart(quantity);
      } else {
        quantity.then((q) => {
          setQuantityInCart(q);
        });
      }
    }
  }, []);

  if (!props.onAddToCart) {
    return null;
  }
  return (
    <div
      className="tv-flex tv-font-semibold tv-rounded-md tv-items-center tv-justify-center tv-w-full tv-gap-1 tv-px-2 tv-py-2 tv-mt-1 tv-bg-[--tv-prop-brand-color] tv-text-white hover:tv-ring-1 hover:tv-ring-[--tv-prop-brand-color] hover:tv-ring-offset-1"
      onClick={async (e) => {
        e.preventDefault();
        e.stopPropagation();
        if (props.onAddToCart) {
          setQuantityInCart(quantityInCart + 1);
          await props.onAddToCart(item.chunk);
        }
      }}
    >
      {quantityInCart && quantityInCart > 0 ? (
        <>
          <CheckIcon />
          {quantityInCart} In Cart
        </>
      ) : (
        <>
          <ShoppingCart />
          Add To Cart
        </>
      )}
    </div>
  );
};
