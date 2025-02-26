import { useEffect, useState } from "react";

export const useChatHeight = (
  modalRef: React.RefObject<HTMLDivElement>,
  absoluteMinimum: number = 0
) => {
  const [chatHeight, setChatHeight] = useState(absoluteMinimum);
  const [minHeight, setMinHeight] = useState(absoluteMinimum);
  const [enabled, setEnabled] = useState(true);

  useEffect(() => {
    const ref = modalRef.current;
    if (ref) {
      const observer = new ResizeObserver((entries) => {
        setChatHeight(entries[0].contentRect.height);
      });
      observer.observe(ref);
      return () => {
        observer.disconnect();
      };
    }
  }, [modalRef]);

  useEffect(() => {
    if (chatHeight > minHeight && enabled) {
      setMinHeight(chatHeight);
    }
  }, [chatHeight]);

  const resetHeight = () => {
    setMinHeight(absoluteMinimum);
    setEnabled(false);
    setTimeout(() => {
      setEnabled(true);
    }, 200);
  };

  return { minHeight, resetHeight };
};
