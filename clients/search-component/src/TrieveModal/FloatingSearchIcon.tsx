import React, { useState, startTransition } from "react";
import { useModalState } from "../utils/hooks/modal-context";

export const FloatingSearchIcon = () => {
  const [isHovered, setIsHovered] = useState(false);
  const { props, setOpen, setMode } = useModalState();

  const setButtonPosition = (position: string) => {
    if (position === "right") {
      return {
        right: "0",
        left: "unset",
        paddingRight: isHovered ? "2rem" : "1rem",
        transition: "padding-right 0.4s ease",
        borderRadius: "50px 0 0 50px",
      };
    } else {
      return {
        right: "unset",
        left: "0",
        paddingLeft: isHovered ? "2rem" : "1rem",
        transition: "padding-left 0.4s ease",
        borderRadius: "0 50px 50px 0",
      };
    }
  };

  return (
    <div
      className={`floating-search-btn-container${
        props.theme == "dark" ? " dark" : ""
      }`}
      style={{
        ...setButtonPosition(props.floatingSearchIconPosition || "right"),
        display: "block",
        zIndex: (props.zIndex ?? 1000) - 1,
      }}
      onMouseEnter={() => setIsHovered(true)}
      onMouseLeave={() => setIsHovered(false)}
      onClick={() => {
        if (!props.disableFloatingSearchIconClick) {
          startTransition(() => {
            setOpen((prev) => {
              return !prev;
            });
            setMode(props.defaultSearchMode || "search");
          });
        }
      }}
    >
      <div className="floating-search-btn">
        <div className="floating-search-icon">
          <svg
            width="26"
            height="26"
            viewBox="0 0 32 32"
            fill="white"
            xmlns="http://www.w3.org/2000/svg"
          >
            <path d="M13.452 20.1373C11.5994 20.1373 10.03 19.4942 8.7438 18.208C7.45757 16.9217 6.81445 15.3523 6.81445 13.4998C6.81445 11.6473 7.45757 10.0779 8.7438 8.79165C10.03 7.50542 11.5994 6.8623 13.452 6.8623C15.3045 6.8623 16.8739 7.50542 18.1601 8.79165C19.4463 10.0779 20.0895 11.6473 20.0895 13.4998C20.0895 14.2411 19.9768 14.9338 19.7514 15.5778C19.5261 16.2218 19.2213 16.7898 18.8373 17.2819L24.3895 22.84C24.6007 23.0552 24.7063 23.3215 24.7063 23.6387C24.7063 23.9559 24.5987 24.2201 24.3835 24.4313C24.1722 24.6426 23.907 24.7482 23.5878 24.7482C23.2686 24.7482 23.0034 24.6426 22.7922 24.4313L17.246 18.8851C16.75 19.2692 16.1779 19.5739 15.5299 19.7993C14.8819 20.0246 14.1893 20.1373 13.452 20.1373ZM13.452 17.8623C14.6661 17.8623 15.6967 17.4388 16.5438 16.5917C17.3909 15.7446 17.8145 14.7139 17.8145 13.4998C17.8145 12.2857 17.3909 11.2551 16.5438 10.408C15.6967 9.56085 14.6661 9.1373 13.452 9.1373C12.2378 9.1373 11.2072 9.56085 10.3601 10.408C9.513 11.2551 9.08945 12.2857 9.08945 13.4998C9.08945 14.7139 9.513 15.7446 10.3601 16.5917C11.2072 17.4388 12.2378 17.8623 13.452 17.8623Z"></path>
            <path d="M22.4282 7.85633L23.5023 5.50232L25.8563 4.42816L23.5023 3.35401L22.4282 1L21.354 3.35401L19 4.42816L21.354 5.50232L22.4282 7.85633Z"></path>
          </svg>
        </div>
      </div>
    </div>
  );
};
