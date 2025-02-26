import { Box, CalloutCard } from "@shopify/polaris";
import { useRevalidator } from "@remix-run/react";
import { useEffect } from "react";
import { useAppBridge } from "@shopify/app-bridge-react";

export const MustLoginPage = ({ authUrl }: { authUrl: string }) => {
  const shopify = useAppBridge();
  const revalidator = useRevalidator();

  useEffect(() => {
    const handleFocus = () => {
      if (revalidator.state === "idle") {
        revalidator.revalidate();
      }
    };
    window.addEventListener("focus", handleFocus);
    return () => {
      window.removeEventListener("focus", handleFocus);
    };
  }, []);

  const navigateToAuth = () => {
    shopify.idToken().then((token) => {
      window.open(`${authUrl}?token=${token}`, "_blank");
    });
  };

  return (
    <Box paddingInline="3200" paddingBlockStart="2000">
      <CalloutCard
        title="Welcome To Trieve."
        illustration="https://cdn.trieve.ai/android-chrome-512x512.png"
        primaryAction={{
          content: "Login",
          onAction: navigateToAuth
        }}
      >
        <p>To get started, login</p>
      </CalloutCard>
    </Box>
  );
};
