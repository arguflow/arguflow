import type { LoaderFunctionArgs } from "@remix-run/node";
import { redirect } from "@remix-run/node";
import "./tailwind.css";
import { useFetcher } from "@remix-run/react";
import jwt, { JwtPayload } from "jsonwebtoken";

import { login } from "../../shopify.server";

import { useEffect, useState } from "react";

export const loader = async ({ request }: LoaderFunctionArgs) => {
  const url = new URL(request.url);

  if (url.searchParams.get("shop")) {
    throw redirect(`/app?${url.searchParams.toString()}`);
  }

  return { showForm: Boolean(login) };
};

export const action = async ({ request }: LoaderFunctionArgs) => {
  const formData = await request.formData();
  const type = formData.get("type")!;
  const apiKey = formData.get("apiKey")!;
  const idToken = formData.get("idToken")!;
  const orgId = formData.get("orgId")!;

  const decoded = jwt.verify(
    idToken.toString(),
    process.env.SHOPIFY_API_SECRET!,
    {
      algorithms: ["HS256"],
      clockTolerance: 10,
    },
  ) as JwtPayload;

  const now = Math.floor(Date.now() / 1000);

  if (decoded.exp && decoded.exp < now) {
    throw new Error("Token expired");
  }

  if (decoded.nbf && decoded.nbf > now) {
    throw new Error("Token not yet valid");
  }
  if (type === "insert") {
    const key = await prisma.apiKey.create({
      data: {
        userId: decoded.sub ?? "",
        organizationId: orgId.toString(),
        shop: decoded.dest ?? "",
        key: apiKey.toString(),
        createdAt: new Date(),
      },
    });
    return key;
  }
};

type User = {
  email: string;
  id: string;
  name: string;
  orgs: Orgs[];
};

type Orgs = {
  id: string;
  name: string;
};

export default function App() {
  const fetcher = useFetcher<typeof action>();
  const [orgs, setOrgs] = useState<Orgs[]>([]);

  useEffect(() => {
    fetch("https://api.trieve.ai/api/auth/me", {
      credentials: "include",
    }).then((response) => {
      if (response.status === 401) {
        window.location.href = `https://api.trieve.ai/api/auth?redirect_uri=${window.location}`;
      }
      response.json().then((data: User) => {
        setOrgs(data.orgs);
      });
    });
  }, []);

  useEffect(() => {
    if (fetcher.data?.key) {
      window.close();
    }
  }, [fetcher.data?.key]);

  const generateApiKey = (selectedOrg: string) => {
    if (!selectedOrg) {
      return;
    }
    fetch("https://api.trieve.ai/api/organization/api_key", {
      method: "POST",
      credentials: "include",
      headers: {
        "Content-Type": "application/json",
        "TR-Organization": selectedOrg.toString(),
      },
      body: JSON.stringify({ name: "Shopify-Access", role: 2 }),
    }).then((response) => {
      response.json().then((data) => {
        let params = new URLSearchParams(window.location.search);

        fetcher.submit(
          {
            apiKey: data.api_key,
            orgId: selectedOrg,
            idToken: params.get("token"),
            type: "insert",
          },
          { method: "POST" },
        );
      });
    });
  };

  return (
    <div className="relative flex min-h-screen flex-col items-center bg-neutral-200 py-36">
      <div className="absolute left-4 top-2 mb-8 flex items-center gap-1">
        <img
          className="h-12 w-12 cursor-pointer"
          src="https://cdn.trieve.ai/trieve-logo.png"
          alt="Logo"
        />
        <span className="text-2xl font-semibold">Trieve</span>
      </div>
      <div className="rounded-md border border-neutral-300 bg-white p-4 md:min-w-[500px]">
        <div className="flex justify-between">
          <div className="text-lg font-medium">Select An Organization</div>
        </div>
        <div className="flex flex-col py-2">
          {orgs?.map((org) => (
            <button
              onClick={() => {
                generateApiKey(org.id);
              }}
              className="flex cursor-pointer items-center justify-between rounded-md border-b border-b-neutral-200 p-2 last:border-b-transparent hover:bg-neutral-100">
              <div className="flex w-full items-center justify-between">
                <div className="text-sm font-medium">{org.name}</div>
                <div className="text-xs text-neutral-500">{org.id}</div>
              </div>
            </button>
          ))}
        </div>
      </div>
    </div>
  );
}
