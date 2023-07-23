import fetch from "node-fetch";
import { getAuthCookie } from "./auth";

const api_endpoint = process.env.API_ENDPOINT || "http://localhost:8090/api";

describe("Card Verification Tests", () => {
  let authCookie = null;
  test("Verification with exact match", async () => {
    authCookie = await getAuthCookie();

    const response = await fetch(`${api_endpoint}/verification`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Cookie: authCookie,
      },
      credentials: "include",
      body: JSON.stringify({
        content:
          "Example Domain This domain is for use in illustrative examples in documents. You may use this domain in literature without prior coordination or asking for permission.",
        url_source: "https://www.example.com",
       
      }),
    });
    const json = await response.json();
    expect(json).toHaveProperty("score");
    expect(json.score).toBe(1);
  });

  test("Verification with exact match and slight changes", async () => {
    authCookie = await getAuthCookie();

    let content =
      "Example Domain This domain is for use in illustrative examples in documents. You may use this domain in literature without prior coordination or asking for permission.";
    content += "L";
    const response = await fetch(`${api_endpoint}/verification`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Cookie: authCookie,
      },
      credentials: "include",
      body: JSON.stringify({
        url_source: "https://blog.arguflow.com/posts/streaming-chatgpt-messages-with-openai-api-and-actix-web",
        content,
      }),
    });
    const json = await response.json();
    expect(json).toHaveProperty("score");
    expect(json.score).toBe(1);
  });

  test("Verification for card", async () => {
    authCookie = await getAuthCookie();

    let card_uuid = "8b53cac3-3f04-42e7-a5c6-0b0d2655db46";
    const response = await fetch(`${api_endpoint}/verification`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Cookie: authCookie,
      },
      credentials: "include",
      body: JSON.stringify({
        card_uuid,
      }),
    });
    const json = await response.json();
    expect(json).toHaveProperty("score");
    console.log("Score: ", json.score);
  });

  test("Adding non relevant content to the end", async () => {
    let content = "When choosing to decide what software to build Arguflow AI with we were tired of using Javascript for our backend services. We wanted something better, something faster, something safer, something rusty. Our main motivation behind choosing to use rust was for the learning experience behind it."

    content += "yeah yeah yeah"

    const response = await fetch(`${api_endpoint}/verification`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Cookie: authCookie,
      },
      body: JSON.stringify({
        url_source: "https://blog.arguflow.com/posts/streaming-chatgpt-messages-with-openai-api-and-actix-web",
        content,
      }),
    });
    const json = await response.json();
    expect(json).toHaveProperty("score");
    expect(json.score).toBe(1);
  });

  test("2 exact match", async () => {
    let content = "When choosing to decide what software to build Arguflow AI with we were tired of using Javascript for our backend services. We wanted something better, something faster, something safer, something rusty. Our main motivation behind choosing to use rust was for the learning experience behind it."
    content += "Streaming data won’t get saved to open AI, so as we iterate through the messages we should keep track of the full message so we can store it to the database when streaming finishes. Giving us this as our final function"

    const response = await fetch(`${api_endpoint}/verification`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Cookie: authCookie,
      },
      body: JSON.stringify({
        url_source: "https://blog.arguflow.com/posts/streaming-chatgpt-messages-with-openai-api-and-actix-web",
        content,
      }),
    });
    const json = await response.json();
    expect(json).toHaveProperty("score");
    expect(json.score).toBe(1);
  });

  test("exact match symbols", async () => {
    let content = "When choosing to decide what software to build Arguflow AI with we were tired of using Javascript for our backend services. We wanted something better, something faster, something safer, something rusty. Our main motivation behind choosing to use rust was for the learning experience behind it."
    content += " -+-;_=(_()_()@#$"

    const response = await fetch(`${api_endpoint}/verification`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Cookie: authCookie,
      },
      body: JSON.stringify({
        url_source: "https://blog.arguflow.com/posts/streaming-chatgpt-messages-with-openai-api-and-actix-web",
        content,
      }),
    });
    const json = await response.json();
    expect(json).toHaveProperty("score");
    expect(json.score).toBe(1);
  });

  test("Not in there at all", async () => {
    let content = "yo yo yo"

    const response = await fetch(`${api_endpoint}/verification`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Cookie: authCookie,
      },
      body: JSON.stringify({
        url_source: "https://blog.arguflow.com/posts/streaming-chatgpt-messages-with-openai-api-and-actix-web",
        content,
      }),
    });
    const json = await response.json();
    expect(json).toHaveProperty("score");
    expect(json.score).toBe(1);
  });

  test("example.com exact match", async () => {
    let content = "This domain is for use in illustrative examples in documents. You may use this domain in literature without prior coordination or asking for permission."

    const response = await fetch(`${api_endpoint}/verification`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Cookie: authCookie,
      },
      body: JSON.stringify({
        url_source: "https://example.com",
        content,
      }),
    });
    const json = await response.json();
    expect(json).toHaveProperty("score");
    expect(json.score).toBe(1);
  });

  test("example.com a little bit added", async () => {
    let content = "This domain is for use in illustrative examples in documents. You may use this domain in literature without prior coordination or asking for permission."
    content += "yeah yeah yeah"

    const response = await fetch(`${api_endpoint}/verification`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Cookie: authCookie,
      },
      body: JSON.stringify({
        url_source: "https://example.com",
        content,
      }),
    });
    const json = await response.json();
    expect(json).toHaveProperty("score");
    expect(json.score).toBe(1);
  });

  test("1 exact then junk", async () => {
    let content = "When choosing to decide what software to build Arguflow AI with we were tired of using Javascript for our backend services. We wanted something better, something faster, something safer, something rusty. Our main motivation behind choosing to use rust was for the learning experience behind it."
    content += "DASFASDFASD"

    const response = await fetch(`${api_endpoint}/verification`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Cookie: authCookie,
      },
      body: JSON.stringify({
        url_source: "https://blog.arguflow.com/posts/streaming-chatgpt-messages-with-openai-api-and-actix-web",
        content,
      }),
    });
    const json = await response.json();
    expect(json).toHaveProperty("score");
    expect(json.score).toBe(1);
  });

  test("Random stuff", async () => {
    let content = "()_()_s()_s()_s()s_()_s()_s()_s(xz[]zx[]_s()"

    const response = await fetch(`${api_endpoint}/verification`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Cookie: authCookie,
      },
      body: JSON.stringify({
        url_source: "https://blog.arguflow.com/posts/streaming-chatgpt-messages-with-openai-api-and-actix-web",
        content,
      }),
    });
    const json = await response.json();
    expect(json).toHaveProperty("score");
    expect(json.score).toBe(1);
  });

});
