<p align="center">
  <img height="100" src="https://cdn.trieve.ai/trieve-logo.png" alt="Trieve Logo">
</p>
<p align="center">
<strong><a href="https://dashboard.trieve.ai">Sign Up (1k chunks free)</a> | <a href="https://pdf2md.trieve.ai">PDF2MD</a> | <a href="https://docs.trieve.ai">Hacker News Search Engine</a> | <a href="https://docs.trieve.ai">Documentation</a> | <a href="https://cal.com/nick.k/meet">Meet a Maintainer</a> | <a href="https://discord.gg/eBJXXZDB8z">Discord</a> | <a href="https://matrix.to/#/#trieve-general:trieve.ai">Matrix</a>
</strong>
</p>

<p align="center">
    <a href="https://github.com/devflowinc/trieve/stargazers">
        <img src="https://img.shields.io/github/stars/devflowinc/trieve.svg?style=flat&color=yellow" alt="Github stars"/>
    </a>
    <a href="https://discord.gg/CuJVfgZf54">
        <img src="https://img.shields.io/discord/1130153053056684123.svg?label=Discord&logo=Discord&colorB=7289da&style=flat" alt="Join Discord"/>
    </a>
    <a href="https://matrix.to/#/#trieve-general:trieve.ai">
        <img src="https://img.shields.io/badge/matrix-join-purple?style=flat&logo=matrix&logocolor=white" alt="Join Matrix"/>
    </a>
    <a href="https://smithery.ai/server/trieve-mcp-server">
        <img src="https://smithery.ai/badge/trieve-mcp-server" alt="smithery badge"/>
    </a>
</p>

<h2 align="center">
    <b>All-in-one solution for search, recommendations, and RAG</b>
</h2>

<a href="https://trieve.ai">
  <img src="https://cdn.trieve.ai/landing-tabs/light-api.webp">
</a>

## Quick Links

- [API Reference + Docs](https://docs.trieve.ai/api-reference)
- [OpenAPI specification](https://api.trieve.ai/redoc)
- [Typescript SDK](https://ts-sdk.trieve.ai/)
- [Python SDK](https://pypi.org/project/trieve-py-client/)

## Features

- **🔒 Self-Hosting in your VPC or on-prem**: We have full self-hosting guides for AWS, GCP, Kubernetes generally, and docker compose available on our [documentation page here](https://docs.trieve.ai/self-hosting/docker-compose).
- **🧠 Semantic Dense Vector Search**: Integrates with OpenAI or Jina embedding models and [Qdrant](https://qdrant.tech) to provide semantic vector search.
- **🔍 Typo Tolerant Full-Text/Neural Search**: Every uploaded chunk is vector'ized with [naver/efficient-splade-VI-BT-large-query](https://huggingface.co/naver/efficient-splade-VI-BT-large-query) for typo tolerant, quality neural sparse-vector search.
- **🖊️ Sub-Sentence Highlighting**: Highlight the matching words or sentences within a chunk and bold them on search to enhance UX for your users. Shout out to the [simsearch](https://github.com/smartdatalake/simsearch) crate!
- **🌟 Recommendations**: Find similar chunks (or files if using grouping) with the recommendation API. Very helpful if you have a platform where users' favorite, bookmark, or upvote content.
- **🤖 Convenient RAG API Routes**: We integrate with OpenRouter to provide you with access to any LLM you would like for RAG. Try our routes for [fully-managed RAG with topic-based memory management](https://api.trieve.ai/redoc#tag/message/operation/create_message_completion_handler) or [select your own context RAG](https://api.trieve.ai/redoc#tag/chunk/operation/generate_off_chunks).
- **💼 Bring Your Own Models**: If you'd like, you can bring your own text-embedding, SPLADE, cross-encoder re-ranking, and/or large-language model (LLM) and plug it into our infrastructure.
- **🔄 Hybrid Search with cross-encoder re-ranking**: For the best results, use hybrid search with [BAAI/bge-reranker-large](https://huggingface.co/BAAI/bge-reranker-large) re-rank optimization.
- **📆 Recency Biasing**: Easily bias search results for what was most recent to prevent staleness
- **🛠️ Tunable Merchandizing**: Adjust relevance using signals like clicks, add-to-carts, or citations
- **🕳️ Filtering**: Date-range, substring match, tag, numeric, and other filter types are supported.
- **👥 Grouping**: Mark multiple chunks as being part of the same file and search on the file-level such that the same top-level result never appears twice

**Are we missing a feature that your use case would need?** - call us at [628-222-4090](mailto:+16282224090), make a [Github issue](https://github.com/devflowinc/trieve/issues), or join the [Matrix community](https://matrix.to/#/#trieve-general:trieve.ai) and tell us! We are a small company who is still very hands-on and eager to build what you need; professional services are available.

## Local development with Linux

### Installing via Smithery

To install Trieve for Claude Desktop automatically via [Smithery](https://smithery.ai/server/trieve-mcp-server):

```bash
npx -y @smithery/cli install trieve-mcp-server --client claude
```

### Debian/Ubuntu Packages needed packages

```sh
sudo apt install curl \
gcc \
g++ \
make \
pkg-config \
python3 \
python3-pip \
libpq-dev \
libssl-dev \
openssl
```

### Arch Packages needed

```sh
sudo pacman -S base-devel postgresql-libs
```

### Install NodeJS and Yarn

You can install [NVM](https://github.com/nvm-sh/nvm) using its install script.

```
curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.5/install.sh | bash
```

You should restart the terminal to update bash profile with NVM. Then, you can install NodeJS LTS release and Yarn.

```
nvm install --lts
npm install -g yarn
```

### Make server tmp dir

```
mkdir server/tmp
```

### Install rust

```
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

### Install cargo-watch

```
cargo install cargo-watch
```

### Setup env's

```
cp .env.analytics ./frontends/analytics/.env
cp .env.chat ./frontends/chat/.env
cp .env.search ./frontends/search/.env
cp .env.example ./server/.env
cp .env.dashboard ./frontends/dashboard/.env
```

### Add your `LLM_API_KEY` to `./server/.env`

[Here is a guide for acquiring that](https://blog.streamlit.io/beginners-guide-to-openai-api/#get-your-own-openai-api-key).

#### Steps once you have the key

1. Open the `./server/.env` file
2. Replace the value for `LLM_API_KEY` to be your own OpenAI API key.
3. Replace the value for `OPENAI_API_KEY` to be your own OpenAI API key.

### Export the following keys in your terminal for local dev

The PAGEFIND_CDN_BASE_URL and S3_SECRET_KEY_CSVJSONL could be set to a random list of strings.

```
export OPENAI_API_KEY="your_OpenAI_api_key" \
LLM_API_KEY="your_OpenAI_api_key" \
PAGEFIND_CDN_BASE_URL="lZP8X4h0Q5Sj2ZmV,aAmu1W92T6DbFUkJ,DZ5pMvz8P1kKNH0r,QAqwvKh8rI5sPmuW,YMwgsBz7jLfN0oX8" \
S3_SECRET_KEY_CSVJSONL="Gq6wzS3mjC5kL7i4KwexnL3gP8Z1a5Xv,V2c4ZnL0uHqBzFvR2NcN8Pb1g6CjmX9J,TfA1h8LgI5zYkH9A9p7NvWlL0sZzF9p8N,pKr81pLq5n6MkNzT1X09R7Qb0Vn5cFr0d,DzYwz82FQiW6T3u9A4z9h7HLOlJb7L2V1"
```

### Start docker container services needed for local dev

```
cat .env.chat .env.search .env.server .env.docker-compose > .env

./convenience.sh -l
```

### Start services for local dev

We recommend managing this through [tmuxp, see the guide here](https://gist.github.com/skeptrunedev/101c7a13bb9b9242999830655470efac) or terminal tabs.

```
cd clients/ts-sdk
yarn build
```

```
cd frontends
yarn
yarn dev
```

```
cd server
cargo watch -x run
```

```
cd server
cargo run --bin ingestion-worker
```

```
cd server
cargo run --bin file-worker
```

```
cd server
cargo run --bin delete-worker
```

```
cd search
yarn
yarn dev
```

### Verify Working Setup

After the cargo build has finished (after the `tmuxp load trieve`):
- check that you can see redoc with the OpenAPI reference at [localhost:8090/redoc](http://localhost:8090/redoc)
- make an account create a dataset with test data at [localhost:5173](http://localhost:5173)
- search that dataset with test data at [localhost:5174](http://localhost:5174)

### Additional Instructions for testing cross encoder reranking models

To test the Cross Encoder rerankers in local dev, 
- click on the dataset, go to the Dataset Settings -> Dataset Options -> Additional Options and uncheck the `Fulltext Enabled` option.
- in the Embedding Settings, select your reranker model and enter the respective key in the adjacent textbox, and hit save.
- in the search playground, set Type -> Semantic and select Rerank By -> Cross Encoder
- if AIMon Reranker is selected in the Embedding Settings, you can enter an optional Task Definition in the search playground to specify the domain of context documents to the AIMon reranker.


### Debugging issues with local dev

Reach out to us on [discord](https://discord.gg/E9sPRZqpDT) for assistance. We are available and more than happy to assist.

## Debug diesel by getting the exact generated SQL

`diesel::debug_query(&query).to_string();`

## Local Setup for Testing Stripe Features

Install Stripe CLI.

1. `stripe login`
2. `stripe listen --forward-to localhost:8090/api/stripe/webhook`
3. set the `STRIPE_WEBHOOK_SECRET` in the `server/.env` to the resulting webhook signing secret
4. `stripe products create --name trieve --default-price-data.unit-amount 1200 --default-price-data.currency usd`
5. `stripe plans create --amount=1200 --currency=usd --interval=month --product={id from response of step 3}`

## Contributors

<a href="https://github.com/devflowinc/trieve/graphs/contributors">
  <img alt="contributors" src="https://contrib.rocks/image?repo=devflowinc/trieve"/>
</a>
