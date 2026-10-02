FROM oven/bun:1-debian AS builder
WORKDIR /app
COPY package.json bun.lock tsconfig.json ./
RUN bun install --frozen-lockfile
COPY src ./src
RUN bun run build

FROM node:24-bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends bash ca-certificates curl git ripgrep \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /app/node_modules ./node_modules
COPY --from=builder /app/dist ./dist
COPY package.json ./

# Postgres and RabbitMQ run separately. Auth and workspaces are supplied at runtime.
RUN mkdir -p /pi-agent /workspace \
    && chmod 1777 /pi-agent \
    && chown node:node /workspace
ENV PI_CODING_AGENT_DIR=/pi-agent \
    WORKSPACE_DIR=/workspace
USER node
CMD ["node", "dist/index.js"]
