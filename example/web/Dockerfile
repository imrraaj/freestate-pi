FROM oven/bun:1-debian
WORKDIR /app
COPY package.json bun.lock ./
RUN bun install --frozen-lockfile --production
COPY server.mjs index.html ./
RUN mkdir -p /data && chown bun:bun /data
USER bun
EXPOSE 8081
CMD ["bun", "run", "server.mjs"]
