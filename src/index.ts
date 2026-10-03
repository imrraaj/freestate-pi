import { startBackend } from "./backend.js";

async function main(): Promise<void> {
    const databaseUrl = process.env.DATABASE_URL;
    const rabbitmqUrl = process.env.RABBITMQ_URL;

    if (!databaseUrl || !rabbitmqUrl) {
        throw new Error("DATABASE_URL and RABBITMQ_URL are required");
    }

    const runTimeoutMs = Number(process.env.RUN_TIMEOUT_MS ?? "1200000");

    if (
        !Number.isInteger(runTimeoutMs) ||
        runTimeoutMs < 1000 ||
        runTimeoutMs > 86_400_000
    ) {
        throw new Error("RUN_TIMEOUT_MS must be between 1000 and 86400000");
    }

    const backend = await startBackend({
        databaseUrl,
        rabbitmqUrl,
        workspaceDir: process.env.WORKSPACE_DIR ?? "/workspace",
        piCliPath:
            "/app/node_modules/@earendil-works/pi-coding-agent/dist/bundle/cli.js",
        runTimeoutMs,
        requestQueue: process.env.REQUEST_QUEUE ?? "freestate-pi.requests.v1",
        resultQueue: process.env.RESULT_QUEUE ?? "freestate-pi.results.v1",
        eventStream: process.env.EVENT_STREAM ?? "freestate-pi.events.v1",
    });

    let closing = false;

    async function shutdown(): Promise<void> {
        if (closing) return;
        closing = true;
        await backend.close();
    }

    process.once("SIGTERM", () => void shutdown());
    process.once("SIGINT", () => void shutdown());

    console.log("freestate-pi AMQP worker ready");
}

void main().catch(() => {
    // Never log credentials, provider diagnostics, or prompt contents.
    console.error(
        "freestate-pi could not start; verify database, broker and runtime configuration",
    );
    process.exitCode = 1;
});
