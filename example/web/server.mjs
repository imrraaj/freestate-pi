import { randomUUID } from "node:crypto";
import { readFile, rename, writeFile } from "node:fs/promises";
import { join } from "node:path";
import amqp from "amqplib";
import { z } from "zod";

const REQUEST_QUEUE = "freestate-pi.requests.v1";
const RESULT_QUEUE = "freestate-pi.results.v1";
const EVENT_STREAM = "freestate-pi.events.v1";
const RESULT_DIR = process.env.RESULT_DIR ?? "/data";
const PORT = Number(process.env.PORT ?? "8081");
const page = await Bun.file(new URL("./index.html", import.meta.url)).text();
const encoder = new TextEncoder();

const submitSchema = z.strictObject({
    prompt: z.string().trim().min(1).max(50_000),
    threadId: z.uuid().optional(),
});
const idSchema = z.uuid();
const eventSchema = z.strictObject({
    version: z.literal(1),
    runId: idSchema,
    type: z.literal("text_delta"),
    sequence: z.number().int().positive(),
    delta: z.string().min(1),
});
const resultSchema = z.discriminatedUnion("status", [
    z.strictObject({
        version: z.literal(1),
        runId: idSchema,
        threadId: idSchema.optional(),
        status: z.literal("completed"),
        output: z.string(),
        error: z.null(),
    }),
    z.strictObject({
        version: z.literal(1),
        runId: idSchema,
        threadId: idSchema.optional(),
        status: z.literal("failed"),
        output: z.null(),
        error: z.string(),
    }),
]);

if (!process.env.RABBITMQ_URL) throw new Error("RABBITMQ_URL is required");
const connection = await amqp.connect(process.env.RABBITMQ_URL);
connection.on("error", () => {});
connection.on("close", () => {
    console.error("Broker disconnected; restarting the demo backend");
    process.exit(1);
});

const requests = await connection.createConfirmChannel();
const results = await connection.createChannel();
const topology = await connection.createChannel();
for (const channel of [requests, results, topology])
    channel.on("error", () => {});
await requests.assertQueue(REQUEST_QUEUE, { durable: true });
await results.assertQueue(RESULT_QUEUE, { durable: true });
await topology.assertQueue(EVENT_STREAM, {
    durable: true,
    arguments: {
        "x-queue-type": "stream",
        "x-max-age": "1D",
        "x-max-length-bytes": 1_000_000_000,
    },
});
await results.prefetch(20);

const completed = new Map();
const listeners = new Map();

async function loadResult(runId) {
    if (completed.has(runId)) return completed.get(runId);
    try {
        const result = resultSchema.parse(
            JSON.parse(
                await readFile(join(RESULT_DIR, `${runId}.json`), "utf8"),
            ),
        );
        completed.set(runId, result);
        return result;
    } catch {
        return null;
    }
}

await results.consume(
    RESULT_QUEUE,
    async (message) => {
        if (!message) return;
        let parsed;
        try {
            parsed = resultSchema.safeParse(
                JSON.parse(message.content.toString("utf8")),
            );
        } catch {
            parsed = { success: false };
        }
        if (!parsed.success) {
            results.nack(message, false, false);
            return;
        }

        try {
            const result = parsed.data;
            const target = join(RESULT_DIR, `${result.runId}.json`);
            const temporary = `${target}.${process.pid}.${randomUUID()}.tmp`;
            await writeFile(temporary, JSON.stringify(result), { mode: 0o600 });
            await rename(temporary, target);
            completed.set(result.runId, result);
            for (const listener of listeners.get(result.runId) ?? [])
                listener(result);
            results.ack(message);
        } catch {
            results.nack(message, false, true);
        }
    },
    { noAck: false },
);

function json(body, status = 200) {
    return Response.json(body, {
        status,
        headers: {
            "Cache-Control": "no-store",
            "X-Content-Type-Options": "nosniff",
        },
    });
}

async function submit(request) {
    if (
        request.headers.get("content-type")?.split(";")[0] !==
        "application/json"
    )
        return json({ error: "Send JSON" }, 415);
    if (Number(request.headers.get("content-length") ?? 0) > 128 * 1024)
        return json({ error: "Request too large" }, 413);
    const body = await request.text();
    if (Buffer.byteLength(body, "utf8") > 128 * 1024)
        return json({ error: "Request too large" }, 413);
    let parsed;
    try {
        parsed = submitSchema.safeParse(JSON.parse(body));
    } catch {
        parsed = { success: false };
    }
    if (!parsed.success)
        return json(
            { error: "Enter a nonempty prompt (up to 50,000 characters)" },
            400,
        );

    const runId = randomUUID();
    const threadId = parsed.data.threadId ?? randomUUID();
    const payload = Buffer.from(
        JSON.stringify({
            version: 1,
            runId,
            threadId,
            prompt: parsed.data.prompt,
        }),
    );
    try {
        await new Promise((resolve, reject) =>
            requests.sendToQueue(
                REQUEST_QUEUE,
                payload,
                {
                    persistent: true,
                    contentType: "application/json",
                    messageId: runId,
                },
                (error) => (error ? reject(error) : resolve()),
            ),
        );
        return json({ runId, threadId }, 202);
    } catch {
        return json({ error: "Could not queue the run" }, 503);
    }
}

function stream(request, runId) {
    // A page reload starts at the first retained event; an EventSource reconnect uses Last-Event-ID.
    const cursor = request.headers.get("last-event-id");
    let after = cursor && /^\d+$/.test(cursor) ? Number(cursor) : 0;
    if (!Number.isSafeInteger(after)) after = 0;
    let live = true;
    let consumer = null;
    let heartbeat = null;
    let finish = null;
    let controller;
    const send = (type, data, sequence) => {
        if (!live) return;
        const id = sequence === undefined ? "" : `id: ${sequence}\n`;
        controller.enqueue(
            encoder.encode(
                `${id}event: ${type}\ndata: ${JSON.stringify(data)}\n\n`,
            ),
        );
    };
    const cleanup = () => {
        if (!live) return;
        live = false;
        request.signal.removeEventListener("abort", cleanup);
        if (heartbeat) clearInterval(heartbeat);
        if (finish) {
            const subscribers = listeners.get(runId);
            subscribers?.delete(finish);
            if (subscribers?.size === 0) listeners.delete(runId);
        }
        if (consumer) void consumer.close().catch(() => {});
    };
    request.signal.addEventListener("abort", cleanup, { once: true });

    const body = new ReadableStream({
        start(value) {
            controller = value;
            void (async () => {
                const saved = await loadResult(runId);
                if (!live) return;
                const deliver = (result) => {
                    if (!live) return;
                    send("result", result);
                    cleanup();
                    controller.close();
                };
                if (saved) {
                    deliver(saved);
                    return;
                }
                finish = deliver;
                const subscribers = listeners.get(runId) ?? new Set();
                subscribers.add(deliver);
                listeners.set(runId, subscribers);
                if (completed.has(runId)) {
                    deliver(completed.get(runId));
                    return;
                }
                heartbeat = setInterval(() => {
                    if (live)
                        controller.enqueue(encoder.encode(": keepalive\n\n"));
                }, 15_000);
                consumer = await connection.createChannel();
                consumer.on("error", () => {});
                if (!live) {
                    await consumer.close();
                    return;
                }
                await consumer.prefetch(100);
                await consumer.consume(
                    EVENT_STREAM,
                    (message) => {
                        if (!message) return;
                        try {
                            const event = eventSchema.safeParse(
                                JSON.parse(message.content.toString("utf8")),
                            );
                            if (
                                live &&
                                event.success &&
                                event.data.runId === runId &&
                                event.data.sequence > after
                            ) {
                                after = event.data.sequence;
                                send("delta", event.data, after);
                            }
                        } catch {
                            /* The final result remains authoritative. */
                        } finally {
                            try {
                                consumer.ack(message);
                            } catch {
                                /* Consumer was cancelled. */
                            }
                        }
                    },
                    { noAck: false, arguments: { "x-stream-offset": "first" } },
                );
                if (!live) await consumer.close().catch(() => {});
            })().catch(() => {
                if (live) {
                    send("status", {
                        error: "Stream unavailable; reload to retry",
                    });
                    cleanup();
                    controller.close();
                }
            });
        },
        cancel() {
            cleanup();
        },
    });
    return new Response(body, {
        headers: {
            "Content-Type": "text/event-stream; charset=utf-8",
            "Cache-Control": "no-store, no-transform",
            "X-Accel-Buffering": "no",
        },
    });
}

Bun.serve({
    hostname: "0.0.0.0",
    port: PORT,
    async fetch(request) {
        try {
            const url = new URL(request.url);
            if (request.method === "GET" && url.pathname === "/") {
                return new Response(page, {
                    headers: {
                        "Content-Type": "text/html; charset=utf-8",
                        "Cache-Control": "no-store",
                        "X-Content-Type-Options": "nosniff",
                    },
                });
            }
            if (request.method === "GET" && url.pathname === "/health")
                return json({ status: "ok" });
            if (request.method === "POST" && url.pathname === "/api/runs")
                return await submit(request);
            const match =
                request.method === "GET" &&
                /^\/api\/runs\/([0-9a-f-]+)\/events$/.exec(url.pathname);
            if (match && idSchema.safeParse(match[1]).success)
                return stream(request, match[1]);
            return json({ error: "Not found" }, 404);
        } catch {
            return json({ error: "Demo backend error" }, 500);
        }
    },
});
console.log(`Local demo ready on port ${PORT}`);
