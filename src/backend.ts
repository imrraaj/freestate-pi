import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { mkdir } from "node:fs/promises";
import { join } from "node:path";
import amqp, {
    type Channel,
    type ChannelModel,
    type ConfirmChannel,
    type ConsumeMessage,
} from "amqplib";
import postgres from "postgres";
import { z } from "zod";

export type BackendConfig = {
    databaseUrl: string;
    rabbitmqUrl: string;
    workspaceDir: string;
    piCliPath: string;
    runTimeoutMs?: number;
    requestQueue?: string;
    resultQueue?: string;
};

export type Backend = { close(): Promise<void> };

const DEFAULT_REQUEST_QUEUE = "freestate-pi.requests.v1";

const DEFAULT_RESULT_QUEUE = "freestate-pi.results.v1";

const DEFAULT_TIMEOUT_MS = 10 * 60 * 1000;

const MAX_OUTPUT_BYTES = 1024 * 1024;

const LEASE_MS = 30_000;

const HEARTBEAT_MS = 5_000;

const RETRY_MS = 1_000;

const requestSchema = z.strictObject({
    version: z.literal(1),
    runId: z.uuid().transform((id) => id.toLowerCase()),
    prompt: z.string().min(1).max(50_000),
    model: z
        .string()
        .min(3)
        .max(200)
        .regex(/^[a-zA-Z0-9][a-zA-Z0-9._-]*\/[a-zA-Z0-9][a-zA-Z0-9._:/-]*$/),
    thinking: z
        .enum(["off", "minimal", "low", "medium", "high", "xhigh", "max"])
        .optional()
        .transform((value) => value ?? null),
});

type Request = z.output<typeof requestSchema>;

const resultSchema = z.discriminatedUnion("status", [
    z.strictObject({
        version: z.literal(1),
        runId: z.uuid(),
        status: z.literal("completed"),
        output: z.string(),
        error: z.null(),
    }),
    z.strictObject({
        version: z.literal(1),
        runId: z.uuid(),
        status: z.literal("failed"),
        output: z.null(),
        error: z.string(),
    }),
]);

type Result = z.output<typeof resultSchema>;

function parseRequest(content: Buffer): Request | null {
    try {
        const value: unknown = JSON.parse(
            new TextDecoder("utf-8", { fatal: true }).decode(content),
        );

        const parsed = requestSchema.safeParse(value);

        return parsed.success ? parsed.data : null;
    } catch {
        return null;
    }
}

// Error messages returned to callers must never contain CLI stderr, URLs, or credentials.
function safeError(cause: unknown): string {
    if (
        cause instanceof Error &&
        /^(Pi run timed out|Pi output exceeded the size limit|Pi exited unsuccessfully|Pi run interrupted)/.test(
            cause.message,
        )
    ) {
        return cause.message;
    }

    return "Pi run could not be started";
}

export async function executePi(
    config: BackendConfig,
    id: string,
    prompt: string,
    model: string,
    thinking: string | null,
    signal?: AbortSignal,
): Promise<string> {
    if (signal?.aborted) throw new Error("Pi run interrupted");
    const cwd = join(config.workspaceDir, "runs", id);
    await mkdir(cwd, { recursive: true });

    if (signal?.aborted) throw new Error("Pi run interrupted");

    const args = [
        config.piCliPath,
        "--print",
        "--no-session",
        "--model",
        model,
    ];

    if (thinking) args.push("--thinking", thinking);

    return new Promise<string>((resolve, reject) => {
        const child = spawn(process.execPath, args, {
            cwd,
            stdio: ["pipe", "pipe", "pipe"],
            detached: true,
        });

        const stopChild = (): void => {
            if (!child.pid) return;

            try {
                process.kill(-child.pid, "SIGKILL");
            } catch {
                child.kill("SIGKILL");
            }
        };

        const chunks: Buffer[] = [];
        let size = 0;
        let exceeded = false;
        let timedOut = false;
        const onAbort = (): void => stopChild();
        signal?.addEventListener("abort", onAbort, { once: true });

        if (signal?.aborted) stopChild();

        const timer = setTimeout(() => {
            timedOut = true;
            stopChild();
        }, config.runTimeoutMs ?? DEFAULT_TIMEOUT_MS);

        child.stdout.on("data", (chunk: Buffer) => {
            size += chunk.length;

            if (size > MAX_OUTPUT_BYTES) {
                exceeded = true;
                stopChild();
            } else if (!exceeded) chunks.push(chunk);
        });
        // Drain but never store/log CLI stderr: it may include authentication material.
        child.stderr.resume();
        child.stdin.on("error", () => {});
        child.stdin.end(prompt);
        child.on("error", () => {});
        child.on("close", (code) => {
            clearTimeout(timer);
            signal?.removeEventListener("abort", onAbort);

            if (signal?.aborted) reject(new Error("Pi run interrupted"));
            else if (timedOut) reject(new Error("Pi run timed out"));
            else if (exceeded)
                reject(new Error("Pi output exceeded the size limit"));
            else if (code !== 0)
                reject(
                    new Error(
                        `Pi exited unsuccessfully (code ${code ?? "unknown"})`,
                    ),
                );
            else resolve(Buffer.concat(chunks).toString("utf8"));
        });
    });
}

export async function startBackend(config: BackendConfig): Promise<Backend> {
    if (
        !Number.isSafeInteger(config.runTimeoutMs ?? DEFAULT_TIMEOUT_MS) ||
        (config.runTimeoutMs ?? DEFAULT_TIMEOUT_MS) <= 0 ||
        (config.runTimeoutMs ?? DEFAULT_TIMEOUT_MS) > 2_147_483_647
    ) {
        throw new Error("runTimeoutMs must be a positive timer duration");
    }

    const requestQueue = config.requestQueue ?? DEFAULT_REQUEST_QUEUE;
    const resultQueue = config.resultQueue ?? DEFAULT_RESULT_QUEUE;

    if (!requestQueue || !resultQueue || requestQueue === resultQueue) {
        throw new Error(
            "Request and result queues must be distinct nonempty names",
        );
    }

    const db = postgres(config.databaseUrl, { max: 4 });
    const owner = randomUUID();
    let connection: ChannelModel | null = null;
    let publisher: ConfirmChannel | null = null;
    let consumer: Channel | null = null;
    let consumerTag: string | null = null;
    let closed = false;
    let connecting: Promise<void> | null = null;
    let publishing: Promise<void> | null = null;
    let activeJob: Promise<void> | null = null;
    let activeAbort: AbortController | null = null;
    let retryTimer: ReturnType<typeof setInterval> | null = null;
    let sweeping: Promise<void> | null = null;

    const sweepExpired = async (): Promise<void> => {
        if (sweeping) return sweeping;

        const work = (async () => {
            // Atomic status transition + outbox: never replay tool side effects after expiry.
            await db.begin(async (tx) => {
                const expired = await tx<{ id: string }[]>`
                    UPDATE pi_runner.runner_runs SET status = 'failed', output = NULL,
                        error = 'Run interrupted before completion', updated_at = now(),
                        lease_owner = NULL, lease_expires_at = NULL
                    WHERE status = 'running' AND (lease_expires_at IS NULL OR lease_expires_at <= now())
                    RETURNING id
                `;

                for (const row of expired) {
                    const result: Result = {
                        version: 1,
                        runId: row.id,
                        status: "failed",
                        output: null,
                        error: "Run interrupted before completion",
                    };

                    await tx`INSERT INTO pi_runner.runner_outbox (run_id, payload, result_queue)
                        VALUES (${row.id}, ${tx.json(result)}, ${resultQueue})
                        ON CONFLICT (run_id) DO UPDATE SET payload = EXCLUDED.payload,
                            result_queue = EXCLUDED.result_queue, published_at = NULL`;
                }
            });
        })();

        sweeping = work;

        try {
            await work;
        } finally {
            sweeping = null;
        }
    };

    const flushOutbox = async (): Promise<void> => {
        if (publishing) return publishing;

        const work = (async () => {
            const channel = publisher;

            if (!channel || closed) return;

            // Lock rows through broker confirmation. Other replicas skip these rows.
            // Publishing can be repeated after a crash between confirmation and PG commit.
            for (
                let batch = 0;
                batch < 100 && !closed && publisher === channel;
                batch++
            ) {
                const sent = await db.begin(async (tx) => {
                    const rows = await tx<
                        {
                            run_id: string;
                            payload: Result;
                            result_queue: string;
                        }[]
                    >`
                        SELECT run_id, payload, result_queue FROM pi_runner.runner_outbox
                        WHERE published_at IS NULL AND payload IS NOT NULL
                        ORDER BY run_id LIMIT 1 FOR UPDATE SKIP LOCKED`;

                    const row = rows[0];

                    if (!row || closed || publisher !== channel) return false;
                    await channel.assertQueue(row.result_queue, {
                        durable: true,
                    });
                    await new Promise<void>((resolve, reject) => {
                        const onClose = (): void =>
                            finish(new Error("RabbitMQ channel closed"));

                        const timer = setTimeout(() => {
                            // A lost publisher confirmation must not hold the DB row lock forever.
                            void channel.close().catch(() => {});
                            finish(
                                new Error("RabbitMQ confirmation timed out"),
                            );
                        }, 10_000);

                        const finish = (error?: Error): void => {
                            clearTimeout(timer);
                            channel.off("close", onClose);

                            if (error) reject(error);
                            else resolve();
                        };

                        channel.once("close", onClose);

                        try {
                            channel.sendToQueue(
                                row.result_queue,
                                Buffer.from(
                                    JSON.stringify(
                                        resultSchema.parse(row.payload),
                                    ),
                                ),
                                {
                                    persistent: true,
                                    contentType: "application/json",
                                    messageId: row.run_id,
                                },
                                (error) =>
                                    finish(
                                        error
                                            ? new Error(
                                                  "RabbitMQ publish failed",
                                              )
                                            : undefined,
                                    ),
                            );
                        } catch {
                            finish(new Error("RabbitMQ publish failed"));
                        }
                    });
                    await tx`UPDATE pi_runner.runner_outbox SET published_at = now() WHERE run_id = ${row.run_id}`;

                    return true;
                });

                if (!sent) return;
            }
        })();

        publishing = work;

        try {
            await work;
        } finally {
            publishing = null;
        }
    };

    const awaitResultPublished = async (id: string): Promise<void> => {
        while (!closed && publisher) {
            await flushOutbox();

            const rows = await db<{ published_at: Date | null }[]>`
                SELECT published_at FROM pi_runner.runner_outbox WHERE run_id = ${id}`;

            if (rows[0]?.published_at) return;
            // The owning replica may still be running. A sweep eventually records an
            // expired owner's failure, without ever executing the request again.
            await new Promise<void>((resolve) => setTimeout(resolve, 200));
        }

        throw new Error("Broker disconnected before result confirmation");
    };

    const processJob = async (
        message: ConsumeMessage,
        channel: Channel,
    ): Promise<void> => {
        const request = parseRequest(message.content);

        if (!request) {
            // An operator-configured DLX receives rejected messages; absent a DLX they are discarded.
            channel.nack(message, false, false);

            return;
        }

        try {
            const { runId: id } = request;

            const claimed = await db.begin(async (tx) => {
                await tx`INSERT INTO pi_runner.runner_runs (id, status, prompt, model, thinking)
                    VALUES (${id}, 'queued', ${request.prompt}, ${request.model}, ${request.thinking})
                    ON CONFLICT (id) DO NOTHING`;

                const rows = await tx<
                    { prompt: string; model: string; thinking: string | null }[]
                >`
                    SELECT prompt, model, thinking FROM pi_runner.runner_runs WHERE id = ${id} FOR UPDATE`;

                const stored = rows[0];

                if (
                    !stored ||
                    stored.prompt !== request.prompt ||
                    stored.model !== request.model ||
                    stored.thinking !== request.thinking
                )
                    return "conflict";

                const rowsClaimed = await tx<{ id: string }[]>`
                    UPDATE pi_runner.runner_runs SET status = 'running', lease_owner = ${owner},
                        lease_expires_at = now() + ${LEASE_MS} * interval '1 millisecond', updated_at = now()
                    WHERE id = ${id} AND status = 'queued' RETURNING id`;

                return rowsClaimed.length ? "claimed" : "duplicate";
            });

            if (claimed === "conflict") {
                // Never fail/mutate a different request under the same ID.
                channel.nack(message, false, false);

                return;
            }

            if (claimed === "duplicate") {
                // A different replica can own this run. Wait for its durable result;
                // never change its status or invoke Pi again on a redelivery.
                await awaitResultPublished(id);
                channel.ack(message);

                return;
            }

            const abort = new AbortController();
            activeAbort = abort;
            let heartbeatBusy = false;

            const heartbeat = setInterval(() => {
                if (heartbeatBusy || abort.signal.aborted) return;
                heartbeatBusy = true;
                void db<{ id: string }[]>`
                    UPDATE pi_runner.runner_runs SET lease_expires_at = now() + ${LEASE_MS} * interval '1 millisecond'
                    WHERE id = ${id} AND status = 'running' AND lease_owner = ${owner}
                        AND lease_expires_at > now() RETURNING id
                `
                    .then(
                        (rows) => {
                            if (!rows.length) abort.abort();
                        },
                        () => abort.abort(),
                    )
                    .finally(() => {
                        heartbeatBusy = false;
                    });
            }, HEARTBEAT_MS);

            let output: string | null = null;
            let error: string | null = null;

            try {
                output = await executePi(
                    config,
                    id,
                    request.prompt,
                    request.model,
                    request.thinking,
                    abort.signal,
                );
            } catch (cause) {
                error = safeError(cause);
            } finally {
                clearInterval(heartbeat);
                activeAbort = null;
            }

            await db.begin(async (tx) => {
                const result = await tx<{ id: string }[]>`
                    UPDATE pi_runner.runner_runs SET status = ${error ? "failed" : "completed"},
                        output = ${output}, error = ${error}, updated_at = now(),
                        lease_owner = NULL, lease_expires_at = NULL
                    WHERE id = ${id} AND status = 'running' AND lease_owner = ${owner}
                        AND lease_expires_at > now() RETURNING id`;

                if (!result.length) return; // Another replica expired the lease; it owns the failure result.

                const payload: Result = error
                    ? {
                          version: 1,
                          runId: id,
                          status: "failed",
                          output: null,
                          error,
                      }
                    : {
                          version: 1,
                          runId: id,
                          status: "completed",
                          output: output ?? "",
                          error: null,
                      };

                await tx`INSERT INTO pi_runner.runner_outbox (run_id, payload, result_queue)
                    VALUES (${id}, ${tx.json(payload)}, ${resultQueue})
                    ON CONFLICT (run_id) DO UPDATE SET payload = EXCLUDED.payload,
                        result_queue = EXCLUDED.result_queue, published_at = NULL`;
            });
            // If the lease expired before the terminal transaction, ensure a failure is persisted.
            await sweepExpired();
            await awaitResultPublished(id);
            channel.ack(message);
        } catch {
            // DB errors must requeue uncommitted work; any already-claimed run will be
            // terminalized by the lease sweeper rather than invoking Pi again.
            activeAbort?.abort();
            await channel.close().catch(() => {});
        }
    };

    const connectBroker = async (): Promise<void> => {
        if (closed || connection) return;

        if (connecting) return connecting;

        const work = (async () => {
            let conn: ChannelModel | null = null;

            try {
                // Never consume a redelivery while the original Pi invocation is live.
                if (activeJob) await activeJob;

                if (closed || connection) return;
                conn = await amqp.connect(config.rabbitmqUrl);
                conn.on("error", () => {});
                conn.on("close", () => {
                    if (connection === conn) {
                        connection = null;
                        publisher = null;
                        consumer = null;
                        consumerTag = null;
                    }
                });

                if (closed) {
                    await conn.close();

                    return;
                }

                const pub = await conn.createConfirmChannel();
                pub.on("error", () => {});
                pub.on("close", () => {
                    if (publisher === pub) void conn?.close().catch(() => {});
                });
                await pub.assertQueue(resultQueue, { durable: true });
                const sub = await conn.createChannel();
                sub.on("error", () => {});
                sub.on("close", () => {
                    if (consumer === sub) {
                        consumer = null;
                        consumerTag = null;
                        void conn?.close().catch(() => {});
                    }
                });
                await sub.assertQueue(requestQueue, { durable: true });
                await sub.prefetch(1);
                connection = conn;
                publisher = pub;
                consumer = sub;

                const subscription = await sub.consume(
                    requestQueue,
                    (message) => {
                        if (!message) return;
                        const job = processJob(message, sub);
                        activeJob = job;
                        void job.finally(() => {
                            if (activeJob === job) activeJob = null;
                        });
                    },
                    { noAck: false },
                );

                consumerTag = subscription.consumerTag;
                await flushOutbox();
            } catch (error) {
                if (conn) await conn.close().catch(() => {});

                if (connection === conn) {
                    connection = null;
                    publisher = null;
                    consumer = null;
                    consumerTag = null;
                }

                throw error;
            }
        })();

        connecting = work;

        try {
            await work;
        } finally {
            connecting = null;
        }
    };

    try {
        // Fresh standalone schema. The transaction-scoped lock serializes startup DDL
        // across replicas; IF NOT EXISTS makes restarts harmless without migrations.
        await db.begin(async (tx) => {
            await tx`SELECT pg_advisory_xact_lock(7167, 1)`;
            await tx`CREATE SCHEMA IF NOT EXISTS pi_runner`;
            await tx`CREATE TABLE IF NOT EXISTS pi_runner.runner_runs (
                id uuid PRIMARY KEY,
                status text NOT NULL CHECK (status IN ('queued', 'running', 'completed', 'failed')),
                prompt text NOT NULL,
                model text NOT NULL,
                thinking text,
                output text,
                error text,
                lease_owner uuid,
                lease_expires_at timestamptz,
                created_at timestamptz NOT NULL DEFAULT now(),
                updated_at timestamptz NOT NULL DEFAULT now()
            )`;
            await tx`CREATE TABLE IF NOT EXISTS pi_runner.runner_outbox (
                run_id uuid PRIMARY KEY REFERENCES pi_runner.runner_runs(id),
                payload jsonb NOT NULL,
                result_queue text NOT NULL,
                published_at timestamptz
            )`;
            await tx`CREATE INDEX IF NOT EXISTS runner_runs_expired_lease_idx
                ON pi_runner.runner_runs (lease_expires_at) WHERE status = 'running'`;
            await tx`CREATE INDEX IF NOT EXISTS runner_outbox_pending_idx
                ON pi_runner.runner_outbox (run_id) WHERE published_at IS NULL`;
        });
        await sweepExpired();
        await connectBroker();
        retryTimer = setInterval(() => {
            void sweepExpired().catch(() => {});
            void (connection ? flushOutbox() : connectBroker()).catch(() => {});
        }, RETRY_MS);
    } catch (error) {
        closed = true;
        // SAFETY: connectBroker assigns connection inside a closure that TS cannot track.
        await (connection as ChannelModel | null)?.close().catch(() => {});
        await db.end().catch(() => {});
        throw error;
    }

    return {
        async close(): Promise<void> {
            if (closed) return;
            closed = true;

            if (retryTimer) clearInterval(retryTimer);

            if (consumer && consumerTag)
                await consumer.cancel(consumerTag).catch(() => {});
            activeAbort?.abort();

            if (activeJob) await activeJob;

            if (publishing) await publishing.catch(() => {});

            if (sweeping) await sweeping.catch(() => {});

            if (connection) await connection.close().catch(() => {});
            await db.end();
        },
    };
}
