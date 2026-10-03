import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { fileURLToPath } from "node:url";
import type {
    SessionEntry,
    SessionHeader,
} from "@earendil-works/pi-coding-agent";
import { z } from "zod";

export type PiJsonOptions = {
    cliPath: string;
    cwd: string;
    prompt: string;
    model: string;
    thinking: string | null;
    timeoutMs: number;
    signal?: AbortSignal;
    onDelta: (delta: string) => void;
};

export type ThreadContext = { header: SessionHeader; entries: SessionEntry[] };

export type PiThreadOptions = Omit<PiJsonOptions, "cliPath"> & {
    context: ThreadContext | null;
};

const MAX_LINE_BYTES = 8 * 1024 * 1024;

const MAX_TEXT_BYTES = 1024 * 1024;

const eventType = z.object({ type: z.string() });

const updateType = z.object({
    type: z.literal("message_update"),
    assistantMessageEvent: eventType,
});

const textDelta = z.object({
    type: z.literal("message_update"),
    assistantMessageEvent: z.object({
        type: z.literal("text_delta"),
        delta: z.string(),
    }),
});

const messageEnd = z.object({
    type: z.literal("message_end"),
    message: z.object({ role: z.string() }),
});

const assistantEnd = z.object({
    type: z.literal("message_end"),
    message: z.object({
        role: z.literal("assistant"),
        stopReason: z.string(),
        content: z.array(z.looseObject({ type: z.string() })),
    }),
});

const textBlock = z.object({ type: z.literal("text"), text: z.string() });

const threadRecord = z.discriminatedUnion("type", [
    z.object({ type: z.literal("text_delta"), delta: z.string() }),
    z.object({ type: z.literal("failure"), error: z.string() }),
    z.object({
        type: z.literal("finished"),
        output: z.string(),
        context: z.object({
            // Preserve Pi's complete session tree; z.object() would strip message
            // content and parent links, silently losing context on the next turn.
            header: z.looseObject({
                type: z.literal("session"),
                id: z.string(),
                cwd: z.string(),
                timestamp: z.string(),
            }),
            entries: z.array(
                z.looseObject({
                    type: z.string(),
                    id: z.string(),
                    parentId: z.string().nullable(),
                    timestamp: z.string(),
                }),
            ),
        }),
    }),
]);

/** Run Pi once, forwarding only live assistant text deltas and returning its last completed assistant text. */
export async function runPiJson(options: PiJsonOptions): Promise<string> {
    if (options.signal?.aborted) throw new Error("Pi run interrupted");

    if (
        !Number.isSafeInteger(options.timeoutMs) ||
        options.timeoutMs <= 0 ||
        options.timeoutMs > 2_147_483_647
    ) {
        throw new Error("timeoutMs must be a positive timer duration");
    }

    const args = [
        options.cliPath,
        "--mode",
        "json",
        "--no-session",
        "--model",
        options.model,
    ];

    if (options.thinking) args.push("--thinking", options.thinking);

    return new Promise<string>((resolve, reject) => {
        let child: ChildProcessWithoutNullStreams;

        try {
            child = spawn(process.execPath, args, {
                cwd: options.cwd,
                stdio: ["pipe", "pipe", "pipe"],
                detached: true,
            });
        } catch {
            reject(new Error("Pi run could not be started"));

            return;
        }

        const stopChild = (): void => {
            if (!child.pid) return;

            try {
                process.kill(-child.pid, "SIGKILL");
            } catch {
                child.kill("SIGKILL");
            }
        };

        let failure: Error | null = null;

        const fail = (message: string): void => {
            if (failure) return;
            failure = new Error(message);
            stopChild();
        };

        let lineBytes = 0;
        let lineParts: Buffer[] = [];
        let deltaBytes = 0;
        let finalText: string | null = null;
        let finalStopReason: string | null = null;
        const decoder = new TextDecoder("utf-8", { fatal: true });

        const handleLine = (line: Buffer): void => {
            if (line.at(-1) === 13) line = line.subarray(0, -1);
            let value: unknown;

            try {
                value = JSON.parse(decoder.decode(line));
            } catch {
                fail("Pi JSON stream is invalid");

                return;
            }

            const base = eventType.safeParse(value);

            if (!base.success) {
                fail("Pi JSON stream is invalid");

                return;
            }

            if (base.data.type === "message_update") {
                const update = updateType.safeParse(value);

                if (!update.success) {
                    fail("Pi JSON stream is invalid");

                    return;
                }

                if (update.data.assistantMessageEvent.type !== "text_delta")
                    return;
                const parsed = textDelta.safeParse(value);

                if (!parsed.success) {
                    fail("Pi JSON stream is invalid");

                    return;
                }

                const delta = parsed.data.assistantMessageEvent.delta;
                deltaBytes += Buffer.byteLength(delta, "utf8");

                if (deltaBytes > MAX_TEXT_BYTES) {
                    fail("Pi output exceeded the size limit");

                    return;
                }

                try {
                    options.onDelta(delta);
                } catch {
                    fail("Pi text delta callback failed");
                }
            } else if (base.data.type === "message_end") {
                const end = messageEnd.safeParse(value);

                if (!end.success) {
                    fail("Pi JSON stream is invalid");

                    return;
                }

                if (end.data.message.role !== "assistant") return;
                const parsed = assistantEnd.safeParse(value);

                if (!parsed.success) {
                    fail("Pi JSON stream is invalid");

                    return;
                }

                const parts: string[] = [];
                let size = 0;

                for (const block of parsed.data.message.content) {
                    if (block.type !== "text") continue;
                    const text = textBlock.safeParse(block);

                    if (!text.success) {
                        fail("Pi JSON stream is invalid");

                        return;
                    }

                    size += Buffer.byteLength(text.data.text, "utf8");

                    if (size > MAX_TEXT_BYTES) {
                        fail("Pi output exceeded the size limit");

                        return;
                    }

                    parts.push(text.data.text);
                }

                finalText = parts.join("");
                finalStopReason = parsed.data.message.stopReason;
            }
        };

        // JSONL is LF-framed, not Unicode-line-separator framed. Keep only one bounded line in memory.
        child.stdout.on("data", (chunk: Buffer) => {
            if (failure) return;
            let start = 0;

            while (start < chunk.length && !failure) {
                const newline = chunk.indexOf(10, start);
                const end = newline === -1 ? chunk.length : newline;
                const part = chunk.subarray(start, end);
                lineBytes += part.length;

                if (lineBytes > MAX_LINE_BYTES) {
                    fail("Pi output exceeded the size limit");

                    return;
                }

                if (part.length) lineParts.push(part);

                if (newline === -1) break;
                handleLine(Buffer.concat(lineParts, lineBytes));
                lineParts = [];
                lineBytes = 0;
                start = newline + 1;
            }
        });

        // stderr can include credentials; consume it without retaining or logging it.
        child.stderr.resume();
        child.stdout.on("error", () => fail("Pi run could not be started"));
        child.stderr.on("error", () => fail("Pi run could not be started"));
        child.stdin.on("error", () => fail("Pi run could not be started"));
        child.on("error", () => fail("Pi run could not be started"));
        const onAbort = (): void => fail("Pi run interrupted");
        options.signal?.addEventListener("abort", onAbort, { once: true });

        if (options.signal?.aborted) onAbort();

        const timer = setTimeout(
            () => fail("Pi run timed out"),
            options.timeoutMs,
        );

        if (!failure) {
            try {
                child.stdin.end(options.prompt);
            } catch {
                fail("Pi run could not be started");
            }
        }

        child.on("close", (code) => {
            clearTimeout(timer);
            options.signal?.removeEventListener("abort", onAbort);

            if (failure) reject(failure);
            else if (lineBytes !== 0)
                reject(new Error("Pi JSON stream is invalid"));
            else if (code !== 0)
                reject(
                    new Error(
                        `Pi exited unsuccessfully (code ${code ?? "unknown"})`,
                    ),
                );
            else if (finalText === null)
                reject(
                    new Error(
                        "Pi JSON stream is missing an assistant response",
                    ),
                );
            else if (
                finalStopReason === "error" ||
                finalStopReason === "aborted"
            ) {
                reject(new Error("Pi assistant response failed"));
            } else resolve(finalText);
        });
    });
}

/** Isolated SDK invocation: model context travels over stdin/stdout, not argv or session files. */
export async function runPiThread(options: PiThreadOptions): Promise<{
    output: string;
    context: ThreadContext;
}> {
    if (options.signal?.aborted) throw new Error("Pi run interrupted");

    const child = spawn(
        process.execPath,
        [fileURLToPath(new URL("./pi-thread-child.js", import.meta.url))],
        {
            cwd: options.cwd,
            stdio: ["pipe", "pipe", "pipe"],
            detached: true,
        },
    );

    const stop = (): void => {
        if (!child.pid) return;

        try {
            process.kill(-child.pid, "SIGKILL");
        } catch {
            child.kill("SIGKILL");
        }
    };

    return new Promise((resolve, reject) => {
        let failure: Error | null = null;
        let finished: { output: string; context: ThreadContext } | null = null;
        let lineBytes = 0;
        let lineParts: Buffer[] = [];
        let deltaBytes = 0;
        const decoder = new TextDecoder("utf-8", { fatal: true });

        const fail = (message: string): void => {
            if (failure) return;
            failure = new Error(message);
            stop();
        };

        const onAbort = (): void => fail("Pi run interrupted");
        options.signal?.addEventListener("abort", onAbort, { once: true });

        if (options.signal?.aborted) onAbort();

        const timer = setTimeout(
            () => fail("Pi run timed out"),
            options.timeoutMs,
        );

        const handleLine = (line: Buffer): void => {
            let record: z.infer<typeof threadRecord>;

            try {
                record = threadRecord.parse(JSON.parse(decoder.decode(line)));
            } catch {
                fail("Pi JSON stream is invalid");

                return;
            }

            if (record.type === "text_delta") {
                deltaBytes += Buffer.byteLength(record.delta, "utf8");

                if (deltaBytes > MAX_TEXT_BYTES) {
                    fail("Pi output exceeded the size limit");

                    return;
                }

                try {
                    options.onDelta(record.delta);
                } catch {
                    fail("Pi text delta callback failed");
                }
            } else if (record.type === "failure") {
                fail(record.error);
            } else {
                if (
                    finished ||
                    Buffer.byteLength(record.output, "utf8") > MAX_TEXT_BYTES
                ) {
                    fail("Pi output exceeded the size limit");

                    return;
                }

                // SAFETY: this is the Pi SDK child's own serialized FileEntry tree;
                // the schema checks its identity/linkage and retains every entry field.
                finished = {
                    output: record.output,
                    context: record.context as ThreadContext,
                };
            }
        };

        child.stdout.on("data", (chunk: Buffer) => {
            if (failure) return;
            let start = 0;

            while (start < chunk.length && !failure) {
                const newline = chunk.indexOf(10, start);
                const end = newline === -1 ? chunk.length : newline;
                const part = chunk.subarray(start, end);
                lineBytes += part.length;

                if (lineBytes > 32 * 1024 * 1024) {
                    fail("Pi output exceeded the size limit");

                    return;
                }

                if (part.length) lineParts.push(part);

                if (newline === -1) break;
                handleLine(Buffer.concat(lineParts, lineBytes));
                lineParts = [];
                lineBytes = 0;
                start = newline + 1;
            }
        });
        child.stderr.resume();
        child.on("error", () => fail("Pi run could not be started"));
        child.stdin.on("error", () => fail("Pi run could not be started"));
        child.stdout.on("error", () => fail("Pi run could not be started"));
        child.stderr.on("error", () => fail("Pi run could not be started"));
        child.on("close", (code) => {
            clearTimeout(timer);
            options.signal?.removeEventListener("abort", onAbort);

            if (failure) reject(failure);
            else if (code !== 0) reject(new Error("Pi exited unsuccessfully"));
            else if (lineBytes !== 0 || !finished)
                reject(new Error("Pi JSON stream is invalid"));
            else resolve(finished);
        });

        if (!failure) {
            try {
                child.stdin.end(
                    JSON.stringify({
                        prompt: options.prompt,
                        model: options.model,
                        thinking: options.thinking,
                        context: options.context,
                    }),
                );
            } catch {
                fail("Pi run could not be started");
            }
        }
    });
}
