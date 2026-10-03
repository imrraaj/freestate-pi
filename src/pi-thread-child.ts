import {
    createAgentSession,
    ModelRuntime,
    SessionManager,
    type FileEntry,
    type SessionEntry,
    type SessionHeader,
} from "@earendil-works/pi-coding-agent";
import { z } from "zod";

const inputSchema = z.strictObject({
    prompt: z.string().min(1),
    model: z.string().regex(/^[a-zA-Z0-9][a-zA-Z0-9._-]*\/.+$/),
    thinking: z.enum([
        "off",
        "minimal",
        "low",
        "medium",
        "high",
        "xhigh",
        "max",
    ]),
    context: z
        .object({
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
        })
        .nullable(),
});

// stdin/stdout are a private JSONL protocol. Never write transcript data to stderr.
async function main(): Promise<void> {
    const chunks: Buffer[] = [];
    let bytes = 0;

    for await (const chunk of process.stdin) {
        const part = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
        bytes += part.length;

        if (bytes > 32 * 1024 * 1024) throw new Error("Input too large");
        chunks.push(part);
    }

    const input = inputSchema.parse(
        JSON.parse(Buffer.concat(chunks).toString("utf8")),
    );

    const [provider, ...modelParts] = input.model.split("/");
    const runtime = await ModelRuntime.create();

    const model = provider
        ? runtime.getModel(provider, modelParts.join("/"))
        : null;

    if (!model) {
        process.stdout.write(
            JSON.stringify({
                type: "failure",
                error: "Pi model is unavailable",
            }) + "\n",
        );
        process.exitCode = 1;

        return;
    }

    // SAFETY: these are the complete Pi FileEntry values sent by our parent process;
    // Zod checks their identity/links and retains their remaining SDK fields.
    const entries: FileEntry[] | undefined = input.context
        ? [
              input.context.header as SessionHeader,
              ...(input.context.entries as SessionEntry[]),
          ]
        : undefined;

    const manager = SessionManager.inMemory(process.cwd(), undefined, entries);

    const { session } = await createAgentSession({
        cwd: process.cwd(),
        sessionManager: manager,
        modelRuntime: runtime,
        model,
        thinkingLevel: input.thinking,
    });

    const unsubscribe = session.subscribe((event) => {
        if (
            event.type === "message_update" &&
            event.assistantMessageEvent.type === "text_delta"
        ) {
            process.stdout.write(
                JSON.stringify({
                    type: "text_delta",
                    delta: event.assistantMessageEvent.delta,
                }) + "\n",
            );
        }
    });

    try {
        await session.prompt(input.prompt);

        const last = [...session.messages]
            .reverse()
            .find((message) => message.role === "assistant");

        if (
            !last ||
            last.stopReason === "error" ||
            last.stopReason === "aborted"
        ) {
            process.stdout.write(
                '{"type":"failure","error":"Pi assistant response failed"}\n',
            );
            process.exitCode = 1;

            return;
        }

        const output = session.getLastAssistantText();

        if (output === undefined) {
            process.stdout.write(
                '{"type":"failure","error":"Pi JSON stream is missing an assistant response"}\n',
            );
            process.exitCode = 1;

            return;
        }

        process.stdout.write(
            JSON.stringify({
                type: "finished",
                output,
                context: {
                    header: manager.getHeader(),
                    entries: manager.getEntries(),
                },
            }) + "\n",
        );
    } finally {
        unsubscribe();
        session.dispose();
    }
}

main().catch(() => {
    // Errors may include credentials, tool output or URLs. The parent reports a generic failure.
    process.stdout.write(
        '{"type":"failure","error":"Pi run could not be started"}\n',
    );
    process.exitCode = 1;
});
