import assert from "node:assert/strict";
import test from "node:test";
import { once } from "node:events";
import { setTimeout as delay } from "node:timers/promises";
import { WebSocket } from "ws";
import { PenpotMcpServer } from "./PenpotMcpServer";

async function fixture(run: (f: any) => Promise<void>, options: Record<string, string> = {}) {
    const settings = {
        PENPOT_MCP_SERVER_HOST: "127.0.0.1",
        PENPOT_MCP_SERVER_PORT: "0",
        PENPOT_MCP_WEBSOCKET_PORT: "0",
        PENPOT_MCP_JSON_RESPONSE: "true",
        PENPOT_MCP_MAX_PLUGIN_CONNECTIONS_PER_USER: "30",
        PENPOT_MCP_MAX_PENDING_TASKS_PER_PLUGIN: "30",
        PENPOT_MCP_TOOL_TIMEOUT_S: "2",
        ...options,
    };
    const before = { ...process.env };
    Object.assign(process.env, settings);
    delete process.env.PENPOT_MCP_REDIS_URI;
    const server = new PenpotMcpServer(true);
    const plugins: WebSocket[] = [];
    try {
        await server.start();
        const http = (server as any).httpServer.address().port;
        const wss = (server.pluginBridge as any).wsServer;
        if (!wss.address()) await once(wss, "listening");
        const target = (token = "owner", session = "") => {
            const query = new URLSearchParams({ userToken: token });
            if (session) query.set("pluginSessionId", session);
            return query.toString();
        };
        const plugin = async (session = "", token = "owner", handler?: (task: any, ws: WebSocket) => void) => {
            const ws = new WebSocket(`ws://127.0.0.1:${wss.address().port}/?${target(token, session)}`);
            plugins.push(ws);
            ws.on("message", (raw) => {
                const task = JSON.parse(raw.toString());
                if (task.task !== "executeCode") {
                    ws.send(JSON.stringify({ id: task.id, success: false, error: "Unknown plugin task type" }));
                    return;
                }
                if (handler) handler(task, ws);
                else ws.send(JSON.stringify({ id: task.id, success: true, data: { session, code: task.params.code } }));
            });
            await once(ws, "open");
            return ws;
        };
        const client = async (session = "", token = "owner") => {
            const url = `http://127.0.0.1:${http}/mcp?${target(token, session)}`;
            const headers: Record<string, string> = {
                "Content-Type": "application/json",
                Accept: "application/json, text/event-stream",
            };
            const post = (body: any, alternate = url) =>
                fetch(alternate, {
                    method: "POST",
                    headers,
                    body: JSON.stringify(body),
                    signal: AbortSignal.timeout(7000),
                });
            const init = await post({
                jsonrpc: "2.0",
                id: 1,
                method: "initialize",
                params: {
                    protocolVersion: "2025-03-26",
                    capabilities: {},
                    clientInfo: { name: "parallel-test", version: "1" },
                },
            });
            await init.json();
            headers["mcp-session-id"] = init.headers.get("mcp-session-id")!;
            assert.ok(headers["mcp-session-id"]);
            await post({ jsonrpc: "2.0", method: "notifications/initialized" });
            return {
                post,
                url,
                closeSession: () => fetch(url, { method: "DELETE", headers }),
                async call(code: string, id = 2) {
                    const response = await post({
                        jsonrpc: "2.0",
                        id,
                        method: "tools/call",
                        params: {
                            name: "execute_code",
                            arguments: { code },
                        },
                    });
                    const reply: any = await response.json();
                    assert.equal(reply.id, id);
                    if (reply.result.content[0].text.startsWith("Tool execution failed:")) {
                        assert.equal(reply.result.isError, true, "tool failures must be protocol errors");
                    }
                    return reply.result.content[0].text as string;
                },
            };
        };
        await run({ server, plugin, client });
    } finally {
        await server.stop();
        for (const ws of plugins) if (ws.readyState !== WebSocket.CLOSED) ws.terminate();
        for (const name of Object.keys(settings)) {
            if (before[name] === undefined) delete process.env[name];
            else process.env[name] = before[name];
        }
        if (before.PENPOT_MCP_REDIS_URI !== undefined) process.env.PENPOT_MCP_REDIS_URI = before.PENPOT_MCP_REDIS_URI;
    }
}

test("30 same-owner plugin sessions route 300 simultaneous calls without cross-talk", async () => {
    await fixture(async ({ plugin, client }) => {
        await Promise.all(Array.from({ length: 30 }, (_, i) => plugin(`worker-${i}`)));
        const clients = await Promise.all(Array.from({ length: 30 }, (_, i) => client(`worker-${i}`)));
        await Promise.all(
            clients.flatMap((c: any, i: number) =>
                Array.from({ length: 10 }, async (_, j) => {
                    const result = JSON.parse(await c.call(`job-${i}-${j}`, j + 2));
                    assert.deepEqual(result, { session: `worker-${i}`, code: `job-${i}-${j}` });
                })
            )
        );
        const wrongOwner = await client("worker-0", "other-owner");
        assert.match(await wrongOwner.call("never"), /No Penpot instance/);
        const unselected = await client();
        assert.match(await unselected.call("never"), /No Penpot instance/);
    });
});

test("30 independent MCP clients share one legacy editor without overlapping tasks", async () => {
    await fixture(async ({ plugin, client }) => {
        let active = 0,
            maximum = 0;
        await plugin("", "owner", async (task: any, ws: WebSocket) => {
            maximum = Math.max(maximum, ++active);
            await delay(5);
            active--;
            ws.send(JSON.stringify({ id: task.id, success: true, data: task.params.code }));
        });
        const clients = await Promise.all(Array.from({ length: 30 }, () => client()));
        await Promise.all(
            clients.map(async (c: any, i: number) => {
                assert.equal(JSON.parse(await c.call(`job-${i}`)), `job-${i}`);
            })
        );
        assert.equal(maximum, 1, "one editor must have one active task");
    });
});

test("duplicate and over-capacity plugins do not evict the owner's connections", async () => {
    await fixture(
        async ({ plugin, client }) => {
            await plugin("a");
            const duplicate = await plugin("a");
            assert.equal((await once(duplicate, "close"))[0], 1008);
            await plugin("b");
            const excess = await plugin("c");
            assert.equal((await once(excess, "close"))[0], 1008);
            assert.equal(JSON.parse(await (await client("a")).call("alive")).session, "a");
            assert.equal(JSON.parse(await (await client("b")).call("alive")).session, "b");
        },
        { PENPOT_MCP_MAX_PLUGIN_CONNECTIONS_PER_USER: "2" }
    );
});

test("a reconnecting editor replaces only its abandoned idle registration", async () => {
    await fixture(
        async ({ server, plugin, client }) => {
            const bridge = server.pluginBridge as any;
            const silence = (ws: WebSocket) => {
                for (const connection of bridge.connectedClients.values()) {
                    if (
                        connection.socket.readyState === WebSocket.OPEN &&
                        connection.pluginSessionId === ws.url.split("pluginSessionId=")[1]
                    ) {
                        connection.lastHeartbeat = Date.now() - 40000;
                    }
                }
            };
            // an idle editor that went silent is released for its reconnecting successor
            const crashed = await plugin("a");
            silence(crashed);
            const reconnected = await plugin("a");
            assert.equal((await once(crashed, "close"))[0], 1006);
            assert.equal(reconnected.readyState, WebSocket.OPEN);
            assert.equal(JSON.parse(await (await client("a")).call("alive")).session, "a");

            // at capacity, an abandoned idle editor frees its slot for a new one
            const idle = await plugin("b");
            silence(idle);
            const added = await plugin("c");
            assert.equal((await once(idle, "close"))[0], 1006);
            assert.equal(JSON.parse(await (await client("c")).call("alive")).session, "c");
            assert.equal(added.readyState, WebSocket.OPEN);

            // a silent editor that still holds work is never released
            silence(reconnected);
            let release: (() => void) | undefined;
            const busy = await plugin("d", "owner", (task: any, ws: WebSocket) => {
                release = () => ws.send(JSON.stringify({ id: task.id, success: true, data: task.params.code }));
            });
            const working = (await client("d")).call("hold");
            while (!release) {
                await delay(5);
            }
            silence(busy);
            const rejected = await plugin("d");
            assert.equal((await once(rejected, "close"))[0], 1008);
            release();
            assert.equal(JSON.parse(await working), "hold");
        },
        { PENPOT_MCP_MAX_PLUGIN_CONNECTIONS_PER_USER: "2" }
    );
});

test("a transport cannot switch plugin selector after initialization", async () => {
    await fixture(async ({ plugin, client }) => {
        await plugin("a");
        await plugin("b");
        const c = await client("a");
        const response = await c.post(
            { jsonrpc: "2.0", id: 2, method: "tools/list" },
            c.url.replace("pluginSessionId=a", "pluginSessionId=b")
        );
        assert.equal(response.status, 403);
        await response.text();
    });
});

test("disconnect rejects in-flight and queued work promptly, with no replay on reconnect", async () => {
    await fixture(async ({ plugin, client }) => {
        let received!: () => void;
        const seen = new Promise<void>((resolve) => (received = resolve));
        const ws = await plugin("a", "owner", () => received());
        const c = await client("a");
        const first = c.call("held");
        await seen;
        const second = c.call("queued", 3);
        await delay(30);
        ws.close();
        await once(ws, "close");
        const results = await Promise.race([
            Promise.all([first, second]),
            delay(500).then(() => {
                throw new Error("disconnect hung");
            }),
        ]);
        assert.match(results[0], /outcome.*unknown/i);
        assert.match(results[1], /not dispatched/i);
        const again = await plugin("a");
        assert.equal(JSON.parse(await c.call("new", 4)).code, "new");
        assert.equal(again.readyState, WebSocket.OPEN);
    });
});

test("execution timeout cancels queued tasks instead of starting uncertain mutations", async () => {
    await fixture(
        async ({ plugin, client }) => {
            const sent: string[] = [];
            let received!: () => void;
            const seen = new Promise<void>((resolve) => (received = resolve));
            await plugin("a", "owner", (task: any) => {
                sent.push(task.params.code);
                received();
            });
            const c = await client("a");
            const first = c.call("held");
            await seen;
            const results = await Promise.all([first, c.call("must-not-run", 3)]);
            assert.match(results[0], /outcome.*unknown/i);
            assert.match(results[1], /not dispatched/i);
            assert.deepEqual(sent, ["held"]);
        },
        { PENPOT_MCP_TOOL_TIMEOUT_S: "1" }
    );
});

test("a different plugin cannot settle a task by guessing its ID", async () => {
    await fixture(async ({ plugin, client }) => {
        const rogue = await plugin("b");
        await plugin("a", "owner", async (task: any, ws: WebSocket) => {
            rogue.send(JSON.stringify({ id: task.id, success: true, data: "forged" }));
            await delay(20);
            ws.send(JSON.stringify({ id: task.id, success: true, data: "correct" }));
        });
        assert.equal(JSON.parse(await (await client("a")).call("read")), "correct");
    });
});

test("queue overflow fails before dispatch and releases capacity after completion", async () => {
    await fixture(
        async ({ plugin, client }) => {
            let held: any;
            let received!: () => void;
            const seen = new Promise<void>((resolve) => (received = resolve));
            const ws = await plugin("a", "owner", (task: any) => {
                held = task;
                received();
            });
            const c = await client("a");
            const first = c.call("held");
            await seen;
            assert.match(await c.call("excess", 3), /queue is full.*not dispatched/i);
            ws.send(JSON.stringify({ id: held.id, success: true, data: "released" }));
            assert.equal(JSON.parse(await first), "released");
        },
        { PENPOT_MCP_MAX_PENDING_TASKS_PER_PLUGIN: "1" }
    );
});

test("queue wait timeout never sends the expired task to the plugin", async () => {
    await fixture(
        async ({ plugin, client }) => {
            let held: any;
            let received!: () => void;
            const seen = new Promise<void>((resolve) => (received = resolve));
            const sent: string[] = [];
            const ws = await plugin("a", "owner", (task: any) => {
                held = task;
                sent.push(task.params.code);
                received();
            });
            const c = await client("a");
            const first = c.call("held");
            await seen;
            assert.match(await c.call("expired", 3), /queue wait timed out.*not dispatched/i);
            ws.send(JSON.stringify({ id: held.id, success: true, data: "released" }));
            assert.equal(JSON.parse(await first), "released");
            await delay(20);
            assert.deepEqual(sent, ["held"]);
        },
        { PENPOT_MCP_QUEUE_TIMEOUT_S: "1", PENPOT_MCP_TOOL_TIMEOUT_S: "3" }
    );
});

test("idle legacy plugin without heartbeats is probed once before 30 clients resume", async () => {
    await fixture(async ({ server, plugin, client }) => {
        const sent: string[] = [];
        await plugin("legacy", "owner", async (task: any, ws: WebSocket) => {
            sent.push(task.params.code);
            await delay(2);
            ws.send(JSON.stringify({ id: task.id, success: true, data: task.params.code }));
        });
        const clients = await Promise.all(Array.from({ length: 30 }, () => client("legacy")));
        for (const connection of (server.pluginBridge as any).connectedClients.values()) {
            connection.lastHeartbeat = Date.now() - 40000;
        }
        await Promise.all(
            clients.map(async (c: any, i: number) => {
                assert.equal(JSON.parse(await c.call(`requested-${i}`)), `requested-${i}`);
            })
        );
        assert.equal(sent[0], "return null;", "read-only event-loop check precedes requested work");
        assert.equal(sent.filter((code) => code === "return null;").length, 1);
        assert.equal(sent.length, 31);
    });
});

test("unresponsive idle plugin never receives the queued operation", async () => {
    await fixture(
        async ({ server, plugin, client }) => {
            const sent: string[] = [];
            await plugin("idle", "owner", (task: any) => sent.push(task.params.code));
            for (const connection of (server.pluginBridge as any).connectedClients.values()) {
                connection.lastHeartbeat = Date.now() - 40000;
            }
            const c = await client("idle");
            assert.match(await c.call("must-not-run"), /not dispatched/i);
            assert.deepEqual(sent, ["return null;"]);
        },
        { PENPOT_MCP_LIVENESS_PROBE_TIMEOUT_S: "1" }
    );
});

for (const cancellation of ["notification", "session-close"]) {
    test(`cancelled queued work is never dispatched after ${cancellation}`, async () => {
        await fixture(async ({ server, plugin, client }) => {
            let first: any;
            const sent: string[] = [];
            const ws = await plugin("", "owner", (task: any, socket: WebSocket) => {
                sent.push(task.params.code);
                if (task.params.code === "hold-first") first = task;
                else socket.send(JSON.stringify({ id: task.id, success: true, data: task.params.code }));
            });
            const a = await client();
            const b = await client();
            const active = a.call("hold-first", 10);
            while (!first) await delay(2);
            // Cancellation may close the HTTP response without a JSON result.
            void b.call("must-not-run", 20).catch(() => {});
            while ((server.pluginBridge as any).pendingTasks.size < 2) await delay(2);
            const cancelled =
                cancellation === "notification"
                    ? await b.post({
                          jsonrpc: "2.0",
                          method: "notifications/cancelled",
                          params: { requestId: 20, reason: "caller stopped" },
                      })
                    : await b.closeSession();
            await cancelled.text();
            await delay(20);
            ws.send(JSON.stringify({ id: first.id, success: true, data: "first-done" }));
            assert.equal(JSON.parse(await active), "first-done");
            await delay(30);
            assert.deepEqual(sent, ["hold-first"], "cancelled queued code must never reach the plugin");
        });
    });
}

test("cancelling active work does not allow another task to overlap its execution", async () => {
    await fixture(async ({ server, plugin, client }) => {
        let first: any;
        const sent: string[] = [];
        const ws = await plugin("", "owner", (task: any, socket: WebSocket) => {
            sent.push(task.params.code);
            if (task.params.code === "hold-first") first = task;
            else socket.send(JSON.stringify({ id: task.id, success: true, data: task.params.code }));
        });
        const a = await client();
        const b = await client();
        void a.call("hold-first", 10).catch(() => {});
        while (!first) await delay(2);
        const next = b.call("next", 20);
        while ((server.pluginBridge as any).pendingTasks.size < 2) await delay(2);
        const cancelled = await a.post({
            jsonrpc: "2.0",
            method: "notifications/cancelled",
            params: { requestId: 10, reason: "caller stopped" },
        });
        await cancelled.text();
        await delay(20);
        assert.deepEqual(sent, ["hold-first"]);
        ws.send(JSON.stringify({ id: first.id, success: true, data: "first-done" }));
        assert.equal(JSON.parse(await next), "next");
        assert.deepEqual(sent, ["hold-first", "next"]);
    });
});
