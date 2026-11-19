const assert = require("node:assert/strict");
const { test } = require("node:test");
const vm = require("node:vm");
const { bindWorkerConnection } = require("./worker-connection.cjs");

async function harness(websocketUrl = "wss://penpot.example/mcp/ws", pageUrl = "https://penpot.example/plugins/mcp/") {
    let init;
    let argument;
    const clientUrl = await bindWorkerConnection(
        {
            addInitScript: async (fn, arg) => {
                init = fn;
                argument = arg;
            },
        },
        { mcpUrl: "https://penpot.example/mcp?userToken=fixture-token", websocketUrl, pluginSessionId: "worker-01" }
    );
    const sockets = [];
    const context = vm.createContext({
        URL,
        location: { href: pageUrl },
        WebSocket: class {
            constructor(address, protocols) {
                sockets.push({ address, protocols });
            }
        },
    });
    vm.runInContext(`(${init.toString()})(${JSON.stringify(argument)})`, context);
    return {
        clientUrl,
        open(address, protocols) {
            context.address = address;
            context.protocols = protocols;
            vm.runInContext("new WebSocket(address, protocols)", context);
            return sockets.at(-1);
        },
    };
}

for (const address of ["wss://penpot.example/mcp/ws", "https://penpot.example/mcp/ws", "/mcp/ws", "../../mcp/ws"]) {
    test(`routes the browser-equivalent endpoint ${address}`, async () => {
        const h = await harness();
        const socket = h.open(address, ["fixture-protocol"]);
        const url = new URL(socket.address);
        assert.equal(url.protocol, "wss:");
        assert.equal(url.searchParams.get("userToken"), "fixture-token");
        assert.equal(url.searchParams.get("pluginSessionId"), "worker-01");
        assert.deepEqual(socket.protocols, ["fixture-protocol"]);
        assert.equal(new URL(h.clientUrl).searchParams.get("pluginSessionId"), "worker-01");
    });
}

for (const address of ["ws://penpot.example/mcp/ws", "http://penpot.example/mcp/ws"]) {
    test(`routes the non-TLS browser-equivalent endpoint ${address}`, async () => {
        const h = await harness("ws://penpot.example/mcp/ws", "http://penpot.example/plugins/mcp/");
        const url = new URL(h.open(address).address);
        assert.equal(url.protocol, "ws:");
        assert.equal(url.searchParams.get("pluginSessionId"), "worker-01");
    });
}

test("does not inject credentials or routing into another origin or path", async () => {
    const h = await harness();
    for (const address of [
        "https://other.example/mcp/ws",
        "https://penpot.example/other",
        "ws://penpot.example/mcp/ws",
    ]) {
        const url = new URL(h.open(address).address);
        assert.equal(url.searchParams.has("userToken"), false);
        assert.equal(url.searchParams.has("pluginSessionId"), false);
    }
});

test("rejects an owner mismatch even when the input uses HTTPS", async () => {
    const h = await harness();
    assert.throws(() => h.open("https://penpot.example/mcp/ws?userToken=other-owner"), /MCP owner mismatch/);
});
