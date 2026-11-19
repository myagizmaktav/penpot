/** Binds one owned browser context and its MCP client to the same plugin session. */
async function bindWorkerConnection(context, { mcpUrl, websocketUrl, pluginSessionId }) {
    if (!/^[a-zA-Z0-9_-]{1,128}$/.test(pluginSessionId)) {
        throw new Error("A valid, distinct pluginSessionId is required");
    }
    const clientUrl = new URL(mcpUrl);
    const socketUrl = new URL(websocketUrl);
    if (!["wss:", "ws:"].includes(socketUrl.protocol)) throw new Error("Invalid WebSocket URL");
    const token = clientUrl.searchParams.get("userToken");
    if (!token) throw new Error("The existing MCP userToken is required");
    clientUrl.searchParams.set("pluginSessionId", pluginSessionId);
    await context.addInitScript(
        ({ origin, pathname, token, pluginSessionId }) => {
            const OriginalWebSocket = globalThis.WebSocket;
            globalThis.WebSocket = class extends OriginalWebSocket {
                constructor(address, protocols) {
                    const url = new URL(address, location.href);
                    // Browsers accept HTTP(S) and relative WebSocket addresses and
                    // normalize them to WS(S). Compare the same endpoint they use.
                    if (url.protocol === "http:") url.protocol = "ws:";
                    if (url.protocol === "https:") url.protocol = "wss:";
                    if (url.origin === origin && url.pathname === pathname) {
                        const suppliedToken = url.searchParams.get("userToken");
                        if (suppliedToken && suppliedToken !== token) throw new Error("MCP owner mismatch");
                        url.searchParams.set("userToken", token);
                        url.searchParams.set("pluginSessionId", pluginSessionId);
                    }
                    if (protocols === undefined) super(url.href);
                    else super(url.href, protocols);
                }
            };
        },
        { origin: socketUrl.origin, pathname: socketUrl.pathname, token, pluginSessionId }
    );
    // Contains the caller's credential: pass directly to the MCP client, never log it.
    return clientUrl.href;
}

module.exports = { bindWorkerConnection };
