import { WebSocket, WebSocketServer } from "ws";
import * as http from "http";
import { AbstractPluginTask, PluginTask } from "./PluginTask";
import { RemotePluginTask } from "./RemotePluginTask";
import { PluginTaskRequest, PluginTaskResponse, PluginTaskResult } from "@penpot/mcp-common";
import { createLogger } from "./logger";
import type { PenpotMcpServer } from "./PenpotMcpServer";
import type { RedisBridge } from "./RedisBridge";
import { ExecuteCodePluginTask } from "./tasks/ExecuteCodePluginTask";

const KEEP_ALIVE_TIME = 30000; // 30 seconds

/**
 * Maximum plugin heartbeat age before a connection is stale.
 *
 * This uses plugin heartbeats rather than WebSocket pongs because the browser can answer
 * protocol pings while the tab's JavaScript event loop is frozen.
 */
export const HEARTBEAT_STALE_THRESHOLD_MS = 30000;

/**
 * Observable liveness state of a plugin connection.
 */
export interface PluginLivenessState {
    /** timestamp of the last plugin message, in ms since epoch. */
    lastHeartbeat: number;
    /** whether the plugin reported a browser freeze. */
    frozen: boolean;
}

interface ClientConnection extends PluginLivenessState {
    socket: WebSocket;
    userToken: string | null;
    pluginSessionId: string;
    pingInterval: NodeJS.Timeout;
    queue: string[];
    activeTaskId?: string;
    blocked: boolean;
    probeId?: string;
    probeTimeout?: NodeJS.Timeout;
}

/** Reads a bounded positive integer; invalid limits must not silently disable admission. */
function positiveLimit(name: string, fallback: number): number {
    const raw = process.env[name];
    if (raw === undefined) return fallback;
    if (!/^[1-9][0-9]*$/.test(raw) || !Number.isSafeInteger(Number(raw)) || Number(raw) > 10000) {
        throw new Error(`${name} must be an integer between 1 and 10000`);
    }
    return Number(raw);
}

/** Keeps the legacy Redis channel unchanged while separating named plugin sessions. */
function routingKey(userToken: string, pluginSessionId: string): string {
    return pluginSessionId ? JSON.stringify([userToken, pluginSessionId]) : userToken;
}

/**
 * Throws if the plugin tab cannot currently run tasks.
 *
 * A socket can stay open while the page event loop is paused, so task dispatch must check
 * plugin-level liveness before sending work.
 */
export function assertPluginResponsive(
    state: PluginLivenessState,
    now: number,
    staleThresholdMs: number = HEARTBEAT_STALE_THRESHOLD_MS
): void {
    if (state.frozen) {
        throw new Error(
            `The Penpot plugin tab has been frozen by the browser and cannot run tasks. ` +
                `Please click/focus the Penpot tab to wake it, then retry.`
        );
    }

    const heartbeatAge = now - state.lastHeartbeat;
    if (heartbeatAge > staleThresholdMs) {
        throw new Error(
            `The Penpot plugin tab appears to be suspended by the browser (no heartbeat for ` +
                `${Math.round(heartbeatAge / 1000)}s). Please click/focus the Penpot tab to wake it, ` +
                `then retry.`
        );
    }
}

/**
 * Manages WebSocket connections to Penpot plugin instances and handles plugin tasks
 * over these connections.
 */
export class PluginBridge {
    public static readonly MULTIUSER_CONNECTION_ERROR_MESSAGE = `No Penpot instance connected for user token. Please ensure that Penpot is connected and that the MCP client connection is using the correct token.`;

    private readonly logger = createLogger("PluginBridge");
    private readonly wsServer: WebSocketServer;

    private readonly connectedClients: Map<WebSocket, ClientConnection> = new Map();
    private readonly clientsByToken: Map<string, ClientConnection> = new Map();
    private readonly pendingTasks: Map<string, AbstractPluginTask<any, any>> = new Map();
    private readonly taskTimeouts: Map<string, NodeJS.Timeout> = new Map();
    private readonly taskConnections: Map<string, ClientConnection> = new Map();
    private readonly taskAbortCleanup: Map<string, () => void> = new Map();
    private readonly maxConnections = positiveLimit("PENPOT_MCP_MAX_PLUGIN_CONNECTIONS_PER_USER", 1);
    private readonly maxPending = positiveLimit("PENPOT_MCP_MAX_PENDING_TASKS_PER_PLUGIN", 30);
    private readonly queueTimeoutSecs = positiveLimit("PENPOT_MCP_QUEUE_TIMEOUT_S", 120);
    private readonly probeTimeoutSecs = positiveLimit("PENPOT_MCP_LIVENESS_PROBE_TIMEOUT_S", 5);
    private closing = false;

    /**
     * Creates the plugin bridge and starts its WebSocket server.
     *
     * @param mcpServer - The owning MCP server
     * @param port - The port on which to listen for plugin WebSocket connections
     * @param redisBridge - Optional Redis bridge enabling multi-instance task routing.
     *   When provided, tasks handled by this instance are routed to the instance
     *   holding the relevant plugin's WebSocket connection (which may be this same
     *   instance) via Redis, rather than dispatched directly over a local socket.
     * @param taskTimeoutSecs - Timeout, in seconds, for plugin task execution
     *   (defaults to {@link DEFAULT_TASK_TIMEOUT_SECS})
     */
    constructor(
        public readonly mcpServer: PenpotMcpServer,
        private port: number,
        private readonly taskTimeoutSecs: number,
        private readonly redisBridge?: RedisBridge
    ) {
        this.wsServer = new WebSocketServer({ port: port, host: mcpServer.host });
        this.setupWebSocketHandlers();
    }

    /**
     * Sets up WebSocket connection handlers for plugin communication.
     *
     * Manages client connections and provides bidirectional communication
     * channel between the MCP mcpServer and Penpot plugin instances.
     */
    private setupWebSocketHandlers(): void {
        this.wsServer.on("connection", (ws: WebSocket, request: http.IncomingMessage) => {
            // extract userToken from query parameters
            const url = new URL(request.url!, `ws://${request.headers.host}`);
            const userToken = url.searchParams.get("userToken");
            const pluginSessionId = url.searchParams.get("pluginSessionId") ?? "";
            if (pluginSessionId && !/^[a-zA-Z0-9_-]{1,128}$/.test(pluginSessionId)) {
                ws.close(1008, "Invalid pluginSessionId");
                return;
            }

            // require userToken if running in multi-user mode
            if (this.mcpServer.isMultiUserMode() && !userToken) {
                this.logger.warn("Connection attempt without userToken in multi-user mode - rejecting");
                ws.close(1008, "Missing userToken parameter");
                return;
            }

            // a reloaded or crashed editor can leave an idle registration behind until its socket times out;
            // release it so the editor's new connection is not refused, but never one that holds work
            const now = Date.now();
            let ownerConnections = [...this.connectedClients.values()].filter((c) => c.userToken === userToken);
            const conflicting =
                ownerConnections.find((c) => c.pluginSessionId === pluginSessionId) ??
                (ownerConnections.length >= this.maxConnections
                    ? ownerConnections.find((c) => this.isAbandoned(c, now))
                    : undefined);
            if (conflicting && this.isAbandoned(conflicting, now)) {
                this.logger.info("Releasing an abandoned idle plugin connection for a new connection");
                this.removeConnection(conflicting.socket);
                conflicting.socket.terminate();
                ownerConnections = ownerConnections.filter((c) => c !== conflicting);
            }
            if (
                ownerConnections.length >= this.maxConnections ||
                ownerConnections.some((c) => c.pluginSessionId === pluginSessionId)
            ) {
                ws.close(1008, "Plugin connection limit or duplicate pluginSessionId; existing connections retained");
                return;
            }

            if (userToken) {
                this.logger.info("New WebSocket connection established (token provided)");
            } else {
                this.logger.info("New WebSocket connection established");
            }

            // start the per-connection keep-alive ping interval
            const pingInterval = setInterval(() => {
                ws.ping();
            }, KEEP_ALIVE_TIME);

            // register the client connection with both indexes
            const connection: ClientConnection = {
                socket: ws,
                userToken,
                pluginSessionId,
                pingInterval,
                lastHeartbeat: Date.now(),
                frozen: false,
                queue: [],
                blocked: false,
            };
            this.connectedClients.set(ws, connection);
            if (userToken) {
                // ensure only one connection per userToken
                const key = routingKey(userToken, pluginSessionId);
                if (this.clientsByToken.has(key)) {
                    this.logger.warn("Duplicate connection for given user token; rejecting new connection");
                    this.removeConnection(ws);
                    ws.close(1008, "Duplicate connection for given user token; close previous connection first.");
                    return;
                }

                this.clientsByToken.set(key, connection);

                // In multi-instance mode, subscribe to this token's Redis request channel so
                // that task requests issued by other instances are dispatched to this plugin.
                if (this.redisBridge) {
                    const tokenForSubscription = key;
                    this.redisBridge
                        .subscribeToTasks(key, (request) => this.dispatchForwardedTask(tokenForSubscription, request))
                        .catch((error) => this.logger.error(error, "Failed to subscribe to Redis task channel"));
                }
            }

            ws.on("message", (data: Buffer) => {
                const text = data.toString();
                if (this.logger.isLevelEnabled("debug")) {
                    this.logger.debug("Received WebSocket message: %s", text);
                }
                try {
                    // any plugin message proves the page event loop is running
                    connection.lastHeartbeat = Date.now();

                    const message = JSON.parse(text);
                    if (message?.type === "freeze") {
                        connection.frozen = true;
                        this.logger.info("Plugin tab reported it is being frozen by the browser");
                        return;
                    }
                    connection.frozen = false;
                    if (message?.type === "heartbeat") {
                        return;
                    }
                    if (connection.probeId && message?.id === connection.probeId) {
                        clearTimeout(connection.probeTimeout);
                        connection.probeId = undefined;
                        connection.probeTimeout = undefined;
                        if (message.success === true) {
                            this.dispatchNext(connection);
                        } else {
                            this.removeConnection(ws);
                            ws.close(1011, "Plugin liveness probe failed");
                        }
                        return;
                    }
                    this.handlePluginTaskResponse(message as PluginTaskResponse<any>, connection);
                } catch (error) {
                    this.logger.error(error, "Failure while processing WebSocket message");
                }
            });

            ws.on("close", () => {
                this.logger.info("WebSocket connection closed");
                this.removeConnection(ws);
            });

            ws.on("error", (error) => {
                this.logger.error(error, "WebSocket connection error");
                this.removeConnection(ws);
            });
        });

        this.logger.info("WebSocket mcpServer started on port %d", this.port);
    }

    /**
     * Removes a client connection and releases all resources associated with it.
     *
     * Clears the per-connection keep-alive interval and removes the connection from the
     * socket-keyed index. The token-keyed index entry (and, in multi-instance mode, the
     * token's Redis task subscription) is removed only if it is owned by the given
     * connection. Safe to call with a socket that is not (or no longer) registered.
     *
     * @param ws - The WebSocket whose connection state should be removed
     */
    private removeConnection(ws: WebSocket): void {
        const connection = this.connectedClients.get(ws);
        if (!connection) {
            return;
        }
        clearInterval(connection.pingInterval);
        clearTimeout(connection.probeTimeout);
        connection.probeId = undefined;
        connection.blocked = true;
        this.connectedClients.delete(ws);
        for (const [id, target] of this.taskConnections) {
            if (target === connection) {
                this.rejectPendingTask(
                    id,
                    new Error(
                        id === connection.activeTaskId
                            ? "Plugin disconnected; task outcome may be unknown. Reconcile saved state before retrying."
                            : "Plugin disconnected; queued task was not dispatched."
                    )
                );
            }
        }
        if (connection.userToken) {
            const key = routingKey(connection.userToken, connection.pluginSessionId);
            // Perform the token-keyed cleanup only if this connection owns the token registration.
            // A connection rejected as a duplicate carries the same token but must not remove token associations.
            if (this.clientsByToken.get(key) !== connection) {
                this.logger.debug("Removed connection does not own its token registration; skipping token cleanup");
            } else {
                this.clientsByToken.delete(key);

                if (this.redisBridge) {
                    this.redisBridge
                        .unsubscribeFromTasks(key)
                        .catch((error) => this.logger.error(error, "Failed to unsubscribe from Redis task channel"));
                }
            }
        }
    }

    /**
     * Handles responses from the plugin for completed tasks.
     *
     * Finds the pending task by ID and resolves or rejects its promise
     * based on the execution result.
     *
     * @param response - The plugin task response containing ID and result
     */
    private handlePluginTaskResponse(response: PluginTaskResponse<any>, source?: ClientConnection): void {
        // A task ID alone does not confer permission to settle another plugin's work.
        if (source && (this.taskConnections.get(response.id) !== source || source.activeTaskId !== response.id)) {
            this.logger.warn("Ignoring response from a plugin that does not own the active task");
            return;
        }
        const task = this.pendingTasks.get(response.id);
        if (!task) {
            this.logger.info(`Received response for unknown task ID: ${response.id}`);
            return;
        }

        // Clear the timeout and remove the task from pending tasks
        const timeoutHandle = this.taskTimeouts.get(response.id);
        if (timeoutHandle) {
            clearTimeout(timeoutHandle);
            this.taskTimeouts.delete(response.id);
        }
        this.pendingTasks.delete(response.id);
        this.releaseConnectionTask(response.id);

        // Resolve or reject the task's promise based on the result
        if (response.success) {
            task.resolveWithResult({ data: response.data });
        } else {
            const error = new Error(response.error || "Task execution failed (details not provided)");
            task.rejectWithError(error);
        }

        this.logger.info(`Task ${response.id} completed: success=${response.success}`);
    }

    /**
     * Rejects a still-pending task with the given error, releasing its correlation state.
     *
     * Clears the task's timeout (if armed) and removes the task from the pending-task
     * index before rejecting its promise. Safe to call for a task that has already been
     * settled (e.g. by a response or a timeout), in which case nothing happens.
     *
     * @param taskId - The ID of the task to reject
     * @param error - The error with which to reject the task
     * @returns Whether the task was still pending and has been rejected
     */
    private rejectPendingTask(taskId: string, error: Error): boolean {
        const pendingTask = this.pendingTasks.get(taskId);
        if (!pendingTask) {
            return false;
        }

        const timeoutHandle = this.taskTimeouts.get(taskId);
        if (timeoutHandle) {
            clearTimeout(timeoutHandle);
            this.taskTimeouts.delete(taskId);
        }
        this.pendingTasks.delete(taskId);
        this.releaseConnectionTask(taskId);

        pendingTask.rejectWithError(error);
        this.logger.info(`Task ${taskId} rejected: ${error.message}`);
        return true;
    }

    /**
     * Determines the client connection to use for executing a task.
     *
     * In single-user mode, returns the single connected client.
     * In multi-user mode, returns the client matching the session's userToken.
     *
     * @returns The client connection to use
     * @throws Error if no suitable connection is found or if configuration is invalid
     */
    private getClientConnection(): ClientConnection {
        if (this.mcpServer.isMultiUserMode()) {
            const sessionContext = this.mcpServer.getSessionContext();
            if (!sessionContext?.userToken) {
                throw new Error("No userToken found in session context. Multi-user mode requires authentication.");
            }

            const connection = this.clientsByToken.get(
                routingKey(sessionContext.userToken, sessionContext.pluginSessionId ?? "")
            );
            if (!connection) {
                throw new Error(PluginBridge.MULTIUSER_CONNECTION_ERROR_MESSAGE);
            }

            return connection;
        } else {
            const selected = this.mcpServer.getSessionContext()?.pluginSessionId;
            if (selected) {
                const matches = [...this.connectedClients.values()].filter((c) => c.pluginSessionId === selected);
                if (matches.length !== 1) throw new Error("No unique Penpot plugin for pluginSessionId");
                return matches[0];
            }
            // single-user mode: return the single connected client
            if (this.connectedClients.size === 0) {
                throw new Error(
                    `No Penpot plugin instances are currently connected. Please ensure the plugin is running and connected.`
                );
            }
            if (this.connectedClients.size > 1) {
                throw new Error(
                    `Multiple (${this.connectedClients.size}) Penpot MCP Plugin instances are connected. ` +
                        `Ask the user to ensure that only one instance is connected at a time.`
                );
            }

            // return the first (and only) connection
            const connection = this.connectedClients.values().next().value;
            return <ClientConnection>connection;
        }
    }

    /**
     * Executes a plugin task by sending it to the connected Penpot plugin instance,
     * either directly via WebSocket or indirectly via Redis (depending on the configuration),
     * and awaiting the result.
     *
     * @param task - The plugin task to execute
     * @throws Error if no plugin instances are connected or available
     */
    public async executePluginTask<TResult extends PluginTaskResult<any>>(
        task: PluginTask<any, TResult>
    ): Promise<TResult> {
        this.sendPluginTask(task, this.redisBridge !== undefined);
        return await task.getResultPromise();
    }

    /**
     * Registers a task for response correlation, sends its request over the appropriate
     * transport, and arms a timeout that rejects the task if no response is received.
     *
     * The response (whether arriving over the local WebSocket or over Redis) is later
     * matched by ID in {@link handlePluginTaskResponse}, which settles the task via its
     * `resolveWithResult`/`rejectWithError` methods. The same correlation and timeout
     * handling therefore applies regardless of the transport.
     *
     * When routing via Redis, the task is rejected immediately (rather than timing out)
     * if the published request reached no instance, i.e. if no instance holds a plugin
     * connection for the session's user token, or if publishing fails outright.
     *
     * @param task - The task to dispatch
     * @param useRedis - Whether to route the request via Redis (multi-instance) rather
     *   than directly over the local WebSocket connection
     * @param connection - The connection to use for a local (non-remote) dispatch; when
     *   omitted, the session's connection is resolved via {@link getClientConnection}.
     *   Ignored when `useRedis` is true.
     * @throws Error if a local dispatch is required but no suitable connection is available
     */
    private sendPluginTask(task: AbstractPluginTask<any, any>, useRedis: boolean, connection?: ClientConnection): void {
        if (this.closing) throw new Error("Plugin bridge is shutting down; task was not dispatched");
        let onTimeout: (() => void) | undefined;

        if (useRedis) {
            const sessionContext = this.mcpServer.getSessionContext();
            if (!sessionContext?.userToken) {
                throw new Error("No userToken found in session context. Multi-user mode requires authentication.");
            }
            const userToken = routingKey(sessionContext.userToken, sessionContext.pluginSessionId ?? "");
            const redisBridge = this.redisBridge!;
            this.logger.debug("Dispatching task %s via Redis", task.id);

            // register the task for result correlation, then publish the request via Redis
            this.pendingTasks.set(task.id, task);
            void redisBridge
                .sendTaskRequest(userToken, task.toRequest(), (response) => this.handlePluginTaskResponse(response))
                .then((receiverCount) => {
                    // fail fast when no instance received the request (no connection with matching user token in any instance)
                    if (receiverCount === 0) {
                        this.rejectPendingTask(task.id, new Error(PluginBridge.MULTIUSER_CONNECTION_ERROR_MESSAGE));
                    }
                })
                .catch((error) => {
                    this.rejectPendingTask(task.id, error instanceof Error ? error : new Error(String(error)));
                });

            // on timeout, release the response-channel subscription, since no response
            // will arrive to trigger its self-unsubscribe.
            onTimeout = () => void redisBridge.unsubscribeFromResponse(task.id);
        } else {
            const target = connection ?? this.getClientConnection();
            const signal = this.mcpServer.getSessionContext()?.signal;
            if (signal?.aborted) throw new Error("MCP request cancelled; task was not dispatched");
            if (target.socket.readyState !== 1) {
                // WebSocket is not open
                throw new Error(`Plugin instance is disconnected. Task could not be sent.`);
            }

            // the socket can be open while browser-throttled plugin JS cannot run tasks
            // Explicit browser freeze is authoritative. Idle plugin timers may
            // be delayed; dispatchNext checks the JavaScript event loop first.
            if (target.frozen) assertPluginResponsive(target, Date.now());
            if (target.blocked) throw new Error("Plugin requires reconnection; task was not dispatched");
            if (target.queue.length + (target.activeTaskId ? 1 : 0) >= this.maxPending) {
                throw new Error("Plugin task queue is full; task was not dispatched");
            }
            this.pendingTasks.set(task.id, task);
            this.taskConnections.set(task.id, target);
            target.queue.push(task.id);
            this.taskTimeouts.set(
                task.id,
                setTimeout(() => {
                    this.rejectPendingTask(task.id, new Error("Plugin queue wait timed out; task was not dispatched"));
                }, this.queueTimeoutSecs * 1000)
            );
            if (signal) {
                const cancelQueued = () => {
                    // A caller cannot stop code already executing in the plugin.
                    // Keep that editor occupied until its response or timeout.
                    if (target.activeTaskId !== task.id) {
                        this.rejectPendingTask(
                            task.id,
                            new Error("MCP request cancelled; queued task was not dispatched")
                        );
                    }
                };
                this.taskAbortCleanup.set(task.id, () => signal.removeEventListener("abort", cancelQueued));
                signal.addEventListener("abort", cancelQueued, { once: true });
                if (signal.aborted) cancelQueued();
            }
            this.dispatchNext(target);
            return;
        }

        // Set up a timeout to reject the task if no response is received
        const timeoutHandle = setTimeout(() => {
            if (
                this.rejectPendingTask(
                    task.id,
                    new Error(`Task ${task.id} timed out after ${this.taskTimeoutSecs} seconds`)
                )
            ) {
                onTimeout?.();
            }
        }, this.taskTimeoutSecs * 1000);

        this.taskTimeouts.set(task.id, timeoutHandle);
        this.logger.info(`Sent task ${task.id}`);
    }

    /**
     * Whether a connection holds no work and its socket is closed or has been silent for longer than the heartbeat
     * threshold, so that releasing it cannot leave the outcome of any task unknown.
     */
    private isAbandoned(connection: ClientConnection, now: number): boolean {
        if (connection.activeTaskId || connection.queue.length > 0 || connection.probeId) {
            return false;
        }
        return (
            connection.socket.readyState !== WebSocket.OPEN ||
            now - connection.lastHeartbeat > HEARTBEAT_STALE_THRESHOLD_MS
        );
    }

    /** Releases a task's lane and schedules the next task only while the connection is healthy. */
    private releaseConnectionTask(id: string): void {
        this.taskAbortCleanup.get(id)?.();
        this.taskAbortCleanup.delete(id);
        const target = this.taskConnections.get(id);
        if (!target) return;
        this.taskConnections.delete(id);
        target.queue = target.queue.filter((queued) => queued !== id);
        if (target.activeTaskId === id) target.activeTaskId = undefined;
        queueMicrotask(() => this.dispatchNext(target));
    }

    /** Runs one task at a time on an editor; different editors have independent lanes. */
    private dispatchNext(target: ClientConnection): void {
        if (this.closing || target.blocked || target.activeTaskId || target.probeId || !target.queue.length) return;
        const id = target.queue[0];
        const task = this.pendingTasks.get(id);
        if (!task) {
            // a settled task must not block the lane
            target.queue.shift();
            this.dispatchNext(target);
            return;
        }
        try {
            if (target.socket.readyState !== WebSocket.OPEN) throw new Error("Plugin disconnected");
            if (target.frozen) assertPluginResponsive(target, Date.now());
        } catch (error) {
            target.blocked = true;
            this.removeConnection(target.socket);
            target.socket.close(1011, "Plugin is not responsive");
            return;
        }
        if (Date.now() - target.lastHeartbeat > HEARTBEAT_STALE_THRESHOLD_MS) {
            // Check the plugin's JavaScript event loop with a read-only no-op.
            // A protocol pong alone does not prove that browser JavaScript runs.
            // Keep all requested operations queued until this exact socket replies.
            const probe = new ExecuteCodePluginTask({ code: "return null;" });
            target.probeId = probe.id;
            target.probeTimeout = setTimeout(() => {
                this.removeConnection(target.socket);
                target.socket.close(1011, "Plugin liveness probe timed out");
            }, this.probeTimeoutSecs * 1000);
            target.socket.send(JSON.stringify(probe.toRequest()), (error) => {
                if (error) {
                    this.removeConnection(target.socket);
                    target.socket.terminate();
                }
            });
            return;
        }
        target.queue.shift();
        target.activeTaskId = id;
        clearTimeout(this.taskTimeouts.get(id));
        this.taskTimeouts.set(
            id,
            setTimeout(() => {
                target.blocked = true;
                this.rejectPendingTask(
                    id,
                    new Error(
                        "Plugin execution timed out; task outcome may be unknown. Reconcile saved state before retrying."
                    )
                );
                this.removeConnection(target.socket);
                target.socket.close(1011, "Task timed out; reconnect required");
            }, this.taskTimeoutSecs * 1000)
        );
        target.socket.send(JSON.stringify(task.toRequest()), (error) => {
            if (error) {
                this.removeConnection(target.socket);
                target.socket.terminate();
            }
        });
    }

    /**
     * Dispatches a task request received over Redis to the locally-connected plugin.
     *
     * Invoked on the instance subscribed to a user token's request channel when another
     * instance (or this one) issues a task request. A {@link RemotePluginTask} is created
     * so that, once the plugin responds, the outcome is published back to the issuing
     * instance's Redis response channel via the standard response-handling path.
     *
     * On failure to dispatch (e.g. the plugin is not connected here), an error response
     * is published immediately so the requester need not wait for its timeout.
     *
     * @param userToken - The user token on whose request channel the request arrived;
     *   identifies the locally-connected plugin to dispatch to
     * @param request - The serialized task request, passed through from Redis
     */
    private dispatchForwardedTask(userToken: string, request: PluginTaskRequest): void {
        if (!this.redisBridge) {
            return;
        }

        // The response is published on the channel keyed by the original request ID.
        const task = new RemotePluginTask(request.task, request.params, this.redisBridge, request.id);
        this.logger.debug("Dispatching remote task %s as %s to Penpot via WebSocket", request.id, task.id);

        const connection = this.clientsByToken.get(userToken);
        if (!connection) {
            task.rejectWithError(new Error("Plugin not connected on the receiving instance"));
            return;
        }

        try {
            this.sendPluginTask(task, false, connection);
        } catch (error) {
            task.rejectWithError(error instanceof Error ? error : new Error(String(error)));
        }
    }

    /**
     * Closes the WebSocket server and all connected client sockets.
     */
    public async close(): Promise<void> {
        this.closing = true;
        for (const id of this.pendingTasks.keys()) {
            this.rejectPendingTask(id, new Error("Plugin bridge is shutting down; task outcome may be unknown"));
        }
        for (const socket of this.connectedClients.keys()) socket.terminate();
        return new Promise((resolve) => {
            this.wsServer.close(() => {
                this.logger.info("WebSocket server closed");
                resolve();
            });
        });
    }
}
