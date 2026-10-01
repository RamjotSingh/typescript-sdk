/**
 * The entry-handled `subscriptions/listen` router for the HTTP serving entry.
 *
 * `createMcpHandler` recognizes a modern-classified `subscriptions/listen`
 * request and routes it here: the entry owns ack-first, per-stream filtering,
 * subscription-id stamping, keepalive, capacity guarding, and teardown. The
 * consumer's factory IS constructed for listen, to read the instance's
 * declared `ServerCapabilities` only — the probe instance is never connected
 * and is closed immediately after the capabilities read. Token verification
 * and any per-request authorization still belong at the middleware layer
 * mounted in front of `createMcpHandler` (the entry's documented authz
 * posture).
 *
 * Per the spec at protocol revision 2026-07-28:
 * - The acknowledged notification is the FIRST message on the stream and
 *   carries the honored subset of the requested filter.
 * - Every notification on the stream (including the ack) carries the listen
 *   request's JSON-RPC id under `_meta['io.modelcontextprotocol/subscriptionId']`.
 * - The server MUST NOT deliver a notification type the client did not request.
 * - Server-side graceful close (`closeAll()`) emits the empty
 *   `subscriptions/listen` JSON-RPC result (the `SubscriptionsListenResult` —
 *   `_meta` carries the subscription id) before closing the stream; an abrupt
 *   transport close carries no response and the client treats it as a
 *   disconnect.
 */
import type {
    AuthInfo,
    ClientCapabilities,
    Implementation,
    JSONRPCRequest,
    RequestId,
    ServerCapabilities,
    SubscriptionFilter
} from '@modelcontextprotocol/core-internal';
import {
    CLIENT_CAPABILITIES_META_KEY,
    codecForVersion,
    MODERN_WIRE_REVISION,
    SERVER_INFO_META_KEY,
    SUBSCRIPTION_ID_META_KEY
} from '@modelcontextprotocol/core-internal';

import type { ServerEvent, ServerEventBus } from './serverEventBus';
import { honoredSubset, listenFilterAccepts, serverEventToNotification } from './serverEventBus';
import { armSseKeepAlive, DEFAULT_SSE_KEEP_ALIVE_MS } from './sseKeepAlive';

/** Default capacity guard: refuse a new subscription when this many are already open. */
export const DEFAULT_MAX_SUBSCRIPTIONS = 1024;

/** Options for {@linkcode createListenRouter}. */
export interface ListenRouterOptions {
    /** The event bus listen streams subscribe to. */
    bus: ServerEventBus;
    /** Reject a new listen with `-32603` when this many subscriptions are already open (default 1024). */
    maxSubscriptions?: number;
    /** SSE comment-frame keepalive interval; `0` disables keepalive (default 15000). */
    keepAliveMs?: number;
    /** Out-of-band error reporting (never alters the response). */
    onerror?: (error: Error) => void;
    /** Prototype draft vocabulary: the Authorization Lifetime SEP. */
    subscriptionLifetime?: SubscriptionLifetimeOptions;
}

/** The information a notification reveals for an authorization decision. */
export type AccessTarget = { kind: 'resource'; uri: string } | { kind: 'toolsList' } | { kind: 'promptsList' } | { kind: 'resourcesList' };
/** A per-notification authorization decision. */
export type AccessDecision = 'allow' | 'deny' | 'unavailable';
/** Server-side options for the prototype Authorization Lifetime SEP. */
export interface SubscriptionLifetimeOptions {
    /** Lifetime SEP §3 rule 2 per-notification access check. Defaults to allow. */
    authorize?: (authInfo: AuthInfo | undefined, target: AccessTarget) => AccessDecision | Promise<AccessDecision>;
    /** Lifetime SEP §3 rule 6 policy cap on stream authorization lifetime. */
    maxAuthorizationLifetimeMs?: number;
    /** Test seam for the current epoch time in milliseconds. */
    now?: () => number;
}
/** Public stream state exposed by {@linkcode SubscriptionControl.list}. */
export interface StreamInfo {
    subscriptionId: RequestId;
    clientId?: string;
    subject?: string;
    state: 'running';
    acknowledged: SubscriptionFilter;
    authorizedUntil?: string;
}
/** Prototype server control API for subscription lifetime signals. */
export interface SubscriptionControl {
    revoke(match?: (s: StreamInfo) => boolean): number;
    recheckAccess(match?: (s: StreamInfo) => boolean): Promise<void>;
    requireScopes(scopes: string[], effectiveAtMs: number, match?: (s: StreamInfo) => boolean): void;
    list(): StreamInfo[];
}

/**
 * A wire-shape notification body (method + loose params).
 * @internal
 */
export interface NotificationBody {
    method: string;
    params: { _meta?: Record<string, unknown>; [key: string]: unknown };
}

function jsonRpcError(id: RequestId | null, code: number, message: string, data?: unknown, status = 200): Response {
    return Response.json({ jsonrpc: '2.0', error: { code, message, ...(data !== undefined && { data }) }, id }, { status });
}

/** Stamp the subscription id onto a notification's `_meta`. Non-mutating. */
function stampSubscriptionId(
    notification: { method: string; params?: { _meta?: Record<string, unknown>; [key: string]: unknown } },
    subscriptionId: RequestId
): NotificationBody {
    return {
        method: notification.method,
        params: {
            ...notification.params,
            _meta: { ...notification.params?._meta, [SUBSCRIPTION_ID_META_KEY]: subscriptionId }
        }
    };
}

/**
 * Read the requested filter off a `subscriptions/listen` request body.
 * Returns the validated filter, or `undefined` when `params.notifications`
 * is absent or fails the schema (the caller answers `-32602` — the spec
 * marks `notifications` REQUIRED on the listen request).
 */
export function parseListenFilter(message: JSONRPCRequest): SubscriptionFilter | undefined {
    // `subscriptions/listen` is 2026-only vocabulary; route through the era
    // codec's request validator (the wire layer owns the filter schema).
    const outcome = codecForVersion(MODERN_WIRE_REVISION).validateRequest('subscriptions/listen', message);
    return outcome.ok ? outcome.value.params?.notifications : undefined;
}

/**
 * The HTTP listen router: holds the set of open subscriptions and serves
 * each listen request as an SSE response.
 */
export interface ListenRouter {
    /**
     * Serve one `subscriptions/listen` request and return the SSE `Response`
     * (or, on capacity / params / authorization rejection, the in-band JSON-RPC error
     * `Response`). The ack notification is the first SSE frame.
     *
     * `capabilities` is required: the acknowledged filter is always narrowed
     * against what the serving instance advertises (honoring a filter without
     * capabilities would fail open and deliver unadvertised types).
     * `serverInfo` is the serving instance's identity, stamped onto the
     * graceful-close result's `_meta` (the spec's `SubscriptionsListenResultMetaObject`
     * extends `ResultMetaObject`, so the serverInfo SHOULD applies there too).
     */
    serve(
        message: JSONRPCRequest,
        signal: AbortSignal | undefined,
        capabilities: ServerCapabilities,
        serverInfo: Implementation,
        authInfo: AuthInfo | undefined
    ): Promise<Response>;
    /**
     * Gracefully close every open subscription stream: emits the empty
     * `subscriptions/listen` JSON-RPC result (the spec's graceful-close
     * signal) as the final SSE frame, then closes the stream.
     */
    closeAll(): void;
    /** Prototype control surface for external authorization signals. */
    readonly subscriptions: SubscriptionControl;
    /** The number of currently open subscription streams (for tests / introspection). */
    readonly openCount: number;
}

function iso(ms: number): string {
    return new Date(ms).toISOString();
}
function subjectOf(authInfo: AuthInfo | undefined): string | undefined {
    const sub = authInfo?.extra?.['sub'];
    return typeof sub === 'string' ? sub : undefined;
}
function hasEntries(filter: SubscriptionFilter): boolean {
    return (
        filter.toolsListChanged === true ||
        filter.promptsListChanged === true ||
        filter.resourcesListChanged === true ||
        (filter.resourceSubscriptions?.length ?? 0) > 0
    );
}
function targetForEvent(event: ServerEvent): AccessTarget {
    switch (event.kind) {
        case 'tools_list_changed': {
            return { kind: 'toolsList' };
        }
        case 'prompts_list_changed': {
            return { kind: 'promptsList' };
        }
        case 'resources_list_changed': {
            return { kind: 'resourcesList' };
        }
        case 'resource_updated': {
            return { kind: 'resource', uri: event.uri };
        }
    }
}
function filterForTarget(target: AccessTarget): SubscriptionFilter {
    switch (target.kind) {
        case 'toolsList': {
            return { toolsListChanged: true };
        }
        case 'promptsList': {
            return { promptsListChanged: true };
        }
        case 'resourcesList': {
            return { resourcesListChanged: true };
        }
        case 'resource': {
            return { resourceSubscriptions: [target.uri] };
        }
    }
}
/** The filter that lists exactly `targets`. */
function filterOf(targets: readonly AccessTarget[]): SubscriptionFilter {
    const uris = targets.flatMap(target => (target.kind === 'resource' ? [target.uri] : []));
    return {
        ...(targets.some(target => target.kind === 'toolsList') && { toolsListChanged: true }),
        ...(targets.some(target => target.kind === 'promptsList') && { promptsListChanged: true }),
        ...(targets.some(target => target.kind === 'resourcesList') && { resourcesListChanged: true }),
        ...(uris.length > 0 && { resourceSubscriptions: uris })
    };
}
function eventForTarget(target: AccessTarget): ServerEvent {
    switch (target.kind) {
        case 'toolsList': {
            return { kind: 'tools_list_changed' };
        }
        case 'promptsList': {
            return { kind: 'prompts_list_changed' };
        }
        case 'resourcesList': {
            return { kind: 'resources_list_changed' };
        }
        case 'resource': {
            return { kind: 'resource_updated', uri: target.uri };
        }
    }
}
function targetsOf(filter: SubscriptionFilter): AccessTarget[] {
    const targets: AccessTarget[] = [];
    if (filter.toolsListChanged === true) targets.push({ kind: 'toolsList' });
    if (filter.promptsListChanged === true) targets.push({ kind: 'promptsList' });
    if (filter.resourcesListChanged === true) targets.push({ kind: 'resourcesList' });
    for (const uri of filter.resourceSubscriptions ?? []) targets.push({ kind: 'resource', uri });
    return targets;
}
function mergeFilters(a: SubscriptionFilter, b: SubscriptionFilter): SubscriptionFilter {
    return {
        toolsListChanged: a.toolsListChanged || b.toolsListChanged || undefined,
        promptsListChanged: a.promptsListChanged || b.promptsListChanged || undefined,
        resourcesListChanged: a.resourcesListChanged || b.resourcesListChanged || undefined,
        resourceSubscriptions: [...new Set([...(a.resourceSubscriptions ?? []), ...(b.resourceSubscriptions ?? [])])]
    };
}
function removeTarget(filter: SubscriptionFilter, target: AccessTarget): SubscriptionFilter {
    const next: SubscriptionFilter = {
        ...filter,
        ...(filter.resourceSubscriptions !== undefined && { resourceSubscriptions: [...filter.resourceSubscriptions] })
    };
    switch (target.kind) {
        case 'toolsList': {
            delete next.toolsListChanged;
            break;
        }
        case 'promptsList': {
            delete next.promptsListChanged;
            break;
        }
        case 'resourcesList': {
            delete next.resourcesListChanged;
            break;
        }
        default: {
            next.resourceSubscriptions = next.resourceSubscriptions?.filter(uri => uri !== target.uri);
            if (next.resourceSubscriptions?.length === 0) delete next.resourceSubscriptions;
        }
    }
    return next;
}

const MAX_TIMER_DELAY_MS = 2_147_483_647;

interface Requirement {
    scopes: string[];
    effectiveAtMs: number;
}

const requirementMet = (authInfo: AuthInfo | undefined, req?: Requirement): boolean =>
    req === undefined || req.scopes.every(scope => authInfo?.scopes.includes(scope));

interface StreamState {
    id: RequestId;
    authInfo?: AuthInfo;
    subject?: string;
    clientId?: string;
    draft: boolean;
    acknowledged: SubscriptionFilter;
    authorizationDeadline?: number;
    deadlineReason: 'token_expiry' | 'insufficient_authorization';
    authorizedAtMs: number;
    state: 'running' | 'ended';
    requirement?: Requirement;
    pendingRemoved: SubscriptionFilter;
    controller: ReadableStreamDefaultController<Uint8Array>;
    unsubscribe?: () => void;
    keepAliveTimer?: ReturnType<typeof setInterval>;
    timers: ReturnType<typeof setTimeout>[];
    abortCleanup?: () => void;
    writeFrame(frame: string): void;
    close(graceful: boolean): void;
}

export function createListenRouter(options: ListenRouterOptions): ListenRouter {
    const { bus, onerror } = options;
    const lifetime = options.subscriptionLifetime;
    const maxSubscriptions = options.maxSubscriptions ?? DEFAULT_MAX_SUBSCRIPTIONS;
    const keepAliveMs = options.keepAliveMs ?? DEFAULT_SSE_KEEP_ALIVE_MS;
    const now = () => lifetime?.now?.() ?? Date.now();
    const open = new Set<StreamState>();

    const report = (error: unknown): void => onerror?.(error instanceof Error ? error : new Error(String(error)));

    const authorize = async (authInfo: AuthInfo | undefined, target: AccessTarget): Promise<AccessDecision> => {
        try {
            return await (lifetime?.authorize?.(authInfo, target) ?? 'allow');
        } catch (error) {
            report(error);
            return 'unavailable';
        }
    };

    const infoOf = (s: StreamState): StreamInfo => ({
        subscriptionId: s.id,
        ...(s.clientId !== undefined && { clientId: s.clientId }),
        ...(s.subject !== undefined && { subject: s.subject }),
        state: 'running',
        acknowledged: {
            ...s.acknowledged,
            ...(s.acknowledged.resourceSubscriptions !== undefined && { resourceSubscriptions: [...s.acknowledged.resourceSubscriptions] })
        },
        ...(s.authorizationDeadline !== undefined && { authorizedUntil: iso(s.authorizationDeadline) })
    });

    const writeJson = (s: StreamState, message: unknown): void => s.writeFrame(`event: message\ndata: ${JSON.stringify(message)}\n\n`);
    const writeNotification = (s: StreamState, method: string, params: Record<string, unknown>): void =>
        writeJson(s, { jsonrpc: '2.0', method, params });

    const recomputeDeadline = (s: StreamState): void => {
        const candidates: { at: number; reason: 'token_expiry' | 'insufficient_authorization' }[] = [];
        if (s.authInfo?.expiresAt !== undefined) candidates.push({ at: s.authInfo.expiresAt * 1000, reason: 'token_expiry' });
        if (lifetime?.maxAuthorizationLifetimeMs !== undefined) {
            candidates.push({ at: s.authorizedAtMs + lifetime.maxAuthorizationLifetimeMs, reason: 'token_expiry' });
        }
        if (s.requirement !== undefined && !requirementMet(s.authInfo, s.requirement)) {
            candidates.push({ at: s.requirement.effectiveAtMs, reason: 'insufficient_authorization' });
        }
        const [first] = candidates.toSorted((a, b) => a.at - b.at);
        s.authorizationDeadline = first?.at;
        s.deadlineReason = first?.reason ?? 'token_expiry';
    };

    const clearTimers = (s: StreamState): void => {
        for (const timer of s.timers) clearTimeout(timer);
        s.timers = [];
    };

    const armAt = (s: StreamState, targetMs: number, callback: () => void): void => {
        const arm = (): void => {
            if (s.state === 'ended') return;
            const delay = targetMs - now();
            if (delay > MAX_TIMER_DELAY_MS) {
                s.timers.push(setTimeout(arm, MAX_TIMER_DELAY_MS));
                return;
            }
            s.timers.push(
                setTimeout(
                    () => {
                        if (s.state === 'ended') return;
                        if (now() >= targetMs) callback();
                        else arm();
                    },
                    Math.max(0, delay)
                )
            );
        };
        arm();
    };

    function endAuthorization(s: StreamState, reason: 'token_expiry' | 'insufficient_authorization' | 'revoked'): void {
        if (s.state === 'ended') return;
        if (s.draft) writeJson(s, { jsonrpc: '2.0', id: s.id, error: { code: -32_028, message: 'Authorization ended', data: { reason } } });
        s.close(false);
    }

    const flushRemoved = (s: StreamState): void => {
        if (!hasEntries(s.pendingRemoved)) return;
        s.pendingRemoved = {};
        if (!hasEntries(s.acknowledged)) endAuthorization(s, 'revoked');
    };

    const onDeadline = (s: StreamState): void => {
        if (s.state === 'ended') return;
        if (!hasEntries(s.acknowledged)) {
            endAuthorization(s, 'revoked');
            return;
        }
        endAuthorization(s, s.deadlineReason);
    };

    function schedule(s: StreamState): void {
        clearTimers(s);
        if (s.state === 'ended') return;
        if (s.authorizationDeadline === undefined) return;
        armAt(s, s.authorizationDeadline, () => onDeadline(s));
    }

    const removeEntry = (s: StreamState, target: AccessTarget): void => {
        s.acknowledged = removeTarget(s.acknowledged, target);
        s.pendingRemoved = mergeFilters(s.pendingRemoved, filterForTarget(target));
    };

    const canWriteData = (s: StreamState, event: ServerEvent): boolean => {
        return (
            s.state === 'running' &&
            listenFilterAccepts(s.acknowledged, event) &&
            (s.authorizationDeadline === undefined || now() < s.authorizationDeadline) &&
            !(s.requirement !== undefined && now() >= s.requirement.effectiveAtMs && !requirementMet(s.authInfo, s.requirement))
        );
    };

    const blockData = (s: StreamState, event: ServerEvent): void => {
        if (s.state === 'ended' || !listenFilterAccepts(s.acknowledged, event)) return;
        if (s.authorizationDeadline !== undefined && now() >= s.authorizationDeadline) {
            endAuthorization(s, hasEntries(s.acknowledged) ? s.deadlineReason : 'revoked');
        }
    };

    const deliver = async (s: StreamState, event: ServerEvent): Promise<void> => {
        if (!canWriteData(s, event)) {
            blockData(s, event);
            return;
        }
        const target = targetForEvent(event);
        const decision = await authorize(s.authInfo, target);
        if (s.state === 'ended' || !listenFilterAccepts(s.acknowledged, event)) return;
        if (decision === 'deny') {
            removeEntry(s, target);
            return;
        }
        if (decision === 'unavailable') return;
        if (!canWriteData(s, event)) {
            blockData(s, event);
            return;
        }
        const note = stampSubscriptionId(serverEventToNotification(event), s.id);
        writeNotification(s, note.method, note.params);
    };

    async function decisionsFor(filter: SubscriptionFilter, authInfo: AuthInfo | undefined): Promise<Map<AccessTarget, AccessDecision>> {
        const decisions = new Map<AccessTarget, AccessDecision>();
        for (const target of targetsOf(filter)) decisions.set(target, await authorize(authInfo, target));
        return decisions;
    }

    function applyDeniedDecisions(s: StreamState, decisions: Map<AccessTarget, AccessDecision>): void {
        for (const [target, decision] of decisions) {
            if (decision === 'deny' && listenFilterAccepts(s.acknowledged, eventForTarget(target))) removeEntry(s, target);
        }
    }

    async function applyFilter(s: StreamState, authInfo: AuthInfo | undefined, announce: boolean): Promise<void> {
        const decisions = await decisionsFor(s.acknowledged, authInfo);
        if (s.state === 'ended') return;
        applyDeniedDecisions(s, decisions);
        if (announce) flushRemoved(s);
    }

    async function serve(
        message: JSONRPCRequest,
        signal: AbortSignal | undefined,
        capabilities: ServerCapabilities,
        serverInfo: Implementation,
        authInfo: AuthInfo | undefined
    ): Promise<Response> {
        if (open.size >= maxSubscriptions) return jsonRpcError(message.id, -32_603, 'Subscription limit reached');
        const filter = parseListenFilter(message);
        if (filter === undefined)
            return jsonRpcError(message.id, -32_602, "Invalid params: 'notifications' is required and must be a valid SubscriptionFilter");
        const params = message.params as { _meta?: Record<string, unknown> } | undefined;
        const honored = honoredSubset(filter, capabilities);
        if (lifetime?.authorize !== undefined) {
            const decisions = await decisionsFor(honored, authInfo);
            const denied = [...decisions].filter(([, decision]) => decision === 'deny').map(([target]) => target);
            // Lifetime SEP §3 rule 5: a stream opens with all of its filter or not at all, and the refusal names what is not permitted.
            if (denied.length > 0) return jsonRpcError(message.id, -32_602, 'Not permitted', { denied: filterOf(denied) });
        }
        const id = message.id;
        let state!: StreamState;
        let controller!: ReadableStreamDefaultController<Uint8Array>;
        const encoder = new TextEncoder();
        const caps = params?._meta?.[CLIENT_CAPABILITIES_META_KEY] as ClientCapabilities | undefined;
        const draft = typeof caps?.experimental?.['io.modelcontextprotocol/subscription-lifetime'] === 'object';

        const readable = new ReadableStream<Uint8Array>({
            start(c) {
                controller = c;
                state = {
                    id,
                    authInfo,
                    subject: subjectOf(authInfo),
                    clientId: authInfo?.clientId,
                    draft,
                    acknowledged: honored,
                    deadlineReason: 'token_expiry',
                    authorizedAtMs: now(),
                    state: 'running',
                    pendingRemoved: {},
                    controller,
                    timers: [],
                    writeFrame(frame) {
                        if (this.state !== 'ended') {
                            try {
                                controller.enqueue(encoder.encode(frame));
                            } catch (error) {
                                report(error);
                            }
                        }
                    },
                    close(graceful) {
                        if (this.state === 'ended') return;
                        if (graceful) {
                            writeJson(this, {
                                jsonrpc: '2.0',
                                id: this.id,
                                result: {
                                    resultType: 'complete',
                                    _meta: { [SUBSCRIPTION_ID_META_KEY]: this.id, [SERVER_INFO_META_KEY]: serverInfo }
                                }
                            });
                        }
                        this.state = 'ended';
                        try {
                            this.unsubscribe?.();
                        } catch (error) {
                            report(error);
                        }
                        if (this.keepAliveTimer !== undefined) clearInterval(this.keepAliveTimer);
                        clearTimers(this);
                        this.abortCleanup?.();
                        open.delete(this);
                        try {
                            controller.close();
                        } catch {
                            // Already closed/cancelled by the consumer.
                        }
                    }
                };
                recomputeDeadline(state);
                const ackParams: Record<string, unknown> = { notifications: honored };
                if (state.authorizationDeadline !== undefined) ackParams['authorizedUntil'] = iso(state.authorizationDeadline);
                const ack = stampSubscriptionId({ method: 'notifications/subscriptions/acknowledged', params: ackParams }, id);
                writeNotification(state, ack.method, ack.params);
                if (!hasEntries(honored)) {
                    state.close(true);
                    return;
                }
                state.unsubscribe = bus.subscribe(event => {
                    void deliver(state, event).catch(report);
                });
                state.keepAliveTimer = armSseKeepAlive(keepAliveMs, () => state.writeFrame(': keepalive\n\n'));
                open.add(state);
                schedule(state);
            },
            cancel() {
                state?.close(false);
            }
        });
        if (signal !== undefined) {
            if (signal.aborted) state?.close(false);
            else {
                const onAbort = () => state?.close(false);
                signal.addEventListener('abort', onAbort, { once: true });
                queueMicrotask(() => {
                    if (state !== undefined) state.abortCleanup = () => signal.removeEventListener('abort', onAbort);
                });
            }
        }
        return new Response(readable, {
            status: 200,
            headers: {
                'Content-Type': 'text/event-stream',
                'Cache-Control': 'no-cache, no-transform',
                Connection: 'keep-alive',
                'X-Accel-Buffering': 'no'
            }
        });
    }

    const control: SubscriptionControl = {
        revoke(match) {
            let count = 0;
            for (const s of open) {
                if (match?.(infoOf(s)) ?? true) {
                    count++;
                    endAuthorization(s, 'revoked');
                }
            }
            return count;
        },
        async recheckAccess(match) {
            for (const s of open) if (match?.(infoOf(s)) ?? true) await applyFilter(s, s.authInfo, true);
        },
        requireScopes(scopes, effectiveAtMs, match) {
            for (const s of open) {
                if (!(match?.(infoOf(s)) ?? true)) continue;
                const old = s.authorizationDeadline;
                s.requirement = { scopes: [...scopes], effectiveAtMs };
                recomputeDeadline(s);
                if (s.authorizationDeadline !== old) {
                    schedule(s);
                }
            }
        },
        list: () => [...open].map(s => infoOf(s))
    };
    return {
        serve,
        closeAll() {
            for (const s of open) s.close(true);
        },
        subscriptions: control,
        get openCount() {
            return open.size;
        }
    };
}

/* ------------------------------------------------------------------------ *
 * Stdio listen router
 * ------------------------------------------------------------------------ */

/** A graceful-close `subscriptions/listen` result frame emitted by {@linkcode StdioListenRouter.teardownAll}. */
export interface ListenCloseFrame {
    jsonrpc: '2.0';
    id: RequestId;
    result: {
        resultType: 'complete';
        _meta: { [SUBSCRIPTION_ID_META_KEY]: RequestId; [SERVER_INFO_META_KEY]?: Implementation };
    };
}

const CHANGE_NOTIFICATION_METHODS: ReadonlySet<string> = new Set([
    'notifications/tools/list_changed',
    'notifications/prompts/list_changed',
    'notifications/resources/list_changed',
    'notifications/resources/updated'
]);

/**
 * Per-connection listen state for the stdio entry. One instance is held by
 * `serveStdio` for the connection lifetime; it routes inbound
 * `subscriptions/listen` / `notifications/cancelled` and rewrites outbound
 * change notifications onto the active subscriptions. No bus — the long-lived
 * pinned instance's existing `send*ListChanged()` calls feed straight into
 * `routeOutbound()`.
 */
export class StdioListenRouter {
    /** Active subscriptions, keyed by the listen request's JSON-RPC id verbatim. */
    private readonly _subs = new Map<RequestId, SubscriptionFilter>();
    /**
     * The serving instance's declared capabilities. Filled in by the entry
     * once the modern instance is constructed (the router is created before
     * the instance exists), so the acknowledged filter is narrowed against
     * what the server can actually deliver.
     */
    private _serverCapabilities: ServerCapabilities | undefined;
    /**
     * The serving instance's identity, stamped onto the graceful-close
     * results' `_meta` (the spec's `SubscriptionsListenResultMetaObject` extends
     * `ResultMetaObject`). Handed over together with the capabilities.
     */
    private _serverInfo: Implementation | undefined;

    constructor(
        private readonly _maxSubscriptions: number = DEFAULT_MAX_SUBSCRIPTIONS,
        serverCapabilities?: ServerCapabilities,
        serverInfo?: Implementation
    ) {
        this._serverCapabilities = serverCapabilities;
        this._serverInfo = serverInfo;
    }

    /**
     * Record the serving instance's declared capabilities and identity once
     * it has been constructed. Called by `serveStdio`'s connect path;
     * subsequent `serve()` calls narrow the honored filter against the
     * capabilities, and `teardownAll()` stamps the identity.
     */
    setServerCapabilities(capabilities: ServerCapabilities, serverInfo?: Implementation): void {
        this._serverCapabilities = capabilities;
        if (serverInfo !== undefined) this._serverInfo = serverInfo;
    }

    /** Whether `id` is an active listen subscription on this connection. */
    has(id: RequestId): boolean {
        return this._subs.has(id);
    }

    /**
     * Serve one inbound `subscriptions/listen` request: registers the
     * subscription and returns the stamped acknowledged notification (or, on
     * capacity / params rejection, the in-band JSON-RPC error response).
     *
     * @throws when called before {@linkcode setServerCapabilities} (or the
     * constructor) has supplied the serving instance's capabilities. Honoring a
     * filter without knowing the server's advertised capabilities would fail
     * open (deliver unadvertised types); the entry guarantees capabilities are
     * set before any listen request is routed here.
     */
    serve(message: JSONRPCRequest): NotificationBody | { jsonrpc: '2.0'; id: RequestId; error: { code: number; message: string } } {
        if (this._serverCapabilities === undefined) {
            throw new Error(
                'StdioListenRouter.serve() called before setServerCapabilities(); refusing to honor a filter without capabilities'
            );
        }
        if (this._subs.size >= this._maxSubscriptions) {
            return { jsonrpc: '2.0', id: message.id, error: { code: -32_603, message: 'Subscription limit reached' } };
        }
        const filter = parseListenFilter(message);
        if (filter === undefined) {
            return {
                jsonrpc: '2.0',
                id: message.id,
                error: { code: -32_602, message: "Invalid params: 'notifications' is required and must be a valid SubscriptionFilter" }
            };
        }
        const honored = honoredSubset(filter, this._serverCapabilities);
        this._subs.set(message.id, honored);
        return stampSubscriptionId({ method: 'notifications/subscriptions/acknowledged', params: { notifications: honored } }, message.id);
    }

    /**
     * Tear down one subscription (inbound `notifications/cancelled`). Returns
     * `true` when a subscription was removed. After this call NOTHING further
     * is delivered for that subscription id (the post-cancel hardening).
     */
    cancel(id: RequestId): boolean {
        return this._subs.delete(id);
    }

    /**
     * Route an outbound notification through the active subscriptions.
     *
     * - For a subscription-gated change notification, returns one stamped copy
     *   per subscription that opted in to it (an empty array means it is
     *   dropped — the modern era never delivers an un-requested change type).
     * - For any other outbound message, returns `'passthrough'` (the entry
     *   forwards it as-is).
     */
    routeOutbound(message: { method: string; params?: { [key: string]: unknown } }): NotificationBody[] | 'passthrough' {
        if (!CHANGE_NOTIFICATION_METHODS.has(message.method)) {
            return 'passthrough';
        }
        const uriParam: unknown = message.params?.['uri'];
        const uri = typeof uriParam === 'string' ? uriParam : undefined;
        const event = notificationToServerEvent(message.method, uri);
        const out: NotificationBody[] = [];
        for (const [subscriptionId, filter] of this._subs) {
            if (listenFilterAccepts(filter, event)) {
                out.push(stampSubscriptionId({ method: message.method, params: message.params ?? {} }, subscriptionId));
            }
        }
        return out;
    }

    /**
     * Server-side graceful teardown of every active subscription: returns the
     * empty `subscriptions/listen` JSON-RPC result for each subscription id —
     * the spec's graceful-close signal, `_meta` carrying the subscription id
     * and the serving instance's identity — for the entry to emit before
     * closing the wire. Clears the set so nothing further is delivered.
     */
    teardownAll(): ListenCloseFrame[] {
        const out: ListenCloseFrame[] = [];
        for (const id of this._subs.keys()) {
            out.push({
                jsonrpc: '2.0',
                id,
                result: {
                    resultType: 'complete',
                    _meta: {
                        [SUBSCRIPTION_ID_META_KEY]: id,
                        ...(this._serverInfo !== undefined && { [SERVER_INFO_META_KEY]: this._serverInfo })
                    }
                }
            });
        }
        this._subs.clear();
        return out;
    }
}

function notificationToServerEvent(method: string, uri: string | undefined): import('./serverEventBus').ServerEvent {
    switch (method) {
        case 'notifications/tools/list_changed': {
            return { kind: 'tools_list_changed' };
        }
        case 'notifications/prompts/list_changed': {
            return { kind: 'prompts_list_changed' };
        }
        case 'notifications/resources/list_changed': {
            return { kind: 'resources_list_changed' };
        }
        default: {
            return { kind: 'resource_updated', uri: uri ?? '' };
        }
    }
}
