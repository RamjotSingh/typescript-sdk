import {
    CLIENT_CAPABILITIES_META_KEY,
    CLIENT_INFO_META_KEY,
    PROTOCOL_VERSION_META_KEY,
    type AuthInfo
} from '@modelcontextprotocol/core-internal';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { createMcpHandler } from '../../src/server/createMcpHandler';
import { requireBearerAuth } from '../../src/server/middleware/bearerAuth';
import type { McpHttpHandler } from '../../src/server/createMcpHandler';
import type { AccessDecision, AccessTarget, SubscriptionLifetimeOptions } from '../../src/server/listenRouter';
import { McpServer } from '../../src/server/mcp';

const MODERN_REVISION = '2026-07-28';
const DRAFT_CAP = 'io.modelcontextprotocol/subscription-lifetime';

type JsonMessage = Record<string, unknown> & {
    params?: Record<string, unknown>;
    result?: Record<string, unknown>;
    error?: Record<string, unknown>;
};

class Deferred<T> {
    promise: Promise<T>;
    resolve!: (value: T) => void;

    constructor() {
        this.promise = new Promise<T>(resolve => {
            this.resolve = resolve;
        });
    }
}

let clock = 1_700_000_000_000;

beforeEach(() => {
    vi.useFakeTimers();
    clock = 1_700_000_000_000;
});

afterEach(() => {
    vi.useRealTimers();
});

function iso(ms: number): string {
    return new Date(ms).toISOString();
}

function envelope(draft = true): Record<string, unknown> {
    return {
        [PROTOCOL_VERSION_META_KEY]: MODERN_REVISION,
        [CLIENT_INFO_META_KEY]: { name: 'lifetime-test-client', version: '1.0.0' },
        [CLIENT_CAPABILITIES_META_KEY]: draft ? { experimental: { [DRAFT_CAP]: {} } } : {}
    };
}

function factory(): McpServer {
    return new McpServer(
        { name: 'lifetime-test-server', version: '1.0.0' },
        {
            capabilities: {
                resources: { subscribe: true, listChanged: true },
                tools: { listChanged: true },
                prompts: { listChanged: true }
            }
        }
    );
}

function auth(expiresAtMs: number | undefined, scopes: string[] = ['read'], subject = 'alice', clientId = 'client-a'): AuthInfo {
    return {
        token: `t-${expiresAtMs ?? 'none'}-${scopes.join('.')}-${subject}-${clientId}`,
        clientId,
        scopes,
        ...(expiresAtMs !== undefined && { expiresAt: expiresAtMs / 1000 }),
        extra: { sub: subject }
    };
}

/** A server that implements the lifetime SEP alone. */
function lifetimeHandler(options: SubscriptionLifetimeOptions = {}): McpHttpHandler {
    return createMcpHandler(factory, { keepAliveMs: 0, subscriptionLifetime: { now: () => clock, ...options } });
}

function listenRequest(
    id: string | number,
    notifications: Record<string, unknown>,
    extra: Record<string, unknown> = {},
    draft = true
): Request {
    return new Request('http://localhost/mcp', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            Accept: 'application/json, text/event-stream',
            'mcp-protocol-version': MODERN_REVISION,
            'mcp-method': 'subscriptions/listen'
        },
        body: JSON.stringify({
            jsonrpc: '2.0',
            id,
            method: 'subscriptions/listen',
            params: { _meta: envelope(draft), notifications, ...extra }
        })
    });
}

class SseReader {
    private readonly reader: ReadableStreamDefaultReader<Uint8Array>;
    private readonly decoder = new TextDecoder();
    private buffer = '';
    private pending: Promise<unknown | undefined> | undefined;
    private pendingSettled = false;

    constructor(response: Response) {
        expect(response.status).toBe(200);
        expect(response.headers.get('content-type')).toBe('text/event-stream');
        this.reader = response.body!.getReader();
    }

    next(): Promise<unknown | undefined> {
        if (this.pending !== undefined) {
            const pending = this.pending;
            this.pending = undefined;
            this.pendingSettled = false;
            return pending;
        }
        return this.readNext();
    }

    async expectNoMessage(): Promise<void> {
        if (this.pending === undefined) {
            this.pendingSettled = false;
            this.pending = this.readNext().then(value => {
                this.pendingSettled = true;
                return value;
            });
        }
        await Promise.resolve();
        await Promise.resolve();
        expect(this.pendingSettled).toBe(false);
    }

    cancel(): Promise<void> {
        return this.reader.cancel();
    }

    private async readNext(): Promise<unknown | undefined> {
        for (;;) {
            const idx = this.buffer.indexOf('\n\n');
            if (idx !== -1) {
                const frame = this.buffer.slice(0, idx);
                this.buffer = this.buffer.slice(idx + 2);
                const data = frame.split('\n').find(line => line.startsWith('data: '));
                if (data !== undefined) return JSON.parse(data.slice(6));
                continue;
            }
            const { done, value } = await this.reader.read();
            if (done) return undefined;
            this.buffer += this.decoder.decode(value, { stream: true });
        }
    }
}

async function open(
    h: McpHttpHandler,
    id: string | number,
    notifications: Record<string, unknown>,
    authInfo = auth(clock + 60_000),
    extra: Record<string, unknown> = {},
    draft = true
): Promise<{ stream: SseReader; ack: JsonMessage }> {
    const response = await h.fetch(listenRequest(id, notifications, extra, draft), { authInfo });
    const stream = new SseReader(response);
    const ack = (await stream.next()) as JsonMessage;
    expect(ack.method).toBe('notifications/subscriptions/acknowledged');
    return { stream, ack };
}

async function json(response: Response): Promise<JsonMessage> {
    return (await response.json()) as JsonMessage;
}

async function advance(ms: number): Promise<void> {
    clock += ms;
    await vi.advanceTimersByTimeAsync(ms);
}

describe('Lifetime SEP server tests', () => {
    it('lifetime SEP §4 rules 1-2: authorizedUntil equals authInfo.expiresAt and is capped by maxAuthorizationLifetimeMs', async () => {
        const h1 = lifetimeHandler();
        const { ack: tokenAck } = await open(h1, 1, { resourceSubscriptions: ['file:///a'] }, auth(clock + 12_345));
        expect(tokenAck.params?.authorizedUntil).toBe(iso(clock + 12_345));
        await h1.close();

        const h2 = lifetimeHandler({ maxAuthorizationLifetimeMs: 5_000 });
        const { ack: cappedAck } = await open(h2, 2, { resourceSubscriptions: ['file:///a'] }, auth(clock + 60_000));
        expect(cappedAck.params?.authorizedUntil).toBe(iso(clock + 5_000));
        await h2.close();
    });

    it('lifetime SEP §4 rule 1: authorizedUntil is omitted when no token expiry or policy cap is known', async () => {
        const h = lifetimeHandler();
        const { ack } = await open(h, 1, { resourceSubscriptions: ['file:///a'] }, auth(undefined));
        expect(ack.params).not.toHaveProperty('authorizedUntil');
        await h.close();
    });

    it('lifetime SEP §3 rule 1: writes no notification after authorization expiry', async () => {
        const h = lifetimeHandler();
        const { stream } = await open(h, 1, { resourceSubscriptions: ['file:///a'] }, auth(clock + 1_000));
        clock += 1_001;
        h.notify.resourceUpdated('file:///a');
        const end = (await stream.next()) as JsonMessage;
        expect(end.error).toMatchObject({ code: -32_028, data: { reason: 'token_expiry' } });
        expect(JSON.stringify(end)).not.toContain('notifications/resources/updated');
        await h.close();
    });

    it('lifetime SEP §5 rules 1 and 4: draft gets AuthorizationEnded last frame; non-draft closes without a response; neither sends cancelled', async () => {
        const draft = lifetimeHandler();
        const { stream } = await open(draft, 1, { resourceSubscriptions: ['file:///a'] }, auth(clock + 500));
        await advance(500);
        const final = (await stream.next()) as JsonMessage;
        expect(final).toEqual({
            jsonrpc: '2.0',
            id: 1,
            error: { code: -32_028, message: 'Authorization ended', data: { reason: 'token_expiry' } }
        });
        expect(JSON.stringify(final)).not.toContain('notifications/cancelled');
        expect(await stream.next()).toBeUndefined();
        await draft.close();

        const old = lifetimeHandler();
        const { stream: oldStream } = await open(old, 2, { resourceSubscriptions: ['file:///a'] }, auth(clock + 500), {}, false);
        await advance(500);
        expect(await oldStream.next()).toBeUndefined();
        await old.close();
    });

    it('lifetime SEP §3 rules 2 and 5: deny for one listed resource silently removes it and keeps the stream open', async () => {
        let denyB = false;
        const h = lifetimeHandler({
            authorize: (_auth, target) => (target.kind === 'resource' && target.uri === 'file:///b' && denyB ? 'deny' : 'allow')
        });
        const { stream } = await open(h, 1, { resourceSubscriptions: ['file:///a', 'file:///b'] });
        denyB = true;
        h.notify.resourceUpdated('file:///b');
        await stream.expectNoMessage();
        h.notify.resourceUpdated('file:///a');
        expect(await stream.next()).toMatchObject({ method: 'notifications/resources/updated', params: { uri: 'file:///a' } });
        h.notify.resourceUpdated('file:///b');
        await stream.expectNoMessage();
        expect(h.subscriptions.list()).toHaveLength(1);
        expect(h.subscriptions.list()[0]?.acknowledged.resourceSubscriptions).toEqual(['file:///a']);
        await h.close();
    });

    it('lifetime SEP §3 rule 4: revoke() ends matching streams at once with revoked, leaves non-matching streams, and returns count', async () => {
        const h = lifetimeHandler();
        const one = await open(h, 'one', { resourceSubscriptions: ['file:///a'] });
        const two = await open(h, 'two', { resourceSubscriptions: ['file:///b'] });
        expect(h.subscriptions.revoke(s => s.subscriptionId === 'one')).toBe(1);
        expect(await one.stream.next()).toMatchObject({ error: { code: -32_028, data: { reason: 'revoked' } } });
        h.notify.resourceUpdated('file:///b');
        expect(await two.stream.next()).toMatchObject({ method: 'notifications/resources/updated', params: { uri: 'file:///b' } });
        await h.close();
    });

    it('lifetime SEP §3 rule 2: an unavailable access check drops the notification and keeps the entry', async () => {
        let unavailable = false;
        const h = lifetimeHandler({ authorize: () => (unavailable ? 'unavailable' : 'allow') });
        const { stream } = await open(h, 1, { resourceSubscriptions: ['file:///a'] });
        unavailable = true;
        h.notify.resourceUpdated('file:///a');
        await stream.expectNoMessage();
        expect(h.subscriptions.list()[0]?.acknowledged.resourceSubscriptions).toEqual(['file:///a']);
        await h.close();
    });

    it('lifetime SEP §3 rules 4 and 5: an access-change signal awaits async checks and ends a stream left with no entries with revoked', async () => {
        const pending = new Deferred<AccessDecision>();
        let calls = 0;
        const h = lifetimeHandler({ authorize: () => (++calls === 1 ? 'allow' : pending.promise) });
        const { stream } = await open(h, 1, { resourceSubscriptions: ['file:///a'] });
        const recheck = h.subscriptions.recheckAccess();
        await stream.expectNoMessage();
        pending.resolve('deny');
        await recheck;
        expect(await stream.next()).toMatchObject({ error: { code: -32_028, data: { reason: 'revoked' } } });
        await h.close();
    });

    it('lifetime SEP §3 rules 3 and 6: a scope requirement the stream already meets does not restart the policy cap on its authorization', async () => {
        const h = lifetimeHandler({ maxAuthorizationLifetimeMs: 1_000 });
        const { stream } = await open(h, 1, { resourceSubscriptions: ['file:///a'] }, auth(undefined, ['read']));
        const before = h.subscriptions.list()[0]!;
        await advance(100);
        h.subscriptions.requireScopes(['read'], clock + 10_000);
        expect(h.subscriptions.list()[0]?.authorizedUntil).toBe(before.authorizedUntil);
        await advance(900);
        h.notify.resourceUpdated('file:///a');
        expect(await stream.next()).toMatchObject({ error: { code: -32_028, data: { reason: 'token_expiry' } } });
        await h.close();
    });

    it('lifetime SEP §3 rule 3: relaxing a requirement moves the deadline back to the token expiry, and the stream keeps delivering', async () => {
        const h = lifetimeHandler();
        const { stream } = await open(h, 1, { resourceSubscriptions: ['file:///a'] }, auth(clock + 60_000, ['read']));
        h.subscriptions.requireScopes(['audit'], clock + 1_000);
        expect(h.subscriptions.list()[0]?.authorizedUntil).toBe(iso(clock + 1_000));
        h.subscriptions.requireScopes(['read'], clock + 1_000);
        expect(h.subscriptions.list()[0]?.authorizedUntil).toBe(iso(clock + 60_000));
        await advance(1_000);
        h.notify.resourceUpdated('file:///a');
        expect(await stream.next()).toMatchObject({ method: 'notifications/resources/updated', params: { uri: 'file:///a' } });
        await h.close();
    });
});

/** Denies tool list changes and `file:///b`, cannot decide `file:///c`, and allows everything else. */
function partlyDenied(_auth: AuthInfo | undefined, target: AccessTarget): AccessDecision {
    if (target.kind === 'toolsList' || (target.kind === 'resource' && target.uri === 'file:///b')) return 'deny';
    return target.kind === 'resource' && target.uri === 'file:///c' ? 'unavailable' : 'allow';
}

describe('open-time authorization', () => {
    it('lifetime SEP §3 rule 5: a filter with entries the authorization does not permit is refused with -32602 naming them in data.denied, and no stream opens', async () => {
        const h = lifetimeHandler({ authorize: partlyDenied });
        const response = await h.fetch(
            listenRequest(1, {
                toolsListChanged: true,
                promptsListChanged: true,
                resourceSubscriptions: ['file:///a', 'file:///b', 'file:///c']
            }),
            { authInfo: auth(clock + 60_000) }
        );
        expect(response.status).toBe(200);
        expect(response.headers.get('content-type')).toContain('application/json');
        expect(await json(response)).toEqual({
            jsonrpc: '2.0',
            error: {
                code: -32_602,
                message: 'Not permitted',
                data: { denied: { toolsListChanged: true, resourceSubscriptions: ['file:///b'] } }
            },
            id: 1
        });
        expect(h.subscriptions.list()).toEqual([]);
        await h.close();
    });

    it('lifetime SEP §3 rule 5: a client of an earlier protocol version gets the same refusal', async () => {
        const h = lifetimeHandler({ authorize: partlyDenied });
        const response = await h.fetch(listenRequest(1, { resourceSubscriptions: ['file:///a', 'file:///b'] }, {}, false), {
            authInfo: auth(clock + 60_000)
        });
        expect(response.status).toBe(200);
        expect(await json(response)).toEqual({
            jsonrpc: '2.0',
            error: { code: -32_602, message: 'Not permitted', data: { denied: { resourceSubscriptions: ['file:///b'] } } },
            id: 1
        });
        expect(h.subscriptions.list()).toEqual([]);
        await h.close();
    });

    it('lifetime SEP §3 rule 5: without the denied entries the same filter is acknowledged in full, including an entry whose check is unavailable', async () => {
        const h = lifetimeHandler({ authorize: partlyDenied });
        const { stream, ack } = await open(h, 2, { promptsListChanged: true, resourceSubscriptions: ['file:///a', 'file:///c'] });
        expect(ack.params?.notifications).toEqual({ promptsListChanged: true, resourceSubscriptions: ['file:///a', 'file:///c'] });
        h.notify.resourceUpdated('file:///c');
        await stream.expectNoMessage();
        h.notify.resourceUpdated('file:///a');
        expect(await stream.next()).toMatchObject({ method: 'notifications/resources/updated', params: { uri: 'file:///a' } });
        expect(h.subscriptions.list()).toHaveLength(1);
        await h.close();
    });

    it('lifetime SEP §3 rule 5: a filter with no permitted entry is refused with -32602 naming every entry, and no stream opens', async () => {
        const h = lifetimeHandler({ authorize: () => 'deny' });
        const response = await h.fetch(listenRequest(1, { resourceSubscriptions: ['file:///a', 'file:///b'] }), {
            authInfo: auth(clock + 60_000)
        });
        expect(response.status).toBe(200);
        expect(response.headers.get('content-type')).toContain('application/json');
        expect(await json(response)).toEqual({
            jsonrpc: '2.0',
            error: { code: -32_602, message: 'Not permitted', data: { denied: { resourceSubscriptions: ['file:///a', 'file:///b'] } } },
            id: 1
        });
        expect(h.subscriptions.list()).toEqual([]);
        await h.close();
    });

    it('subscriptions ack compatibility: without authorize configured, the capability-honored acknowledgment is unchanged', async () => {
        const h = lifetimeHandler();
        const { ack } = await open(h, 1, {
            toolsListChanged: true,
            promptsListChanged: true,
            resourcesListChanged: true,
            resourceSubscriptions: ['file:///a', 'file:///b']
        });
        expect(ack.params?.notifications).toEqual({
            toolsListChanged: true,
            promptsListChanged: true,
            resourcesListChanged: true,
            resourceSubscriptions: ['file:///a', 'file:///b']
        });
        await h.close();
    });

    it('lifetime SEP §3 rule 5: async open-time authorize is awaited before the server answers', async () => {
        const pending = new Deferred<AccessDecision>();
        const h = lifetimeHandler({
            authorize: (_auth, target) => (target.kind === 'resource' && target.uri === 'file:///b' ? pending.promise : 'allow')
        });
        let settled = false;
        const answered = h
            .fetch(listenRequest(1, { resourceSubscriptions: ['file:///a', 'file:///b'] }), { authInfo: auth(clock + 60_000) })
            .then(value => {
                settled = true;
                return value;
            });
        await Promise.resolve();
        await Promise.resolve();
        expect(settled).toBe(false);
        pending.resolve('deny');
        const response = await answered;
        expect(response.status).toBe(200);
        expect(await json(response)).toMatchObject({
            error: { code: -32_602, data: { denied: { resourceSubscriptions: ['file:///b'] } } }
        });
        expect(h.subscriptions.list()).toEqual([]);
        await h.close();
    });
});

describe('reviewed defect regressions', () => {
    it('defect 1: token deadlines beyond 2^31-1ms do not fire immediately', async () => {
        const h = lifetimeHandler();
        const { stream } = await open(h, 1, { resourceSubscriptions: ['file:///a'] }, auth(clock + 30 * 24 * 60 * 60 * 1000));
        await advance(5);
        h.notify.resourceUpdated('file:///a');
        expect(await stream.next()).toMatchObject({ method: 'notifications/resources/updated', params: { uri: 'file:///a' } });
        await h.close();
    });

    it('defect 2: async delivery re-checks after authorize and does not write after a deadline closes the stream', async () => {
        const pending = new Deferred<AccessDecision>();
        let calls = 0;
        const h = lifetimeHandler({ authorize: () => (++calls === 1 ? 'allow' : pending.promise) });
        const { stream } = await open(h, 1, { resourceSubscriptions: ['file:///a'] }, auth(clock + 1_000));
        h.notify.resourceUpdated('file:///a');
        await advance(1_000);
        pending.resolve('allow');
        await Promise.resolve();
        expect(await stream.next()).toMatchObject({ error: { code: -32_028, data: { reason: 'token_expiry' } } });
        await h.close();
    });

    it('defect 6: last-entry removal discovered on a change ends with revoked only at the deadline', async () => {
        let deny = false;
        const h = lifetimeHandler({ authorize: () => (deny ? 'deny' : 'allow') });
        const { stream } = await open(h, 1, { resourceSubscriptions: ['file:///a'] }, auth(clock + 1_000));
        deny = true;
        h.notify.resourceUpdated('file:///a');
        await stream.expectNoMessage();
        await advance(1_000);
        expect(await stream.next()).toMatchObject({ error: { code: -32_028, data: { reason: 'revoked' } } });
        await h.close();
    });

    it('lifetime SEP §5 rule 3: expired tokens are rejected with HTTP 401 by bearer-auth middleware before listen opens', async () => {
        const gate = requireBearerAuth({
            verifier: { verifyAccessToken: async () => ({ ...auth(1_000), expiresAt: 1 }) }
        });
        const request = listenRequest(1, { resourceSubscriptions: ['file:///a'] });
        request.headers.set('authorization', 'Bearer expired');
        const result = await gate(request);
        expect(result).toBeInstanceOf(Response);
        expect((result as Response).status).toBe(401);
        expect(await json(result as Response)).toMatchObject({ error: 'invalid_token' });
    });

    it('lifetime SEP §5 rule 4: non-draft publish after expiry writes nothing and closes without response', async () => {
        const h = lifetimeHandler();
        const { stream } = await open(h, 1, { resourceSubscriptions: ['file:///a'] }, auth(clock + 1_000), {}, false);
        clock += 1_001;
        h.notify.resourceUpdated('file:///a');
        expect(await stream.next()).toBeUndefined();
        await h.close();
    });
});
