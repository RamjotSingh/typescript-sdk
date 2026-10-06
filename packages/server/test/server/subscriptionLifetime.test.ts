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
import type {
    AccessDecision,
    AccessTarget,
    SubscriptionLifecycleOptions,
    SubscriptionLifetimeOptions
} from '../../src/server/listenRouter';
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

/** A server that implements both SEPs. */
function handler(
    options: Pick<SubscriptionLifetimeOptions, 'authorize' | 'maxAuthorizationLifetimeMs'> & SubscriptionLifecycleOptions = {}
): McpHttpHandler {
    const { authorize, maxAuthorizationLifetimeMs, ...lifecycle } = options;
    return createMcpHandler(factory, {
        keepAliveMs: 0,
        subscriptionLifetime: { now: () => clock, authorize, maxAuthorizationLifetimeMs },
        subscriptionLifecycle: { random: () => 0, ...lifecycle }
    });
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

function updateRequest(
    id: string | number,
    streamId: string,
    params: Record<string, unknown> = {},
    options: { mcpName?: string; draft?: boolean } = {}
): Request {
    return new Request('http://localhost/mcp', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            Accept: 'application/json',
            'mcp-protocol-version': MODERN_REVISION,
            'mcp-method': 'subscriptions/update',
            'mcp-name': options.mcpName ?? streamId
        },
        body: JSON.stringify({
            jsonrpc: '2.0',
            id,
            method: 'subscriptions/update',
            params: { _meta: envelope(options.draft ?? true), streamId, ...params }
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

    it('lifetime SEP §3 rule 2 and lifecycle SEP §3.5: authorize unavailable drops, keeps the entry, and sends missed', async () => {
        const h = handler({ authorize: () => 'unavailable' });
        const { stream } = await open(h, 1, { resourceSubscriptions: ['file:///a'], lifecycle: true });
        h.notify.resourceUpdated('file:///a');
        expect(await stream.next()).toMatchObject({ method: 'notifications/subscriptions/lifecycle', params: { type: 'missed' } });
        expect(h.subscriptions.list()[0]?.acknowledged.resourceSubscriptions).toEqual(['file:///a']);
        await h.close();
    });

    it('lifetime SEP §3 rule 5 + lifecycle SEP §3.4 rule 4: last entry removal ends with revoked no earlier than the next reminder', async () => {
        let deny = false;
        const h = handler({ authorize: () => (deny ? 'deny' : 'allow'), random: () => 0 });
        const { stream } = await open(h, 1, { resourceSubscriptions: ['file:///a'], lifecycle: true }, auth(clock + 1_000));
        const initialUpdatedAt = h.subscriptions.list()[0]?.lastUpdatedAt;
        deny = true;
        h.notify.resourceUpdated('file:///a');
        await stream.expectNoMessage();
        expect(h.subscriptions.list()[0]?.lastUpdatedAt).toBe(initialUpdatedAt);
        await advance(900);
        expect(await stream.next()).toMatchObject({ method: 'notifications/subscriptions/lifecycle', params: { type: 'access_reduced' } });
        expect(await stream.next()).toMatchObject({ error: { code: -32_028, data: { reason: 'revoked' } } });
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

describe('Lifecycle SEP server tests', () => {
    it('lifecycle SEP §3.1: lifecycle is acknowledged only when requested and no lifecycle notifications are sent otherwise', async () => {
        const h = handler({ authorize: () => 'unavailable' });
        const noLifecycle = await open(h, 1, { resourceSubscriptions: ['file:///a'] });
        expect(noLifecycle.ack.params?.notifications).not.toHaveProperty('lifecycle');
        h.notify.resourceUpdated('file:///a');
        await noLifecycle.stream.expectNoMessage();

        const withLifecycle = await open(h, 2, { resourceSubscriptions: ['file:///a'], lifecycle: true });
        expect(withLifecycle.ack.params?.notifications).toMatchObject({ lifecycle: true });
        h.notify.resourceUpdated('file:///a');
        expect(await withLifecycle.stream.next()).toMatchObject({
            method: 'notifications/subscriptions/lifecycle',
            params: { type: 'missed' }
        });
        await h.close();
    });

    it('lifecycle SEP §2 rule 1: uses requested expiresAt exactly and rejects past expiresAt with -32602', async () => {
        const h = handler();
        const requested = iso(clock + 5_000);
        const { ack } = await open(h, 1, { resourceSubscriptions: ['file:///a'] }, auth(clock + 60_000), { expiresAt: requested });
        expect(ack.params?.expiresAt).toBe(requested);

        const response = await h.fetch(listenRequest(2, { resourceSubscriptions: ['file:///a'] }, { expiresAt: iso(clock - 1) }), {
            authInfo: auth(clock + 60_000)
        });
        expect(await json(response)).toMatchObject({ error: { code: -32_602, message: 'Invalid expiresAt' } });
        await h.close();
    });

    it('lifecycle SEP §2 rule 1: rejects expiresAt beyond the maximum with -32602 and maxExpiresAt', async () => {
        const h = handler({ maxStreamLifetimeMs: 10_000 });
        const response = await h.fetch(listenRequest(1, { resourceSubscriptions: ['file:///a'] }, { expiresAt: iso(clock + 10_001) }), {
            authInfo: auth(clock + 60_000)
        });
        expect(await json(response)).toMatchObject({ error: { code: -32_602, data: { maxExpiresAt: iso(clock + 10_000) } } });
        await h.close();
    });

    it('lifecycle SEP §2 rules 1-3: omitted expiresAt uses server maximum and ends at expiry with completion result while paused', async () => {
        const h = handler({ maxStreamLifetimeMs: 2_000, reminderLeadsMs: () => [] });
        const { stream, ack } = await open(h, 1, { resourceSubscriptions: ['file:///a'], lifecycle: true }, auth(clock + 1_000));
        expect(ack.params?.expiresAt).toBe(iso(clock + 2_000));
        await advance(1_000);
        expect(await stream.next()).toMatchObject({
            method: 'notifications/subscriptions/lifecycle',
            params: { type: 'reauthorization_required' }
        });
        await advance(1_000);
        expect(await stream.next()).toMatchObject({ id: 1, result: { resultType: 'complete' } });
        await h.close();
    });

    it('lifecycle SEP §2 rule 3: expiry coinciding with authorization deadline sends completion result, not AuthorizationEnded', async () => {
        const h = handler();
        const { stream } = await open(h, 1, { resourceSubscriptions: ['file:///a'] }, auth(clock + 1_000), {
            expiresAt: iso(clock + 1_000)
        });
        await advance(1_000);
        const final = (await stream.next()) as JsonMessage;
        expect(final.result).toMatchObject({ resultType: 'complete' });
        expect(final).not.toHaveProperty('error');
        await h.close();
    });

    it('lifecycle SEP §3.3 rule 2: default reminders for <10 min use 10/5/3/2/1 percent with first jittered earlier', async () => {
        const h = handler({ random: () => 0.5 });
        const { stream } = await open(h, 1, { resourceSubscriptions: ['file:///a'], lifecycle: true }, auth(clock + 100_000));
        await advance(89_499);
        await stream.expectNoMessage();
        await advance(1);
        expect(await stream.next()).toMatchObject({ params: { type: 'reauthorization_required', authorizedUntil: iso(clock + 10_500) } });
        await h.close();
    });

    it('lifecycle SEP §3.3 rule 2: default reminders for >=10 min use 60/30/15/10/5 second leads', async () => {
        const h = handler({ random: () => 0 });
        const { stream } = await open(h, 1, { resourceSubscriptions: ['file:///a'], lifecycle: true }, auth(clock + 600_000));
        await advance(540_000);
        expect(await stream.next()).toMatchObject({ params: { type: 'reauthorization_required' } });
        await h.close();
    });

    it('lifecycle SEP §3.3 rule 1: no reminders precede an expiry that comes before the authorization deadline', async () => {
        const h = handler();
        const { stream } = await open(h, 1, { resourceSubscriptions: ['file:///a'], lifecycle: true }, auth(clock + 60_000), {
            expiresAt: iso(clock + 500)
        });
        await advance(500);
        const final = (await stream.next()) as JsonMessage;
        expect(final.result).toMatchObject({ resultType: 'complete' });
        expect(JSON.stringify(final)).not.toContain('reauthorization_required');
        await h.close();
    });

    it('lifecycle SEP §3.3 rule 4 and §4 rule 2: requireScopes sends immediate insufficient_authorization reminder, 403 challenge, then scoped update succeeds', async () => {
        const h = handler();
        const { stream, ack } = await open(h, 1, { resourceSubscriptions: ['file:///a'], lifecycle: true }, auth(clock + 60_000, ['read']));
        h.subscriptions.requireScopes(['audit'], clock + 5_000);
        expect(await stream.next()).toMatchObject({
            params: { type: 'reauthorization_required', reason: 'insufficient_authorization', authorizedUntil: iso(clock + 5_000) }
        });

        const forbidden = await h.fetch(updateRequest(2, ack.params?.streamId as string), { authInfo: auth(clock + 70_000, ['read']) });
        expect(forbidden.status).toBe(403);
        expect(forbidden.headers.get('www-authenticate')).toContain('error="insufficient_scope"');
        expect(forbidden.headers.get('www-authenticate')).toContain('read audit');

        const ok = await h.fetch(updateRequest(3, ack.params?.streamId as string), { authInfo: auth(clock + 70_000, ['read', 'audit']) });
        expect(await json(ok)).toMatchObject({ result: { authorizedUntil: iso(clock + 70_000) } });
        await h.close();
    });

    it('lifecycle SEP §5 rules 1-2: at the deadline lifecycle+streamId pauses and repeats only lifecycle reminders', async () => {
        const h = handler({ pausedReminderIntervalMs: 60_000 });
        const { stream } = await open(h, 1, { resourceSubscriptions: ['file:///a'], lifecycle: true }, auth(clock + 1_000));
        await advance(1_000);
        expect(h.subscriptions.list()[0]?.state).toBe('paused');
        expect(await stream.next()).toMatchObject({
            method: 'notifications/subscriptions/lifecycle',
            params: { type: 'reauthorization_required' }
        });
        h.notify.resourceUpdated('file:///a');
        await advance(60_000);
        expect(await stream.next()).toMatchObject({
            method: 'notifications/subscriptions/lifecycle',
            params: { type: 'reauthorization_required' }
        });
        await h.close();
    });

    it('lifecycle SEP §5: lifecycle stream without a streamId ends at the deadline instead of pausing', async () => {
        const h = handler({ inPlaceUpdates: false, reminderLeadsMs: () => [] });
        const { stream, ack } = await open(h, 1, { resourceSubscriptions: ['file:///a'], lifecycle: true }, auth(clock + 1_000));
        expect(ack.params).not.toHaveProperty('streamId');
        await advance(1_000);
        expect(await stream.next()).toMatchObject({ error: { code: -32_028, data: { reason: 'token_expiry' } } });
        await h.close();
    });

    it('lifecycle SEP §5 rules 4-5: held notifications while paused are delivered after update in order and re-checked', async () => {
        const denied = new Set<string>();
        const h = handler({
            reminderLeadsMs: () => [],
            authorize: (_auth, target) => (target.kind === 'resource' && denied.has(target.uri) ? 'deny' : 'allow')
        });
        const { stream, ack } = await open(
            h,
            1,
            { resourceSubscriptions: ['file:///a', 'file:///b'], lifecycle: true },
            auth(clock + 1_000)
        );
        await advance(1_000);
        await stream.next();
        h.notify.resourceUpdated('file:///a');
        h.notify.resourceUpdated('file:///b');
        denied.add('file:///b');
        await h.fetch(updateRequest(2, ack.params?.streamId as string), { authInfo: auth(clock + 60_000) });
        await Promise.resolve();
        expect(await stream.next()).toMatchObject({ method: 'notifications/subscriptions/lifecycle', params: { type: 'access_reduced' } });
        expect(await stream.next()).toMatchObject({ method: 'notifications/resources/updated', params: { uri: 'file:///a' } });
        await stream.expectNoMessage();
        await h.close();
    });

    it('lifecycle SEP §5 rules 4-5: hold false drops paused notifications and sends missed after update', async () => {
        const h = handler({ hold: false, reminderLeadsMs: () => [] });
        const { stream, ack } = await open(h, 1, { resourceSubscriptions: ['file:///a'], lifecycle: true }, auth(clock + 1_000));
        await advance(1_000);
        await stream.next();
        h.notify.resourceUpdated('file:///a');
        await h.fetch(updateRequest(2, ack.params?.streamId as string), { authInfo: auth(clock + 60_000) });
        await Promise.resolve();
        expect(await stream.next()).toMatchObject({ method: 'notifications/subscriptions/lifecycle', params: { type: 'missed' } });
        await h.close();
    });

    it('lifecycle SEP §5 rule 4: hold cap drops excess and sends missed after update', async () => {
        const h = handler({ hold: { maxNotifications: 1 }, reminderLeadsMs: () => [] });
        const { stream, ack } = await open(
            h,
            1,
            { resourceSubscriptions: ['file:///a', 'file:///b'], lifecycle: true },
            auth(clock + 1_000)
        );
        await advance(1_000);
        await stream.next();
        h.notify.resourceUpdated('file:///a');
        h.notify.resourceUpdated('file:///b');
        await h.fetch(updateRequest(2, ack.params?.streamId as string), { authInfo: auth(clock + 60_000) });
        await Promise.resolve();
        expect(await stream.next()).toMatchObject({ method: 'notifications/resources/updated', params: { uri: 'file:///a' } });
        expect(await stream.next()).toMatchObject({ method: 'notifications/subscriptions/lifecycle', params: { type: 'missed' } });
        await h.close();
    });

    it('lifecycle SEP §5 rule 4: held notifications are written only to their own stream', async () => {
        const h = handler({ reminderLeadsMs: () => [] });
        const one = await open(h, 'one', { resourceSubscriptions: ['file:///a'], lifecycle: true }, auth(clock + 1_000));
        const two = await open(h, 'two', { resourceSubscriptions: ['file:///a'], lifecycle: true }, auth(clock + 60_000));
        await advance(1_000);
        await one.stream.next();
        h.notify.resourceUpdated('file:///a');
        expect(await two.stream.next()).toMatchObject({ method: 'notifications/resources/updated', params: { uri: 'file:///a' } });
        await h.fetch(updateRequest(2, one.ack.params?.streamId as string), { authInfo: auth(clock + 60_000) });
        await Promise.resolve();
        expect(await one.stream.next()).toMatchObject({ method: 'notifications/resources/updated', params: { uri: 'file:///a' } });
        await two.stream.expectNoMessage();
        await h.close();
    });

    it('lifecycle SEP §3.4 rule 3: access_reduced is held until reminder and lastUpdatedAt does not move until sent', async () => {
        let denyB = false;
        const h = handler({
            authorize: (_auth, target) => (target.kind === 'resource' && target.uri === 'file:///b' && denyB ? 'deny' : 'allow')
        });
        const { stream } = await open(h, 1, { resourceSubscriptions: ['file:///a', 'file:///b'], lifecycle: true }, auth(clock + 1_000));
        denyB = true;
        const initial = h.subscriptions.list()[0]?.lastUpdatedAt;
        h.notify.resourceUpdated('file:///b');
        await stream.expectNoMessage();
        expect(h.subscriptions.list()[0]?.lastUpdatedAt).toBe(initial);
        await advance(900);
        expect(await stream.next()).toMatchObject({ method: 'notifications/subscriptions/lifecycle', params: { type: 'access_reduced' } });
        expect(h.subscriptions.list()[0]?.lastUpdatedAt).not.toBe(initial);
        await h.close();
    });

    it('lifecycle SEP §3.4 rule 1: recheckAccess sends access_reduced at once', async () => {
        let denyB = false;
        const h = handler({
            authorize: (_auth, target) => (target.kind === 'resource' && target.uri === 'file:///b' && denyB ? 'deny' : 'allow')
        });
        const { stream } = await open(h, 1, { resourceSubscriptions: ['file:///a', 'file:///b'], lifecycle: true });
        denyB = true;
        await h.subscriptions.recheckAccess();
        expect(await stream.next()).toMatchObject({ method: 'notifications/subscriptions/lifecycle', params: { type: 'access_reduced' } });
        await h.close();
    });

    it('lifecycle SEP §4 rule 5: successful update keeps stream open, advances lastUpdatedAt, and delivers past old token expiry', async () => {
        const h = handler();
        const { stream, ack } = await open(h, 1, { resourceSubscriptions: ['file:///a'], lifecycle: true }, auth(clock + 1_000));
        const oldUpdated = ack.params?.lastUpdatedAt as string;
        const response = await h.fetch(updateRequest(2, ack.params?.streamId as string), { authInfo: auth(clock + 60_000) });
        const body = await json(response);
        expect(body.result?.authorizedUntil).toBe(iso(clock + 60_000));
        expect((body.result?.lastUpdatedAt as string) > oldUpdated).toBe(true);
        await advance(1_001);
        h.notify.resourceUpdated('file:///a');
        expect(await stream.next()).toMatchObject({ method: 'notifications/resources/updated', params: { uri: 'file:///a' } });
        await h.close();
    });

    it('lifecycle SEP §4 rule 3: unknown, ended, different clientId, and different subject all return generic -32602', async () => {
        const h = handler();
        const { stream, ack } = await open(h, 1, { resourceSubscriptions: ['file:///a'], lifecycle: true });
        for (const [request, authInfo] of [
            [updateRequest(2, 'missing'), auth(clock + 60_000)],
            [updateRequest(3, ack.params?.streamId as string), auth(clock + 60_000, ['read'], 'alice', 'client-b')],
            [updateRequest(4, ack.params?.streamId as string), auth(clock + 60_000, ['read'], 'bob')]
        ] as const) {
            expect(await json(await h.fetch(request, { authInfo }))).toMatchObject({
                error: { code: -32_602, message: 'Unknown stream ID' }
            });
        }
        h.subscriptions.revoke();
        await stream.next();
        expect(
            await json(await h.fetch(updateRequest(5, ack.params?.streamId as string), { authInfo: auth(clock + 60_000) }))
        ).toMatchObject({
            error: { code: -32_602, message: 'Unknown stream ID' }
        });
        await h.close();
    });

    it('lifecycle SEP §4 rules 3-4: an unsupported field, and -32602 naming the entries when none is permitted, do not change the stream', async () => {
        let denyA = false;
        const h = handler({
            authorize: (_auth, target) => (target.kind === 'resource' && target.uri === 'file:///a' && denyA ? 'deny' : 'allow')
        });
        const { stream, ack } = await open(h, 1, { resourceSubscriptions: ['file:///a'], lifecycle: true });
        denyA = true;
        const before = h.subscriptions.list()[0];
        expect(
            await json(await h.fetch(updateRequest(2, ack.params?.streamId as string, { extra: true }), { authInfo: auth(clock + 60_000) }))
        ).toMatchObject({
            error: { code: -32_602, data: { unsupportedFields: ['extra'] } }
        });
        const refused = await h.fetch(updateRequest(3, ack.params?.streamId as string), { authInfo: auth(clock + 60_000) });
        expect(refused.status).toBe(200);
        expect(await json(refused)).toEqual({
            jsonrpc: '2.0',
            error: { code: -32_602, message: 'Not permitted', data: { denied: { resourceSubscriptions: ['file:///a'] } } },
            id: 3
        });
        expect(h.subscriptions.list()[0]).toEqual(before);
        h.notify.resourceUpdated('file:///a');
        await stream.expectNoMessage();
        await h.close();
    });

    it('lifecycle SEP §4 rule 4: same update twice yields the same result and expiresAt:null removes requested expiry under server max', async () => {
        const h = handler({ maxStreamLifetimeMs: 120_000 });
        const { ack } = await open(h, 1, { resourceSubscriptions: ['file:///a'], lifecycle: true }, auth(clock + 60_000), {
            expiresAt: iso(clock + 30_000)
        });
        const first = await json(
            await h.fetch(updateRequest(2, ack.params?.streamId as string, { expiresAt: null }), { authInfo: auth(clock + 70_000) })
        );
        const second = await json(
            await h.fetch(updateRequest(3, ack.params?.streamId as string, { expiresAt: null }), { authInfo: auth(clock + 70_000) })
        );
        expect(second.result).toEqual(first.result);
        expect(first.result?.expiresAt).toBe(iso(clock + 120_000));
        await h.close();
    });

    it('lifecycle SEP §4 rule 3: subscriptions/update with mismatched Mcp-Name is rejected by the standard-header rung', async () => {
        const h = handler();
        const { ack } = await open(h, 1, { resourceSubscriptions: ['file:///a'], lifecycle: true });
        const response = await h.fetch(updateRequest(2, ack.params?.streamId as string, {}, { mcpName: 'wrong' }), {
            authInfo: auth(clock + 60_000)
        });
        expect(response.status).toBe(400);
        expect(await json(response)).toMatchObject({ error: { code: -32_020 } });
        await h.close();
    });

    it('lifecycle SEP Backward Compatibility: a server without subscriptionLifecycle ignores lifecycle and expiresAt, and acknowledges with authorizedUntil alone', async () => {
        const h = lifetimeHandler();
        const { ack } = await open(h, 1, { resourceSubscriptions: ['file:///a'], lifecycle: true }, auth(clock + 60_000), {
            expiresAt: iso(clock - 1)
        });
        expect(ack.params?.authorizedUntil).toBe(iso(clock + 60_000));
        expect(ack.params?.notifications).toEqual({ resourceSubscriptions: ['file:///a'] });
        expect(ack.params).not.toHaveProperty('expiresAt');
        expect(ack.params).not.toHaveProperty('lastUpdatedAt');
        expect(ack.params).not.toHaveProperty('streamId');
        await h.close();
    });

    it('lifecycle SEP Backward Compatibility: a server without subscriptionLifecycle sends no lifecycle notification and ends the stream at the deadline', async () => {
        const h = lifetimeHandler();
        const { stream } = await open(h, 1, { resourceSubscriptions: ['file:///a'], lifecycle: true }, auth(clock + 10_000));
        await advance(10_000);
        const end = (await stream.next()) as JsonMessage;
        expect(end).toMatchObject({ id: 1, error: { code: -32_028, data: { reason: 'token_expiry' } } });
        expect(await stream.next()).toBeUndefined();
        await h.close();
    });

    it('lifecycle SEP Backward Compatibility: a server without subscriptionLifecycle answers subscriptions/update with -32601', async () => {
        const h = lifetimeHandler();
        const response = await h.fetch(updateRequest(2, 'any-stream-id'), { authInfo: auth(clock + 60_000) });
        expect(await json(response)).toMatchObject({ id: 2, error: { code: -32_601 } });
        await h.close();
    });
});

describe('prototype options', () => {
    it('subscriptionLifecycle without subscriptionLifetime is rejected when the handler is created', () => {
        expect(() => createMcpHandler(factory, { subscriptionLifecycle: {} })).toThrow(TypeError);
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

    it('lifecycle SEP §3.4 rule 5: open-time unavailable keeps the entry in the acknowledgment', async () => {
        const h = handler({
            authorize: (_auth, target) => (target.kind === 'resource' && target.uri === 'file:///b' ? 'unavailable' : 'allow')
        });
        const { ack } = await open(h, 1, { resourceSubscriptions: ['file:///a', 'file:///b'] });
        expect(ack.params?.notifications).toEqual({ resourceSubscriptions: ['file:///a', 'file:///b'] });
        await h.close();
    });

    it('lifecycle SEP §3.4 rule 5: open-time throwing authorize counts as unavailable and keeps the entry', async () => {
        const h = handler({
            authorize: (_auth, target) => {
                if (target.kind === 'resource' && target.uri === 'file:///b') throw new Error('acl offline');
                return 'allow';
            }
        });
        const { ack } = await open(h, 1, { resourceSubscriptions: ['file:///a', 'file:///b'] });
        expect(ack.params?.notifications).toEqual({ resourceSubscriptions: ['file:///a', 'file:///b'] });
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

    it('defect 1: requested expiresAt beyond 2^31-1ms does not complete immediately', async () => {
        const h = handler();
        const { stream } = await open(h, 1, { resourceSubscriptions: ['file:///a'] }, auth(clock + 60 * 24 * 60 * 60 * 1000), {
            expiresAt: iso(clock + 30 * 24 * 60 * 60 * 1000)
        });
        await advance(5);
        await stream.expectNoMessage();
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

    it('defect 2 and 7: event that first notices a passed deadline is held while pausing and delivered after update', async () => {
        const h = handler({ reminderLeadsMs: () => [] });
        const { stream, ack } = await open(h, 1, { resourceSubscriptions: ['file:///a'], lifecycle: true }, auth(clock + 1_000));
        clock += 1_001;
        h.notify.resourceUpdated('file:///a');
        expect(await stream.next()).toMatchObject({
            method: 'notifications/subscriptions/lifecycle',
            params: { type: 'reauthorization_required' }
        });
        await h.fetch(updateRequest(2, ack.params?.streamId as string), { authInfo: auth(clock + 60_000) });
        await Promise.resolve();
        expect(await stream.next()).toMatchObject({ method: 'notifications/resources/updated', params: { uri: 'file:///a' } });
        await h.close();
    });

    it('defect 3: updating a paused stream schedules reminders before the new deadline and pauses again', async () => {
        const h = handler({ reminderLeadsMs: () => [500] });
        const { stream, ack } = await open(h, 1, { resourceSubscriptions: ['file:///a'], lifecycle: true }, auth(clock + 1_000));
        await advance(1_000);
        await stream.next();
        await stream.next();
        await h.fetch(updateRequest(2, ack.params?.streamId as string), { authInfo: auth(clock + 2_000) });
        await advance(1_500);
        expect(await stream.next()).toMatchObject({
            method: 'notifications/subscriptions/lifecycle',
            params: { type: 'reauthorization_required' }
        });
        await advance(500);
        expect(await stream.next()).toMatchObject({
            method: 'notifications/subscriptions/lifecycle',
            params: { type: 'reauthorization_required' }
        });
        expect(h.subscriptions.list()[0]?.state).toBe('paused');
        await h.close();
    });

    it('defect 4: requireScopes does not restart maxAuthorizationLifetimeMs for streams that already satisfy the scope', async () => {
        const h = handler({ maxAuthorizationLifetimeMs: 1_000 });
        const { stream } = await open(h, 1, { resourceSubscriptions: ['file:///a'] }, auth(undefined, ['read']));
        const before = h.subscriptions.list()[0]!;
        await advance(100);
        h.subscriptions.requireScopes(['read'], clock + 10_000);
        expect(h.subscriptions.list()[0]?.authorizedUntil).toBe(before.authorizedUntil);
        expect(h.subscriptions.list()[0]?.lastUpdatedAt).toBe(before.lastUpdatedAt);
        await advance(900);
        h.notify.resourceUpdated('file:///a');
        expect(await stream.next()).toMatchObject({ error: { code: -32_028, data: { reason: 'token_expiry' } } });
        await h.close();
    });

    it('defect 5: update idempotency includes token when tokens lack exp under a max authorization cap', async () => {
        const h = handler({ maxAuthorizationLifetimeMs: 10_000 });
        const { ack } = await open(h, 1, { resourceSubscriptions: ['file:///a'], lifecycle: true }, { ...auth(undefined), token: 'old' });
        await advance(1_000);
        const first = await json(
            await h.fetch(updateRequest(2, ack.params?.streamId as string), { authInfo: { ...auth(undefined), token: 'new-1' } })
        );
        await advance(1_000);
        const second = await json(
            await h.fetch(updateRequest(3, ack.params?.streamId as string), { authInfo: { ...auth(undefined), token: 'new-2' } })
        );
        expect(second.result?.authorizedUntil).not.toBe(first.result?.authorizedUntil);
        expect(second.result?.authorizedUntil).toBe(iso(clock + 10_000));
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

    it('defect 8: update treats throwing authorize as unavailable and does not return 500 or remove entries', async () => {
        const h = handler({
            authorize: () => {
                throw new Error('acl down');
            }
        });
        const { ack } = await open(h, 1, { resourceSubscriptions: ['file:///a'], lifecycle: true });
        const response = await h.fetch(updateRequest(2, ack.params?.streamId as string, { expiresAt: iso(clock + 30_000) }), {
            authInfo: auth(clock + 60_000)
        });
        expect(response.status).toBe(200);
        expect(await json(response)).toMatchObject({ result: { expiresAt: iso(clock + 30_000) } });
        expect(h.subscriptions.list()[0]?.acknowledged.resourceSubscriptions).toEqual(['file:///a']);
        await h.close();
    });

    it('defect 8: queued update re-checks ended state after awaits and reports unknown stream ID after revoke', async () => {
        const pending = new Deferred<AccessDecision>();
        let calls = 0;
        const h = handler({ authorize: () => (++calls === 1 ? 'allow' : pending.promise) });
        const { ack } = await open(h, 1, { resourceSubscriptions: ['file:///a'], lifecycle: true });
        const updatePromise = h.fetch(updateRequest(2, ack.params?.streamId as string), { authInfo: auth(clock + 60_000) });
        h.subscriptions.revoke();
        pending.resolve('allow');
        expect(await json(await updatePromise)).toMatchObject({ error: { code: -32_602, message: 'Unknown stream ID' } });
        await h.close();
    });

    it('defect 9: recheckAccess sends one access_reduced notification for multiple removed entries', async () => {
        let deny = false;
        const h = handler({ authorize: (_auth, target) => (target.kind === 'resource' && deny ? 'deny' : 'allow') });
        const { stream } = await open(h, 1, { resourceSubscriptions: ['file:///a', 'file:///b'], lifecycle: true });
        deny = true;
        await h.subscriptions.recheckAccess();
        expect(await stream.next()).toMatchObject({
            method: 'notifications/subscriptions/lifecycle',
            params: { type: 'access_reduced', removed: { resourceSubscriptions: ['file:///a', 'file:///b'] } }
        });
        expect(await stream.next()).toMatchObject({ error: { code: -32_028, data: { reason: 'revoked' } } });
        await h.close();
    });

    it('defect 10: subscriptions/update is not refused when access checks are unavailable', async () => {
        const h = handler({ authorize: () => 'unavailable' });
        const { ack } = await open(h, 1, { resourceSubscriptions: ['file:///a'], lifecycle: true });
        const response = await h.fetch(updateRequest(2, ack.params?.streamId as string), { authInfo: auth(clock + 60_000) });
        expect(response.status).toBe(200);
        expect(await json(response)).toMatchObject({ result: { authorizedUntil: iso(clock + 60_000) } });
        await h.close();
    });

    it('defect 11: no streamId is issued when the authorization deadline is unknown', async () => {
        const h = handler();
        const { ack } = await open(h, 1, { resourceSubscriptions: ['file:///a'], lifecycle: true }, auth(undefined));
        expect(ack.params).not.toHaveProperty('streamId');
        await h.close();
    });

    it('defect 12: no streamId is issued when the token lacks a subject', async () => {
        const h = handler();
        const noSubject: AuthInfo = { token: 't', clientId: 'client-a', scopes: ['read'], expiresAt: (clock + 60_000) / 1000 };
        const { ack } = await open(h, 1, { resourceSubscriptions: ['file:///a'], lifecycle: true }, noSubject);
        expect(ack.params).not.toHaveProperty('streamId');
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

    it('lifecycle SEP §4 rule 5 and §3.3 rule 3: resumed stream delivers past the old expiry, reminds before the new deadline, and pauses again', async () => {
        const h = handler({ reminderLeadsMs: () => [500] });
        const { stream, ack } = await open(h, 1, { resourceSubscriptions: ['file:///a'], lifecycle: true }, auth(clock + 1_000));
        await advance(1_000);
        await stream.next();
        await stream.next();
        await h.fetch(updateRequest(2, ack.params?.streamId as string), { authInfo: auth(clock + 2_000) });
        h.notify.resourceUpdated('file:///a');
        expect(await stream.next()).toMatchObject({ method: 'notifications/resources/updated', params: { uri: 'file:///a' } });
        await advance(1_500);
        expect(await stream.next()).toMatchObject({
            method: 'notifications/subscriptions/lifecycle',
            params: { type: 'reauthorization_required' }
        });
        await advance(500);
        expect(await stream.next()).toMatchObject({
            method: 'notifications/subscriptions/lifecycle',
            params: { type: 'reauthorization_required' }
        });
        expect(h.subscriptions.list()[0]?.state).toBe('paused');
        await h.close();
    });

    it('lifecycle SEP §3.4 rule 1: other resources keep delivering after one lifecycle entry is removed', async () => {
        let denyB = false;
        const h = handler({
            authorize: (_auth, target) => (target.kind === 'resource' && target.uri === 'file:///b' && denyB ? 'deny' : 'allow')
        });
        const { stream } = await open(h, 1, { resourceSubscriptions: ['file:///a', 'file:///b'], lifecycle: true }, auth(clock + 1_000));
        denyB = true;
        await h.subscriptions.recheckAccess();
        expect(await stream.next()).toMatchObject({ method: 'notifications/subscriptions/lifecycle', params: { type: 'access_reduced' } });
        h.notify.resourceUpdated('file:///a');
        expect(await stream.next()).toMatchObject({ method: 'notifications/resources/updated', params: { uri: 'file:///a' } });
        await h.close();
    });

    it('async authorize promises are awaited during recheckAccess', async () => {
        const pending = new Deferred<AccessDecision>();
        let calls = 0;
        const h = handler({ authorize: () => (++calls === 1 ? 'allow' : pending.promise) });
        const { stream } = await open(h, 1, { resourceSubscriptions: ['file:///a'], lifecycle: true });
        const recheck = h.subscriptions.recheckAccess();
        await stream.expectNoMessage();
        pending.resolve('deny');
        await recheck;
        expect(await stream.next()).toMatchObject({ method: 'notifications/subscriptions/lifecycle', params: { type: 'access_reduced' } });
        await h.close();
    });

    it('async authorize promises are awaited during held replay', async () => {
        const replay = new Deferred<AccessDecision>();
        let authorizeCalls = 0;
        const h = handler({ reminderLeadsMs: () => [], authorize: () => (++authorizeCalls >= 3 ? replay.promise : 'allow') });
        const { stream, ack } = await open(h, 1, { resourceSubscriptions: ['file:///a'], lifecycle: true }, auth(clock + 1_000));
        await advance(1_000);
        await stream.next();
        h.notify.resourceUpdated('file:///a');
        await h.fetch(updateRequest(2, ack.params?.streamId as string), { authInfo: auth(clock + 60_000) });
        await stream.expectNoMessage();
        replay.resolve('allow');
        expect(await stream.next()).toMatchObject({ method: 'notifications/resources/updated', params: { uri: 'file:///a' } });
        await h.close();
    });
});
