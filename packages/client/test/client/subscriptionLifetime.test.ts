import type { JSONRPCMessage } from '@modelcontextprotocol/core-internal';
import {
    AUTHORIZATION_ENDED,
    InMemoryTransport,
    PROTOCOL_VERSION_META_KEY,
    SdkError,
    SUBSCRIPTION_ID_META_KEY
} from '@modelcontextprotocol/core-internal';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { Client } from '../../src/client/client';
import { StreamableHTTPClientTransport } from '../../src/client/streamableHttp';

const MODERN = '2026-07-28';
const flush = () => new Promise(resolve => setTimeout(resolve, 10));

async function scriptedModern(onMessage?: (message: JSONRPCMessage, send: (message: JSONRPCMessage) => void) => boolean | void) {
    const [clientTx, serverTx] = InMemoryTransport.createLinkedPair();
    const written: JSONRPCMessage[] = [];
    const send = (message: JSONRPCMessage): void => void serverTx.send(message);
    serverTx.onmessage = message => {
        written.push(message);
        if (onMessage?.(message, send)) return;
        const req = message as { id?: number | string; method?: string; params?: { notifications?: unknown } };
        if (req.method === 'server/discover' && req.id !== undefined) {
            send({
                jsonrpc: '2.0',
                id: req.id,
                result: {
                    resultType: 'complete',
                    supportedVersions: [MODERN],
                    capabilities: { tools: { listChanged: true }, resources: { listChanged: true } },
                    _meta: { 'io.modelcontextprotocol/serverInfo': { name: 'scripted', version: '1' } }
                }
            });
        }
        if (req.method === 'subscriptions/listen' && req.id !== undefined) {
            send({
                jsonrpc: '2.0',
                method: 'notifications/subscriptions/acknowledged',
                params: { _meta: { [SUBSCRIPTION_ID_META_KEY]: req.id }, notifications: req.params?.notifications ?? {} }
            });
        }
    };
    await serverTx.start();
    return { clientTx, serverTx, written, send };
}

async function modernClient(clientTx: InMemoryTransport): Promise<Client> {
    const client = new Client({ name: 'c', version: '1' }, { versionNegotiation: { mode: 'auto' } });
    await client.connect(clientTx);
    return client;
}

function listenIds(written: JSONRPCMessage[]): string[] {
    return written
        .filter(message => (message as { method?: string }).method === 'subscriptions/listen')
        .map(message => (message as { id: string }).id);
}

describe('Client subscription lifetime prototype', () => {
    afterEach(() => {
        vi.restoreAllMocks();
    });

    it('lifecycle SEP §2 client rule: expiresAt is sent in subscriptions/listen params', async () => {
        const { clientTx, written } = await scriptedModern();
        const client = await modernClient(clientTx);
        const expiresAt = '2026-09-29T12:00:00.000Z';
        const sub = await client.listen({ toolsListChanged: true }, { expiresAt });

        const listen = written.find(message => (message as { method?: string }).method === 'subscriptions/listen') as {
            params: { expiresAt?: string };
        };
        expect(listen.params.expiresAt).toBe(expiresAt);
        await sub.close();
        await client.close();
    });

    it('lifetime SEP §4: authorizedUntil from the acknowledgment is exposed on McpSubscription', async () => {
        const authorizedUntil = '2026-09-29T11:00:00.000Z';
        const { clientTx } = await scriptedModern((message, send) => {
            const req = message as { id?: number | string; method?: string; params?: { notifications?: unknown } };
            if (req.method !== 'subscriptions/listen' || req.id === undefined) return;
            send({
                jsonrpc: '2.0',
                method: 'notifications/subscriptions/acknowledged',
                params: {
                    _meta: { [SUBSCRIPTION_ID_META_KEY]: req.id },
                    notifications: req.params?.notifications ?? {},
                    authorizedUntil
                }
            });
            return true;
        });
        const client = await modernClient(clientTx);
        const sub = await client.listen({ toolsListChanged: true });

        expect(sub.authorizedUntil).toBe(authorizedUntil);
        await sub.close();
        await client.close();
    });

    it('lifetime SEP §4 and lifecycle SEP §4 rule 1: ack fields are exposed on McpSubscription', async () => {
        const ack = {
            authorizedUntil: '2026-09-29T11:00:00.000Z',
            expiresAt: '2026-09-29T12:00:00.000Z',
            streamId: 'sub_ack',
            lastUpdatedAt: '2026-09-29T10:00:00.000Z'
        };
        const { clientTx } = await scriptedModern((message, send) => {
            const req = message as { id?: number | string; method?: string; params?: { notifications?: unknown } };
            if (req.method !== 'subscriptions/listen' || req.id === undefined) return;
            send({
                jsonrpc: '2.0',
                method: 'notifications/subscriptions/acknowledged',
                params: {
                    _meta: { [SUBSCRIPTION_ID_META_KEY]: req.id },
                    notifications: req.params?.notifications ?? {},
                    ...ack
                }
            });
            return true;
        });
        const client = await modernClient(clientTx);
        const sub = await client.listen({ toolsListChanged: true });

        expect(sub.authorizedUntil).toBe(ack.authorizedUntil);
        expect(sub.expiresAt).toBe(ack.expiresAt);
        expect(sub.streamId).toBe(ack.streamId);
        expect(sub.lastUpdatedAt).toBe(ack.lastUpdatedAt);
        await sub.close();
        await client.close();
    });

    it('lifecycle SEP §4 rule 2: update() sends subscriptions/update with the streamId and refreshes properties', async () => {
        const updateResult = {
            resultType: 'complete',
            expiresAt: '2026-09-29T13:00:00.000Z',
            authorizedUntil: '2026-09-29T11:30:00.000Z',
            lastUpdatedAt: '2026-09-29T10:30:00.000Z'
        };
        const { clientTx, written } = await scriptedModern((message, send) => {
            const req = message as { id?: number | string; method?: string; params?: Record<string, unknown> };
            if (req.method === 'subscriptions/listen' && req.id !== undefined) {
                send({
                    jsonrpc: '2.0',
                    method: 'notifications/subscriptions/acknowledged',
                    params: {
                        _meta: { [SUBSCRIPTION_ID_META_KEY]: req.id },
                        notifications: req.params?.notifications ?? {},
                        streamId: 'sub_update',
                        authorizedUntil: '2026-09-29T11:00:00.000Z',
                        lastUpdatedAt: '2026-09-29T10:00:00.000Z'
                    }
                });
                return true;
            }
            if (req.method === 'subscriptions/update' && req.id !== undefined) {
                send({ jsonrpc: '2.0', id: req.id, result: updateResult });
                return true;
            }
            return undefined;
        });
        const client = await modernClient(clientTx);
        const sub = await client.listen({ toolsListChanged: true });
        const result = await sub.update({ expiresAt: updateResult.expiresAt });

        const update = written.find(message => (message as { method?: string }).method === 'subscriptions/update') as {
            params: { streamId?: string; expiresAt?: string };
        };
        expect(update.params).toMatchObject({ streamId: 'sub_update', expiresAt: updateResult.expiresAt });
        expect(result).toMatchObject({
            expiresAt: updateResult.expiresAt,
            authorizedUntil: updateResult.authorizedUntil,
            lastUpdatedAt: updateResult.lastUpdatedAt
        });
        expect(sub.expiresAt).toBe(updateResult.expiresAt);
        expect(sub.authorizedUntil).toBe(updateResult.authorizedUntil);
        expect(sub.lastUpdatedAt).toBe(updateResult.lastUpdatedAt);
        await sub.close();
        await client.close();
    });

    it('lifecycle SEP §4 rule 2: Streamable HTTP emits Mcp-Name from subscriptions/update streamId', async () => {
        vi.spyOn(globalThis, 'fetch').mockResolvedValueOnce({ ok: true, status: 202, headers: new Headers() } as Response);
        const transport = new StreamableHTTPClientTransport(new URL('http://localhost:1234/mcp'));
        await transport.send({
            jsonrpc: '2.0',
            id: 1,
            method: 'subscriptions/update',
            params: { _meta: { [PROTOCOL_VERSION_META_KEY]: MODERN }, streamId: 'sub_http' }
        });

        const headers = (globalThis.fetch as unknown as ReturnType<typeof vi.fn>).mock.calls[0]![1].headers as Headers;
        expect(headers.get('mcp-method')).toBe('subscriptions/update');
        expect(headers.get('mcp-name')).toBe('sub_http');
        await transport.close().catch(() => {});
    });

    it('lifecycle SEP §4 client rule 1: update() rejects clearly without a streamId', async () => {
        const { clientTx } = await scriptedModern();
        const client = await modernClient(clientTx);
        const sub = await client.listen({ toolsListChanged: true });

        const error = await sub.update().catch(e => e as SdkError);
        expect(error).toBeInstanceOf(SdkError);
        expect(error.message).toContain('streamId');
        await sub.close();
        await client.close();
    });

    it('lifecycle SEP §3.2: lifecycle notifications route to the matching subscription onlifecycle', async () => {
        const { clientTx, written, send } = await scriptedModern();
        const client = await modernClient(clientTx);
        const [a, b] = await Promise.all([client.listen({ toolsListChanged: true }), client.listen({ resourcesListChanged: true })]);
        const [aId, bId] = listenIds(written);
        const seenA: string[] = [];
        const seenB: string[] = [];
        a.onlifecycle = notification => seenA.push(notification.params.type);
        b.onlifecycle = notification => seenB.push(notification.params.type);

        send({
            jsonrpc: '2.0',
            method: 'notifications/subscriptions/lifecycle',
            params: { _meta: { [SUBSCRIPTION_ID_META_KEY]: bId }, type: 'missed', lastUpdatedAt: '2026-09-29T10:01:00.000Z' }
        });
        send({
            jsonrpc: '2.0',
            method: 'notifications/subscriptions/lifecycle',
            params: {
                _meta: { [SUBSCRIPTION_ID_META_KEY]: aId },
                type: 'reauthorization_required',
                authorizedUntil: '2026-09-29T11:00:00.000Z',
                reason: 'token_expiry',
                lastUpdatedAt: '2026-09-29T10:02:00.000Z'
            }
        });
        await flush();

        expect(seenA).toEqual(['reauthorization_required']);
        expect(seenB).toEqual(['missed']);
        expect(a.authorizedUntil).toBe('2026-09-29T11:00:00.000Z');
        await a.close();
        await b.close();
        await client.close();
    });

    it('lifecycle SEP §3.2 rule 2: stale reminders after an update result are dropped', async () => {
        const { clientTx, send, written } = await scriptedModern((message, sendMessage) => {
            const req = message as { id?: number | string; method?: string; params?: Record<string, unknown> };
            if (req.method === 'subscriptions/listen' && req.id !== undefined) {
                sendMessage({
                    jsonrpc: '2.0',
                    method: 'notifications/subscriptions/acknowledged',
                    params: {
                        _meta: { [SUBSCRIPTION_ID_META_KEY]: req.id },
                        notifications: req.params?.notifications ?? {},
                        streamId: 'sub_stale',
                        authorizedUntil: '2026-09-29T11:00:00.000Z',
                        lastUpdatedAt: '2026-09-29T10:00:00.000Z'
                    }
                });
                return true;
            }
            if (req.method === 'subscriptions/update' && req.id !== undefined) {
                sendMessage({
                    jsonrpc: '2.0',
                    id: req.id,
                    result: {
                        resultType: 'complete',
                        authorizedUntil: '2026-09-29T11:30:00.000Z',
                        lastUpdatedAt: '2026-09-29T10:30:00.000Z'
                    }
                });
                return true;
            }
            return undefined;
        });
        const client = await modernClient(clientTx);
        const sub = await client.listen({ toolsListChanged: true });
        const seen: string[] = [];
        sub.onlifecycle = notification => seen.push(notification.params.type);
        await sub.update();
        const [id] = listenIds(written);

        send({
            jsonrpc: '2.0',
            method: 'notifications/subscriptions/lifecycle',
            params: {
                _meta: { [SUBSCRIPTION_ID_META_KEY]: id },
                type: 'reauthorization_required',
                authorizedUntil: '2026-09-29T11:05:00.000Z',
                reason: 'token_expiry',
                lastUpdatedAt: '2026-09-29T10:15:00.000Z'
            }
        });
        await flush();

        expect(seen).toEqual([]);
        expect(sub.authorizedUntil).toBe('2026-09-29T11:30:00.000Z');
        await sub.close();
        await client.close();
    });

    it('lifecycle SEP §3.2 rule 2: access_reduced and missed older than an update result are still delivered', async () => {
        const { clientTx, send, written } = await scriptedModern((message, sendMessage) => {
            const req = message as { id?: number | string; method?: string; params?: Record<string, unknown> };
            if (req.method === 'subscriptions/listen' && req.id !== undefined) {
                sendMessage({
                    jsonrpc: '2.0',
                    method: 'notifications/subscriptions/acknowledged',
                    params: {
                        _meta: { [SUBSCRIPTION_ID_META_KEY]: req.id },
                        notifications: req.params?.notifications ?? {},
                        streamId: 'sub_events',
                        authorizedUntil: '2026-09-29T11:00:00.000Z',
                        lastUpdatedAt: '2026-09-29T10:00:00.000Z'
                    }
                });
                return true;
            }
            if (req.method === 'subscriptions/update' && req.id !== undefined) {
                sendMessage({
                    jsonrpc: '2.0',
                    id: req.id,
                    result: {
                        resultType: 'complete',
                        authorizedUntil: '2026-09-29T11:30:00.000Z',
                        lastUpdatedAt: '2026-09-29T10:30:00.000Z'
                    }
                });
                return true;
            }
            return undefined;
        });
        const client = await modernClient(clientTx);
        const sub = await client.listen({ resourceSubscriptions: ['file:///a.md', 'file:///b.md'] });
        const seen: string[] = [];
        sub.onlifecycle = notification => seen.push(notification.params.type);
        await sub.update();
        const [id] = listenIds(written);

        // Both were sent before the update and overtaken by its result.
        send({
            jsonrpc: '2.0',
            method: 'notifications/subscriptions/lifecycle',
            params: {
                _meta: { [SUBSCRIPTION_ID_META_KEY]: id },
                type: 'access_reduced',
                removed: { resourceSubscriptions: ['file:///b.md'] },
                lastUpdatedAt: '2026-09-29T10:20:00.000Z'
            }
        });
        send({
            jsonrpc: '2.0',
            method: 'notifications/subscriptions/lifecycle',
            params: { _meta: { [SUBSCRIPTION_ID_META_KEY]: id }, type: 'missed', lastUpdatedAt: '2026-09-29T10:20:00.000Z' }
        });
        await flush();

        expect(seen).toEqual(['access_reduced', 'missed']);
        expect(sub.lastUpdatedAt).toBe('2026-09-29T10:30:00.000Z');
        expect(sub.authorizedUntil).toBe('2026-09-29T11:30:00.000Z');
        await sub.close();
        await client.close();
    });

    it('lifecycle SEP §3.2 rule 2: an update result older than a reminder keeps the reminder deadline', async () => {
        let listenId: number | string | undefined;
        const { clientTx } = await scriptedModern((message, sendMessage) => {
            const req = message as { id?: number | string; method?: string; params?: Record<string, unknown> };
            if (req.method === 'subscriptions/listen' && req.id !== undefined) {
                listenId = req.id;
                sendMessage({
                    jsonrpc: '2.0',
                    method: 'notifications/subscriptions/acknowledged',
                    params: {
                        _meta: { [SUBSCRIPTION_ID_META_KEY]: req.id },
                        notifications: req.params?.notifications ?? {},
                        streamId: 'sub_newer',
                        authorizedUntil: '2026-09-29T11:00:00.000Z',
                        lastUpdatedAt: '2026-09-29T10:00:00.000Z'
                    }
                });
                return true;
            }
            if (req.method === 'subscriptions/update' && req.id !== undefined) {
                // The server applies the update, then a new requirement moves the
                // deadline earlier; that reminder overtakes the update result.
                sendMessage({
                    jsonrpc: '2.0',
                    method: 'notifications/subscriptions/lifecycle',
                    params: {
                        _meta: { [SUBSCRIPTION_ID_META_KEY]: listenId },
                        type: 'reauthorization_required',
                        authorizedUntil: '2026-09-29T10:45:00.000Z',
                        reason: 'insufficient_authorization',
                        lastUpdatedAt: '2026-09-29T10:30:00.500Z'
                    }
                });
                sendMessage({
                    jsonrpc: '2.0',
                    id: req.id,
                    result: {
                        resultType: 'complete',
                        authorizedUntil: '2026-09-29T11:30:00.000Z',
                        lastUpdatedAt: '2026-09-29T10:30:00.000Z'
                    }
                });
                return true;
            }
            return undefined;
        });
        const client = await modernClient(clientTx);
        const sub = await client.listen({ toolsListChanged: true });
        const seen: string[] = [];
        sub.onlifecycle = notification => seen.push(notification.params.type);
        const result = await sub.update();

        expect(result.authorizedUntil).toBe('2026-09-29T11:30:00.000Z');
        expect(seen).toEqual(['reauthorization_required']);
        expect(sub.authorizedUntil).toBe('2026-09-29T10:45:00.000Z');
        expect(sub.lastUpdatedAt).toBe('2026-09-29T10:30:00.500Z');
        await sub.close();
        await client.close();
    });

    it('lifecycle SEP §3.2 rule 1: unknown lifecycle types are passed through untouched', async () => {
        const { clientTx, send, written } = await scriptedModern();
        const client = await modernClient(clientTx);
        const sub = await client.listen({ toolsListChanged: true });
        const seen: unknown[] = [];
        sub.onlifecycle = notification => seen.push(notification);
        const [id] = listenIds(written);

        send({
            jsonrpc: '2.0',
            method: 'notifications/subscriptions/lifecycle',
            params: {
                _meta: { [SUBSCRIPTION_ID_META_KEY]: id },
                type: 'com.example/quota_warning',
                lastUpdatedAt: '2026-09-29T10:01:00.000Z'
            }
        });
        await flush();

        expect(seen).toHaveLength(1);
        expect((seen[0] as { params?: unknown }).params).toMatchObject({
            type: 'com.example/quota_warning',
            lastUpdatedAt: '2026-09-29T10:01:00.000Z'
        });
        await sub.close();
        await client.close();
    });

    it("lifetime SEP §5: AuthorizationEnded sets endReason and closes with 'remote'", async () => {
        const { clientTx, send, written } = await scriptedModern();
        const client = await modernClient(clientTx);
        const sub = await client.listen({ toolsListChanged: true });
        const [id] = listenIds(written);

        send({
            jsonrpc: '2.0',
            id,
            error: { code: AUTHORIZATION_ENDED, message: 'Authorization ended', data: { reason: 'token_expiry' } }
        });

        await expect(sub.closed).resolves.toBe('remote');
        expect(sub.endReason).toBe('token_expiry');
        await client.close();
    });

    it('backward compatibility: subscriptions without lifetime fields still dispatch notifications and close locally', async () => {
        const { clientTx, send, written } = await scriptedModern();
        const client = await modernClient(clientTx);
        const seen: string[] = [];
        client.setNotificationHandler('notifications/tools/list_changed', () => {
            seen.push('tools');
        });
        const sub = await client.listen({ toolsListChanged: true });
        const [id] = listenIds(written);

        send({
            jsonrpc: '2.0',
            method: 'notifications/tools/list_changed',
            params: { _meta: { [SUBSCRIPTION_ID_META_KEY]: id } }
        });
        await flush();
        await sub.close();

        expect(sub.authorizedUntil).toBeUndefined();
        expect(sub.endReason).toBeUndefined();
        expect(seen).toEqual(['tools']);
        await expect(sub.closed).resolves.toBe('local');
        await client.close();
    });
});
