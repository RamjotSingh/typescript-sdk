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
