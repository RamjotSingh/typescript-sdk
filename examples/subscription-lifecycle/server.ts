/**
 * Subscription Lifecycle prototype demo server.
 *
 * One Node process hosts a toy authorization server (`POST /token`) and an
 * MCP resource server (`/mcp`). The resource server verifies HS256 JWT bearer
 * tokens, passes `AuthInfo` to `createMcpHandler`, and scripts lifecycle-SEP
 * access/policy changes against the prototype `handler.subscriptions` API.
 */
import { createSecretKey, randomBytes } from 'node:crypto';
import { createServer } from 'node:http';

import { parseExampleArgs } from '@mcp-examples/shared';
import { toNodeHandler } from '@modelcontextprotocol/node';
import type { AuthInfo, McpHttpHandler } from '@modelcontextprotocol/server';
import {
    createMcpHandler,
    hostHeaderValidationResponse,
    localhostAllowedHostnames,
    localhostAllowedOrigins,
    McpServer,
    OAuthError,
    OAuthErrorCode,
    originValidationResponse
} from '@modelcontextprotocol/server';
import { jwtVerify, SignJWT } from 'jose';
import * as z from 'zod/v4';

const PROJECT_URI = 'file:///project/config.json';
const HR_URI = 'file:///hr/case-114.md';
const CLIENT_ID = 'demo-client';
const SUBJECT = 'user-114';
const DEFAULT_SCOPE = 'files:read';
const AUDIT_SCOPE = 'files:audit';
const TOKEN_LIFETIME_SECONDS = Number(process.env.DEMO_ACCESS_TOKEN_SECONDS ?? '120');
const TOKEN_LIFETIME_MS = TOKEN_LIFETIME_SECONDS * 1000;
const SECRET = createSecretKey(Buffer.from('subscription-lifetime-demo-secret-key-32b'));

interface RefreshRecord {
    clientId: string;
    sub: string;
    scopes: string[];
}

const refreshTokens = new Map<string, RefreshRecord>();
let projectVersion = 0;
let hrVersion = 0;
let hrAccessRevoked = false;
let scriptStarted = false;
let sawPaused = false;
let policyRaised = false;
let auditRequiredAtMs: number | undefined;

function scopesOf(raw: string | null): string[] {
    const scopes = (raw ?? DEFAULT_SCOPE).split(/\s+/).filter(Boolean);
    return scopes.length === 0 ? [DEFAULT_SCOPE] : [...new Set(scopes)];
}

function formBody(body: string): URLSearchParams {
    return new URLSearchParams(body);
}

async function readBody(request: Request): Promise<string> {
    return await request.text();
}

function json(status: number, body: unknown, headers?: Record<string, string>): Response {
    return Response.json(body, { status, headers });
}

async function mintTokens(scopes: string[], clientId = CLIENT_ID, sub = SUBJECT): Promise<Response> {
    const now = Math.floor(Date.now() / 1000);
    const token = await new SignJWT({ scope: scopes.join(' '), client_id: clientId })
        .setProtectedHeader({ alg: 'HS256', typ: 'JWT' })
        .setSubject(sub)
        .setIssuedAt(now)
        .setExpirationTime(now + TOKEN_LIFETIME_SECONDS)
        .sign(SECRET);
    const refreshToken = `rt_${randomBytes(18).toString('base64url')}`;
    refreshTokens.set(refreshToken, { clientId, sub, scopes });
    if (scopes.includes(AUDIT_SCOPE)) {
        console.error('[auth-server] user consented to files:audit for demo step-up');
    }
    return json(200, {
        access_token: token,
        token_type: 'Bearer',
        expires_in: TOKEN_LIFETIME_SECONDS,
        scope: scopes.join(' '),
        refresh_token: refreshToken
    });
}

async function tokenEndpoint(request: Request): Promise<Response> {
    if (request.method !== 'POST') return json(405, { error: 'method_not_allowed' });
    const params = formBody(await readBody(request));
    const grantType = params.get('grant_type');
    if (grantType === 'client_credentials') {
        const scopes = scopesOf(params.get('scope'));
        if (!scopes.includes(DEFAULT_SCOPE)) scopes.unshift(DEFAULT_SCOPE);
        return mintTokens(scopes, params.get('client_id') ?? CLIENT_ID);
    }
    if (grantType === 'refresh_token') {
        const old = params.get('refresh_token') ?? '';
        const record = refreshTokens.get(old);
        if (record === undefined) return json(400, { error: 'invalid_grant' });
        refreshTokens.delete(old);
        return mintTokens(record.scopes, record.clientId, record.sub);
    }
    return json(400, { error: 'unsupported_grant_type' });
}

async function verifyBearer(request: Request): Promise<AuthInfo | Response> {
    const header = request.headers.get('authorization');
    if (!header?.startsWith('Bearer ')) {
        return bearerChallenge('invalid_token', 'missing bearer token');
    }
    const token = header.slice('Bearer '.length);
    try {
        const verified = await jwtVerify(token, SECRET, { algorithms: ['HS256'] });
        const payload = verified.payload;
        const scope = typeof payload.scope === 'string' ? payload.scope : '';
        const clientId = typeof payload.client_id === 'string' ? payload.client_id : CLIENT_ID;
        const sub = typeof payload.sub === 'string' ? payload.sub : SUBJECT;
        if (payload.exp === undefined || payload.exp <= Math.floor(Date.now() / 1000)) {
            throw new OAuthError(OAuthErrorCode.InvalidToken, 'expired token');
        }
        const scopes = scope.split(/\s+/).filter(Boolean);
        if (sub === SUBJECT && auditRequiredAtMs !== undefined && Date.now() >= auditRequiredAtMs && !scopes.includes(AUDIT_SCOPE)) {
            return insufficientScopeChallenge();
        }
        return { token, clientId, scopes, expiresAt: payload.exp, extra: { sub } };
    } catch {
        return bearerChallenge('invalid_token', 'invalid or expired token');
    }
}

function bearerChallenge(error: string, description: string): Response {
    return json(
        401,
        { error, error_description: description },
        { 'WWW-Authenticate': `Bearer error="${error}", error_description="${description}"` }
    );
}

function insufficientScopeChallenge(): Response {
    return json(
        403,
        { error: 'insufficient_scope' },
        { 'WWW-Authenticate': `Bearer error="insufficient_scope", scope="${DEFAULT_SCOPE} ${AUDIT_SCOPE}"` }
    );
}

function buildServer(): McpServer {
    const server = new McpServer(
        { name: 'subscription-lifecycle-demo', version: '1.0.0' },
        { capabilities: { resources: { subscribe: true } } }
    );
    server.registerResource('project-config', PROJECT_URI, { title: 'Project config', mimeType: 'application/json' }, async uri => ({
        contents: [{ uri: uri.href, mimeType: 'application/json', text: JSON.stringify({ projectVersion }) }]
    }));
    server.registerResource('hr-case-114', HR_URI, { title: 'HR case 114', mimeType: 'text/markdown' }, async uri => ({
        contents: [{ uri: uri.href, mimeType: 'text/markdown', text: `# HR Case 114\n\nRevision ${hrVersion}\n` }]
    }));
    server.registerTool('demo-status', { description: 'Returns scripted demo state.', inputSchema: z.object({}) }, async () => ({
        content: [
            {
                type: 'text',
                text: JSON.stringify({
                    projectVersion,
                    hrVersion,
                    hrAccessRevoked,
                    auditRequiredAtMs,
                    streams: handler.subscriptions.list()
                })
            }
        ]
    }));
    return server;
}

const handler: McpHttpHandler = createMcpHandler(buildServer, {
    keepAliveMs: 5000,
    subscriptionLifetime: {
        authorize: (_authInfo, target) => (target.kind === 'resource' && target.uri === HR_URI && hrAccessRevoked ? 'deny' : 'allow')
    },
    subscriptionLifecycle: {
        maxStreamLifetimeMs: TOKEN_LIFETIME_MS * 8
    }
});

/**
 * The same server without either proposal, at `/mcp-current`, run as today's SDK runs it: requests are
 * checked against their token, but the handler is not given the token, so streams are not tied to it.
 */
const currentHandler: McpHttpHandler = createMcpHandler(buildServer, { keepAliveMs: 5000 });

function resourceChanged(uri: string): void {
    handler.notify.resourceUpdated(uri);
    currentHandler.notify.resourceUpdated(uri);
}

function startSharedResourceChanges(): void {
    setInterval(
        () => {
            projectVersion++;
            resourceChanged(PROJECT_URI);
        },
        Math.max(1500, Math.floor(TOKEN_LIFETIME_MS * 0.12))
    );

    setInterval(
        () => {
            hrVersion++;
            resourceChanged(HR_URI);
        },
        Math.max(1800, Math.floor(TOKEN_LIFETIME_MS * 0.15))
    );
}

function startLifecycleScriptedEvents(): void {
    console.error(`[demo] scripted lifecycle demo started; token lifetime ${TOKEN_LIFETIME_SECONDS}s`);
    startSharedResourceChanges();

    setTimeout(
        () => {
            hrAccessRevoked = true;
            console.error('[demo] HR case access revoked; next HR update records a pending access_reduced notice');
            hrVersion++;
            resourceChanged(HR_URI);
        },
        Math.max(5000, Math.floor(TOKEN_LIFETIME_MS * 1.25))
    );

    setInterval(() => {
        const streams = handler.subscriptions.list();
        if (streams.some(s => s.state === 'paused')) sawPaused = true;
        if (sawPaused && !policyRaised && streams.some(s => s.state === 'running')) {
            policyRaised = true;
            const effectiveAtMs = Date.now() + 1500;
            auditRequiredAtMs = effectiveAtMs;
            console.error('[demo] policy now requires files:audit; subscriptions/update should step up');
            handler.subscriptions.requireScopes([AUDIT_SCOPE], effectiveAtMs);
        }
    }, 500);
}

function startScriptedEvents(): void {
    if (scriptStarted) return;
    scriptStarted = true;
    startLifecycleScriptedEvents();
}

const { port } = parseExampleArgs();
const nodeHandler = toNodeHandler({
    fetch: async request => {
        const url = new URL(request.url);
        if (url.pathname === '/token') return tokenEndpoint(request);
        if (url.pathname !== '/mcp' && url.pathname !== '/mcp-current') return json(404, { error: 'not_found' });
        const rejected =
            hostHeaderValidationResponse(request, localhostAllowedHostnames()) ??
            originValidationResponse(request, localhostAllowedOrigins());
        if (rejected) return rejected;
        const authInfo = await verifyBearer(request);
        if (authInfo instanceof Response) return authInfo;
        if (url.pathname === '/mcp-current') return currentHandler.fetch(request);
        const response = await handler.fetch(request, { authInfo });
        queueMicrotask(() => {
            if (handler.subscriptions.list().length > 0) startScriptedEvents();
        });
        return response;
    }
});

createServer(nodeHandler).listen(port, '127.0.0.1', () => {
    console.error(`[server] MCP on http://127.0.0.1:${port}/mcp, and without the proposals on http://127.0.0.1:${port}/mcp-current`);
    console.error(`[auth-server] token endpoint on http://127.0.0.1:${port}/token`);
});
