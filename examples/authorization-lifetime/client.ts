/**
 * Self-verifying client for the Authorization Lifetime for Subscription Streams SEP.
 *
 * It runs three scenarios at once, each a pairing of a client and an MCP server, with or without the
 * proposal: a current client with the current server at `/mcp-current` (scenarios 1 and 2), a
 * proposed client with the proposed server at `/mcp` (scenario 3), and a current client with the
 * proposed server (scenario 4). The proposed client declares a draft capability that stands in for
 * the new protocol version. Every exchange is recorded in a wire log, written when the demo ends.
 */
import { performance } from 'node:perf_hooks';

import { check, parseExampleArgs } from '@mcp-examples/shared';
import type { FetchLike, McpSubscription, SubscriptionFilter } from '@modelcontextprotocol/client';
import {
    Client,
    InsufficientScopeError,
    ProtocolError,
    ProtocolErrorCode,
    StreamableHTTPClientTransport,
    UnauthorizedError
} from '@modelcontextprotocol/client';

import type { Story } from './wireLog';
import { CURRENT_SERVER_PATH, WireLog, wireLogPath } from './wireLog';

const PROJECT_URI = 'file:///project/config.json';
const HR_URI = 'file:///hr/case-114.md';
const ACCESS_TOKEN_SECONDS = Number(process.env.DEMO_ACCESS_TOKEN_SECONDS ?? '120');
const MCP_SERVER_NAME = 'authorization-lifetime-demo';
const AUTH_SERVER_NAME = 'toy HS256 authorization server';
const DRAFT_CAPABILITIES = { experimental: { 'io.modelcontextprotocol/subscription-lifetime': {} } };
const start = performance.now();

/** The scenarios, as the wire log groups them. */
const CURRENT_SERVER = 'current-server';
const PROPOSED = 'proposed';
const CURRENT_CLIENT = 'current-client';

interface TokenSet {
    access_token: string;
    refresh_token: string;
    expires_in: number;
    scope: string;
}

interface DemoStatus {
    hrAccessRevoked: boolean;
    auditRequiredAtMs?: number;
}

interface ResourceEvent {
    at: number;
    subId: string;
    uri: string;
}

interface OpenStream {
    name: string;
    subId: string;
    subscription: McpSubscription;
    authorizedUntilMs?: number;
    closedCause?: 'local' | 'graceful' | 'remote';
}

/** A client in one scenario, with the resource notifications it received. */
interface DemoClient {
    client: Client;
    tokens: DemoTokens;
    events: ResourceEvent[];
    listens: number;
}

type Rejection = 'unauthorized' | 'insufficient';

class TokenEndpointError extends Error {
    constructor(
        readonly error: string,
        readonly status: number,
        body: string
    ) {
        super(`token endpoint failed: ${status} ${body}`);
    }
}

class DemoTokens {
    private current?: TokenSet;
    tokenRequests = 0;
    refreshRequests = 0;
    stepUpRequests = 0;

    constructor(
        private readonly tokenUrl: URL,
        private readonly subject: string,
        private readonly clientId: string,
        private readonly fetch: FetchLike
    ) {}

    async start(scopes = 'files:read'): Promise<void> {
        this.current = await this.request(
            new URLSearchParams({ grant_type: 'client_credentials', client_id: this.clientId, subject: this.subject, scope: scopes })
        );
        log(`token(${this.subject}): client_credentials [${this.current.scope}] exp=${this.current.expires_in}s`);
    }

    async refresh(): Promise<void> {
        const refreshToken = this.current?.refresh_token;
        if (refreshToken === undefined) throw new Error('cannot refresh before initial token');
        this.refreshRequests++;
        this.current = await this.request(new URLSearchParams({ grant_type: 'refresh_token', refresh_token: refreshToken }));
        log(`token(${this.subject}): refresh_token [${this.current.scope}] exp=${this.current.expires_in}s`);
    }

    async stepUp(scopes: string): Promise<void> {
        this.stepUpRequests++;
        this.current = await this.request(
            new URLSearchParams({ grant_type: 'client_credentials', client_id: this.clientId, subject: this.subject, scope: scopes })
        );
        log(`token(${this.subject}): stepped up [${this.current.scope}] exp=${this.current.expires_in}s`);
    }

    token(): string {
        if (this.current === undefined) throw new Error('token requested before initialization');
        return this.current.access_token;
    }

    expiresIn(): number {
        if (this.current === undefined) throw new Error('token requested before initialization');
        return this.current.expires_in;
    }

    /** When the current access token expires, from its `exp` claim. */
    expiresAt(): number {
        const claims = JSON.parse(Buffer.from(this.token().split('.')[1] ?? '', 'base64url').toString('utf8')) as { exp?: unknown };
        if (typeof claims.exp !== 'number') throw new Error('access token has no exp claim');
        return claims.exp * 1000;
    }

    private async request(body: URLSearchParams): Promise<TokenSet> {
        this.tokenRequests++;
        const response = await this.fetch(this.tokenUrl, {
            method: 'POST',
            headers: { 'content-type': 'application/x-www-form-urlencoded' },
            body
        });
        if (!response.ok) {
            const text = await response.text();
            let error = 'unknown_error';
            try {
                const parsed = JSON.parse(text) as { error?: unknown };
                if (typeof parsed.error === 'string') error = parsed.error;
            } catch {
                // The raw body is included in the thrown error.
            }
            throw new TokenEndpointError(error, response.status, text);
        }
        return (await response.json()) as TokenSet;
    }
}

function rel(): string {
    return `+${Math.round(performance.now() - start)
        .toString()
        .padStart(5)}ms`;
}

function log(message: string): void {
    console.log(`${rel()} ${message}`);
}

/** Starts a step of a scenario, on the console and in the wire log. */
function step(scenario: string, title: string, text: string): void {
    log(`STEP [${scenario}] ${title}`);
    wire.step(scenario, title, text);
}

async function sleep(ms: number): Promise<void> {
    await new Promise(resolve => setTimeout(resolve, ms));
}

async function waitFor(predicate: () => boolean, description: string, timeoutMs: number): Promise<void> {
    const deadline = performance.now() + timeoutMs;
    while (!predicate()) {
        if (performance.now() > deadline) throw new Error(`timed out waiting for ${description}`);
        await sleep(50);
    }
}

async function waitUntilWallClock(targetMs: number): Promise<void> {
    const delay = targetMs - Date.now();
    if (delay > 0) await sleep(delay);
}

function subIdOf(notification: { params?: { _meta?: Record<string, unknown> } }): string | undefined {
    const id = notification.params?._meta?.['io.modelcontextprotocol/subscriptionId'];
    return typeof id === 'string' ? id : undefined;
}

function isUnauthorized(error: unknown): boolean {
    if (error instanceof UnauthorizedError) return true;
    if (typeof error === 'object' && error !== null && 'data' in error) {
        return isUnauthorized((error as { data?: { cause?: unknown } }).data?.cause);
    }
    return false;
}

function isInsufficientScope(error: unknown): error is InsufficientScopeError {
    if (error instanceof InsufficientScopeError) return true;
    if (typeof error === 'object' && error !== null && 'data' in error) {
        return isInsufficientScope((error as { data?: { cause?: unknown } }).data?.cause);
    }
    return false;
}

/** The entries a refusal of `subscriptions/listen` names as not permitted, in `error.data.denied` (lifetime §3 rule 5). */
function deniedEntries(error: unknown): SubscriptionFilter | undefined {
    if (!(error instanceof ProtocolError) || error.code !== ProtocolErrorCode.InvalidParams) return undefined;
    return (error.data as { denied?: SubscriptionFilter } | undefined)?.denied;
}

function observeClosed(stream: OpenStream): void {
    void stream.subscription.closed.then(cause => {
        stream.closedCause = cause;
        log(`${stream.name}: closed cause=${cause} endReason=${stream.subscription.endReason ?? '(none)'}`);
    });
}

async function readStatus(client: Client): Promise<DemoStatus> {
    const result = await client.callTool({ name: 'demo-status', arguments: {} });
    const text = result.content?.[0]?.type === 'text' ? result.content[0].text : '{}';
    return JSON.parse(text) as DemoStatus;
}

async function resync(client: Client, uris: readonly string[], label: string): Promise<void> {
    for (const uri of uris) {
        await client.readResource({ uri });
    }
    log(`${label}: resynchronized ${uris.join(', ')} (lifetime §5 client rule 2 / §6 rule 4)`);
}

async function expectClosed(stream: OpenStream, reason: string | undefined, timeoutMs: number): Promise<'local' | 'graceful' | 'remote'> {
    let timer: ReturnType<typeof setTimeout> | undefined;
    const timedOut = new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error(`${stream.name} did not close within ${timeoutMs}ms`)), timeoutMs);
    });
    try {
        const cause = await Promise.race([stream.subscription.closed, timedOut]);
        stream.closedCause = cause;
        check.equal(stream.subscription.endReason, reason, `${stream.name} endReason`);
        return cause;
    } finally {
        clearTimeout(timer);
    }
}

async function expectRejected(request: () => Promise<unknown>, expected: Rejection, description: string): Promise<void> {
    try {
        await request();
    } catch (error) {
        if (expected === 'unauthorized' && isUnauthorized(error)) return;
        if (expected === 'insufficient' && isInsufficientScope(error)) return;
        throw error;
    }
    throw new Error(`expected ${description} to be rejected (${expected})`);
}

/** Gets a token for `subject` and connects a client, current or proposed, to `endpoint`. */
async function connect(
    scenario: string,
    options: { subject: string; clientId: string; endpoint: URL; proposed: boolean }
): Promise<DemoClient> {
    const fetch = wire.fetch(scenario, options.proposed ? 'Client (proposed)' : 'Client (current)');
    const tokens = new DemoTokens(tokenUrl, options.subject, options.clientId, fetch);
    await tokens.start();
    const client = new Client(
        { name: options.proposed ? 'proposed-client' : 'current-client', version: '1.0.0' },
        options.proposed
            ? { versionNegotiation: { mode: 'auto' }, capabilities: DRAFT_CAPABILITIES }
            : { versionNegotiation: { mode: 'auto' } }
    );
    await client.connect(
        new StreamableHTTPClientTransport(options.endpoint, {
            fetch,
            authProvider: { token: async () => tokens.token() },
            onInsufficientScope: 'throw'
        })
    );
    const demo: DemoClient = { client, tokens, events: [], listens: 0 };
    client.setNotificationHandler('notifications/resources/updated', notification => {
        const subId = subIdOf(notification);
        const uri = notification.params.uri;
        if (subId !== undefined && typeof uri === 'string') demo.events.push({ at: Date.now(), subId, uri });
    });
    return demo;
}

async function openStream(demo: DemoClient, name: string, filter: SubscriptionFilter): Promise<OpenStream> {
    const subId = `listen:${demo.listens++}`;
    const subscription = await demo.client.listen(filter, { timeout: 15_000 });
    const until = subscription.authorizedUntil;
    const stream: OpenStream = { name, subId, subscription, authorizedUntilMs: until === undefined ? undefined : Date.parse(until) };
    observeClosed(stream);
    log(`${name}: acknowledged as ${subId}, authorizedUntil=${until ?? '(none)'}`);
    return stream;
}

async function openRejected(demo: DemoClient, filter: SubscriptionFilter, expected: Rejection): Promise<void> {
    demo.listens++;
    await expectRejected(() => demo.client.listen(filter, { timeout: 15_000 }), expected, 'subscriptions/listen');
}

/** Opens a stream that the server refuses, and returns the entries the refusal names as not permitted. */
async function openDenied(demo: DemoClient, filter: SubscriptionFilter): Promise<SubscriptionFilter> {
    demo.listens++;
    try {
        await demo.client.listen(filter, { timeout: 15_000 });
    } catch (error) {
        const denied = deniedEntries(error);
        if (denied !== undefined) return denied;
        throw error;
    }
    throw new Error('expected subscriptions/listen to be refused with -32602 and error.data.denied');
}

function deadlineOf(stream: OpenStream): number {
    if (stream.authorizedUntilMs === undefined) throw new Error(`${stream.name} has no authorizedUntil`);
    return stream.authorizedUntilMs;
}

/** Scenarios 1 and 2: a current client with the current server. The stream outlives the token and the user's access. */
async function currentServerScenario(): Promise<void> {
    step(
        CURRENT_SERVER,
        'Step 1: Opening a stream',
        'The client gets a token that is valid for 20 seconds, and opens a stream on `config.json` and `case-114.md` with it.'
    );
    const demo = await connect(CURRENT_SERVER, { subject: 'user-114', clientId: 'today-client', endpoint: currentUrl, proposed: false });
    const stream = await openStream(demo, '[1,2] stream [config, hr]', { resourceSubscriptions: [PROJECT_URI, HR_URI] });
    check.equal(stream.subscription.authorizedUntil, undefined, 'a current server reports no authorizedUntil');
    const expiresAt = demo.tokens.expiresAt();
    await waitUntilWallClock(expiresAt + 1500);

    step(
        CURRENT_SERVER,
        'Step 2: The token expires; the stream does not',
        'The token has expired. The client reads `config.json` with it, and the server refuses the request with `401`, yet the stream the same token opened keeps delivering.'
    );
    await expectRejected(() => demo.client.readResource({ uri: PROJECT_URI }), 'unauthorized', 'a read with an expired token');
    await waitFor(() => demo.events.some(event => event.at > expiresAt + 500), 'a notification after the token expired', 10_000);
    log('[1,2] the token has expired: a read gets 401, but the stream keeps delivering');

    await waitFor(() => hrRevokedAt !== undefined, 'the HR access change', ACCESS_TOKEN_SECONDS * 2000);
    step(
        CURRENT_SERVER,
        'Step 3: The user loses access; the stream does not',
        'The user loses access to `case-114.md`, but the stream keeps delivering its changes. The client then refreshes its token, so that the server accepts its cancellation, and closes the stream.'
    );
    await waitFor(
        () => demo.events.some(event => event.uri === HR_URI && event.at > (hrRevokedAt ?? Number.POSITIVE_INFINITY)),
        'a case-114.md change after access was lost',
        10_000
    );
    log('[1,2] the user lost access to case-114.md, but the stream still delivers its changes');
    await demo.tokens.refresh();
    await stream.subscription.close();
    await expectClosed(stream, undefined, 5000);
    await demo.client.close();
}

/** Scenario 3: a proposed client with the proposed server. */
async function proposedScenario(): Promise<void> {
    step(
        PROPOSED,
        'Step 1: Streams report their deadline',
        'The client gets a token that is valid for 20 seconds and opens two streams with it: stream A on `config.json` and `case-114.md`, and stream B on `case-114.md` alone. Each acknowledgment carries `authorizedUntil`, the time after which the server writes nothing to the stream.'
    );
    const demo = await connect(PROPOSED, { subject: 'user-114', clientId: 'demo-client', endpoint: mcpUrl, proposed: true });
    let streamA = await openStream(demo, '[3] A1 [config, hr]', { resourceSubscriptions: [PROJECT_URI, HR_URI] });
    let streamB = await openStream(demo, '[3] B1 [hr]', { resourceSubscriptions: [HR_URI] });
    check.ok(streamA.subscription.authorizedUntil, 'A1 acknowledgment includes authorizedUntil');
    check.ok(streamB.subscription.authorizedUntil, 'B1 acknowledgment includes authorizedUntil');

    const firstDeadline = Math.min(deadlineOf(streamA), deadlineOf(streamB));
    const reconnectWindowMs = Math.min(60_000, Math.floor(demo.tokens.expiresIn() * 1000 * 0.1));
    const latestReconnectAt = firstDeadline - 1000;
    const earliestReconnectAt = firstDeadline - reconnectWindowMs;
    const reconnectAt = earliestReconnectAt + Math.floor(Math.random() * Math.max(1, latestReconnectAt - earliestReconnectAt));
    log(`[3] A/B replacement scheduled inside the final ${reconnectWindowMs}ms before authorizedUntil (lifetime §6 rule 5)`);
    await waitUntilWallClock(reconnectAt);

    step(
        PROPOSED,
        'Step 2: Replacing the streams before the deadline',
        'Shortly before the deadline, the client refreshes its token once, opens replacement streams with the new token, cancels the old ones, and rereads the resources to resynchronize.'
    );
    const refreshesBeforeReplacement = demo.tokens.refreshRequests;
    await demo.tokens.refresh();
    check.equal(demo.tokens.refreshRequests - refreshesBeforeReplacement, 1, 'exactly one refresh request reauthorizes A and B');
    const replacementA = await openStream(demo, '[3] A2 replacement [config, hr]', { resourceSubscriptions: [PROJECT_URI, HR_URI] });
    const replacementB = await openStream(demo, '[3] B2 replacement [hr]', { resourceSubscriptions: [HR_URI] });
    await streamA.subscription.close();
    await streamB.subscription.close();
    await expectClosed(streamA, undefined, 5000);
    await expectClosed(streamB, undefined, 5000);
    check.equal(streamA.closedCause, 'local', 'old A is cancelled locally after replacement ack');
    check.equal(streamB.closedCause, 'local', 'old B is cancelled locally after replacement ack');
    await resync(demo.client, [PROJECT_URI, HR_URI], '[3] A2/B2');
    streamA = replacementA;
    streamB = replacementB;

    await waitFor(
        asyncFlag(() => readStatus(demo.client).then(status => status.hrAccessRevoked)),
        'HR ACL revocation',
        ACCESS_TOKEN_SECONDS * 1200
    );
    hrRevokedAt = Date.now();
    step(
        PROPOSED,
        'Step 3: Losing access to one resource',
        'The user loses access to `case-114.md`. The server stops writing its changes to both streams, without telling the client. Stream A keeps delivering `config.json`. Stream B has nothing left to deliver, but stays open until its deadline, so that its end does not reveal when the resource changed.'
    );
    await sleep(1000);
    check.equal(streamB.closedCause, undefined, 'B remains open at least 1s after the HR change');
    const current = streamA;
    const configEventsAtHrRevocation = demo.events.filter(event => event.subId === current.subId && event.uri === PROJECT_URI).length;
    await waitFor(
        () => demo.events.filter(event => event.subId === current.subId && event.uri === PROJECT_URI).length > configEventsAtHrRevocation,
        'A config notification after HR access reduction',
        10_000
    );
    check.equal(
        demo.events.some(event => event.subId === current.subId && event.uri === HR_URI && event.at > (hrRevokedAt ?? 0)),
        false,
        'A receives no HR notification after HR access is removed'
    );
    log('[3] A keeps config updates and HR updates are dropped; B still awaits its deadline (lifetime §3 rules 2,5)');

    step(
        PROPOSED,
        'Step 4: Missing the deadline',
        'The client lets the next deadline pass. Stream A ends with `token_expiry`. Stream B ends with `revoked`, because nothing it carried is authorized any more. The client refreshes its token and reopens stream A with the same filter. The server refuses the whole stream: instead of an acknowledgment, it answers with an error that names `case-114.md` as not permitted. The client reports that `case-114.md` is no longer covered, and reopens stream A on `config.json` alone. Its one attempt to reopen stream B is refused the same way, so it reports that subscription lost.'
    );
    await expectClosed(streamA, 'token_expiry', ACCESS_TOKEN_SECONDS * 1500);
    await expectClosed(streamB, 'revoked', ACCESS_TOKEN_SECONDS * 1500);
    const [endedA, endedB] = [streamA, streamB];
    const lateEvents = demo.events.filter(
        event =>
            (event.subId === endedA.subId && event.at > deadlineOf(endedA) + 250) ||
            (event.subId === endedB.subId && event.at > deadlineOf(endedB) + 250)
    );
    check.deepEqual(lateEvents, [], 'no notifications are written after authorizedUntil');
    await demo.tokens.refresh();
    const deniedA = await openDenied(demo, { resourceSubscriptions: [PROJECT_URI, HR_URI] });
    check.deepEqual(deniedA, { resourceSubscriptions: [HR_URI] }, 'reopening A [config, hr] is refused, naming case-114.md');
    log(
        '[3] reopening A [config, hr] was refused with -32602 naming case-114.md in error.data.denied; reporting case-114.md no longer covered (lifetime §3 rule 5 / §6 rule 4)'
    );
    streamA = await openStream(demo, '[3] A3 after token_expiry [config]', { resourceSubscriptions: [PROJECT_URI] });
    check.deepEqual(
        streamA.subscription.honoredFilter.resourceSubscriptions,
        [PROJECT_URI],
        'A3 acknowledges the whole filter it asked for'
    );
    await resync(demo.client, [PROJECT_URI], '[3] A3');
    const deniedB = await openDenied(demo, { resourceSubscriptions: [HR_URI] });
    check.deepEqual(deniedB, { resourceSubscriptions: [HR_URI] }, 'reopening B [hr] is refused, naming case-114.md');
    log(
        '[3] B revoked: one silent reopen attempt was refused, naming case-114.md; subscription lost and needs action (lifetime §5 reason table / §7 rule 4)'
    );

    step(
        PROPOSED,
        'Step 5: A policy change requires more scope',
        'The server starts to require the scope `files:audit`. Stream A ends with `insufficient_authorization`. The client reopens it with its current token and gets a `403` challenge; only then does it ask the user, get a token with the wider scope, and reopen the stream.'
    );
    await expectClosed(streamA, 'insufficient_authorization', ACCESS_TOKEN_SECONDS * 1500);
    await openRejected(demo, { resourceSubscriptions: [PROJECT_URI] }, 'insufficient');
    log(
        `[3] 403 insufficient_scope received; confirming with user for MCP server "${MCP_SERVER_NAME}" and authorization server "${AUTH_SERVER_NAME}" before step-up (lifetime §7 rules 1-2)`
    );
    await demo.tokens.stepUp('files:read files:audit');
    streamA = await openStream(demo, '[3] A4 after files:audit step-up [config]', { resourceSubscriptions: [PROJECT_URI] });
    await resync(demo.client, [PROJECT_URI], '[3] A4');

    step(
        PROPOSED,
        'Step 6: The grant is revoked',
        "The authorization server revokes the user's grant, and the MCP server ends the stream with `revoked`. The client's one refresh without the user fails, so it reports the subscription lost instead of prompting."
    );
    await expectClosed(streamA, 'revoked', ACCESS_TOKEN_SECONDS * 1500);
    try {
        await demo.tokens.refresh();
        throw new Error('revoked grant refresh unexpectedly succeeded');
    } catch (error) {
        if (!(error instanceof TokenEndpointError) || error.error !== 'invalid_grant') throw error;
        log(
            '[3] revoked grant: one silent refresh attempt failed invalid_grant; reporting subscription lost without prompting (lifetime §7 rule 4)'
        );
    }
    await demo.client.close();
}

/** Scenario 4: a current client with the proposed server. The stream stops at its deadline, as an ordinary disconnect. */
async function currentClientScenario(): Promise<void> {
    step(
        CURRENT_CLIENT,
        'Step 1: The stream stops at its deadline',
        'The client gets a token and opens a stream on `config.json`. The acknowledgment carries `authorizedUntil`, which this client does not know about. At the deadline the server closes the stream without a response: to this client, an ordinary disconnect.'
    );
    const demo = await connect(CURRENT_CLIENT, {
        subject: 'legacy-user-114',
        clientId: 'legacy-client',
        endpoint: mcpUrl,
        proposed: false
    });
    const stream = await openStream(demo, '[4] C1 [config]', { resourceSubscriptions: [PROJECT_URI] });
    check.ok(stream.subscription.authorizedUntil, 'the proposed server reports authorizedUntil to every client');
    await expectClosed(stream, undefined, ACCESS_TOKEN_SECONDS * 1500);
    check.equal(stream.closedCause, 'remote', 'the stream closes as an unexpected disconnect');
    check.deepEqual(
        demo.events.filter(event => event.subId === stream.subId && event.at > deadlineOf(stream) + 250),
        [],
        'no notifications are written after authorizedUntil'
    );

    step(
        CURRENT_CLIENT,
        'Step 2: Reconnecting, as today',
        'The client reconnects with its expired token and gets `401`. It refreshes the token, reconnects, and closes the stream when it is done.'
    );
    await openRejected(demo, { resourceSubscriptions: [PROJECT_URI] }, 'unauthorized');
    log('[4] stale-token reconnect got HTTP 401 (lifetime §5 rule 3 / Backward Compatibility)');
    await demo.tokens.refresh();
    const reopened = await openStream(demo, '[4] C2 [config]', { resourceSubscriptions: [PROJECT_URI] });
    await resync(demo.client, [PROJECT_URI], '[4] C2');
    await reopened.subscription.close();
    await expectClosed(reopened, undefined, 5000);
    check.equal(reopened.closedCause, 'local', 'the reopened stream closes locally');
    check.equal(reopened.subscription.endReason, undefined, 'the reopened stream closes without AuthorizationEnded');
    await demo.client.close();
}

const story: Story = {
    title: 'Wire log: Authorization Lifetime demo',
    notes: [
        "Left out, because the proposal changes nothing in them: version discovery (`server/discover`), the reads a client makes to resynchronize after reopening a stream, and the demo's own status tool.",
        '§ numbers refer to the Authorization Lifetime SEP.'
    ],
    intro: "The demo's server has two endpoints, which run at the same time: `/mcp` implements the proposal, and `/mcp-current` runs without it, as today's SDK does. Clients with and without the proposal use them at once. This log groups what each pairing sent and received.",
    scenarios: [
        {
            id: CURRENT_SERVER,
            label: '1 and 2',
            title: 'Current or proposed client with current MCP server',
            client: 'Current or proposed',
            server: 'Current',
            change: 'Nothing changes. A current server does not tie a stream to the token that opened it, so the stream keeps delivering after the token expires and after the user loses access to a resource, while a new request with the same token is refused. This is the gap the proposal closes. A proposed client speaks the current protocol revision with this server, and gets no `authorizedUntil` either.',
            intro: "The demo runs this pairing with a current client. A proposed client looks the same on the wire: version discovery shows that the server does not support the next protocol revision, so the client speaks `2026-07-28` with it, gets no `authorizedUntil`, and can schedule reopening its stream only from its own token's expiry."
        },
        {
            id: PROPOSED,
            label: '3',
            title: 'Proposed client with proposed MCP server',
            client: 'Proposed',
            server: 'Proposed',
            change: 'Everything the proposal adds: `authorizedUntil` in the acknowledgment; each notification checked against the token; streams that end at the deadline with `AuthorizationEnded` and a reason; a stream the token does not fully permit refused with an error that names what it does not permit; and clients that re-establish, step up, or give up according to the reason, without prompting the user because of a stream error alone.',
            intro: 'The client and the server both implement the proposal, so the client speaks the next protocol revision, `‹vNext›`.'
        },
        {
            id: CURRENT_CLIENT,
            label: '4',
            title: 'Current client with proposed MCP server',
            client: 'Current',
            server: 'Proposed',
            change: 'The stream stops at its deadline. The server closes it without a response, which a current client treats as an ordinary disconnect; reconnecting with the expired token gets `401`, so the client refreshes and reconnects, as it does today. The acknowledgment also carries `authorizedUntil`, which a current client ignores.',
            intro: 'The client speaks the current protocol revision, `2026-07-28`, so the server treats it as a client of that revision.'
        }
    ]
};

const { url } = parseExampleArgs();
const mcpUrl = new URL(url);
const tokenUrl = new URL('/token', mcpUrl);
const currentUrl = new URL(CURRENT_SERVER_PATH, mcpUrl);

const wire = new WireLog(wireLogPath('authorization-lifetime'), story);
console.log(`Wire log, written when the demo ends: ${wire.file}`);

/** When scenario 3 saw the user lose access to the HR case. Scenarios 1 and 2 wait for it. */
let hrRevokedAt: number | undefined;

await Promise.all([currentServerScenario(), proposedScenario(), currentClientScenario()]);
wire.write();

console.log('\nPASS summary');
console.log('| Scenario | Checked |');
console.log('| --- | --- |');
console.log(
    '| 1 and 2: current server | no authorizedUntil; after the token expired, a read got 401 while the stream kept delivering; after the user lost access to case-114.md, the stream still delivered its changes |'
);
console.log('| 3, step 1 | authorizedUntil on both streams |');
console.log('| 3, step 2 | one refresh re-established both streams before the deadline; the old streams closed locally |');
console.log('| 3, step 3 | case-114.md notifications dropped; stream A continued; stream B stayed open past the change |');
console.log(
    '| 3, step 4 | token_expiry and revoked; no writes after the deadline; reopening A and B was refused, naming case-114.md; A reopened on config.json |'
);
console.log('| 3, step 5 | insufficient_authorization led to a request, a 403 challenge, named confirmation, and step-up |');
console.log('| 3, step 6 | revoked; the one silent refresh failed with invalid_grant; no prompt |');
console.log(
    '| 4: current client | the stream closed without a response at its deadline; reconnecting got 401; refresh and reconnect worked |'
);
console.log('\nPASS authorization lifetime demo');
console.log(`\nWire log, by scenario, with every request and response: ${wire.file}`);

function asyncFlag(read: () => Promise<boolean>): () => boolean {
    let value = false;
    let reading = false;
    return () => {
        if (!value && !reading) {
            reading = true;
            void read()
                .then(next => {
                    value = next;
                })
                .finally(() => {
                    reading = false;
                });
        }
        return value;
    };
}
