/**
 * Self-verifying client for the Subscription Lifecycle SEP, which builds on the Authorization Lifetime SEP.
 *
 * It runs four scenarios at once, each a pairing of a client and an MCP server, with or without the
 * proposals: a current client and a proposed client with the current server at `/mcp-current`
 * (scenarios 1 and 2), a proposed client with the proposed server at `/mcp` (scenario 3), and a
 * current client with the proposed server (scenario 4). The proposed client declares a draft
 * capability that stands in for the new protocol version, and opts in to lifecycle notifications.
 * Every exchange is recorded in a wire log, written when the demo ends.
 */
import { performance } from 'node:perf_hooks';

import { check, parseExampleArgs } from '@mcp-examples/shared';
import type { FetchLike, McpSubscription } from '@modelcontextprotocol/client';
import { Client, InsufficientScopeError, StreamableHTTPClientTransport, UnauthorizedError } from '@modelcontextprotocol/client';

import type { Story } from './wireLog';
import { CURRENT_SERVER_PATH, WireLog, wireLogPath } from './wireLog';

const PROJECT_URI = 'file:///project/config.json';
const HR_URI = 'file:///hr/case-114.md';
const ACCESS_TOKEN_SECONDS = Number(process.env.DEMO_ACCESS_TOKEN_SECONDS ?? '120');
const DRAFT_CAPABILITIES = { experimental: { 'io.modelcontextprotocol/subscription-lifetime': {} } };
const start = performance.now();

/** The scenarios, as the wire log groups them. */
const CURRENT_SERVER = 'current-server';
const PROPOSED_CLIENT_CURRENT_SERVER = 'proposed-client-current-server';
const PROPOSED = 'proposed';
const CURRENT_CLIENT = 'current-client';

interface TokenSet {
    access_token: string;
    refresh_token: string;
    expires_in: number;
    scope: string;
}

interface TimingReport {
    tokenLifetimeSeconds: number;
    reminderLeadTimesMs: number[];
    firstReminderToUpdateMs?: number;
    tooLittleTimeToRefresh: boolean;
    pauseDurationMs?: number;
    pausedReminders: number;
}

interface ResourceEvent {
    at: number;
    uri: string;
}

/** A client in one scenario, with the resource notifications it received. */
interface DemoClient {
    client: Client;
    tokens: DemoTokens;
    events: ResourceEvent[];
}

const timing: TimingReport = {
    tokenLifetimeSeconds: ACCESS_TOKEN_SECONDS,
    reminderLeadTimesMs: [],
    tooLittleTimeToRefresh: false,
    pausedReminders: 0
};

const events = {
    initialUpdate: false,
    accessReduced: false,
    paused: false,
    lateUpdate: false,
    heldOrMissed: false,
    insufficientReminder: false,
    insufficientRejected: false,
    stepUp: false,
    expiredGracefully: false
};

function rel(): string {
    return `+${Math.round(performance.now() - start)
        .toString()
        .padStart(5)}ms`;
}

function log(message: string): void {
    console.log(`${rel()} ${message}`);
}

/**
 * Starts a step of a scenario, on the console and in the wire log. `withLatestMessage` marks a step that
 * a lifecycle notification started, so that the log shows the notification as part of the step.
 */
function step(scenario: string, title: string, text: string, withLatestMessage = false): void {
    log(`STEP [${scenario}] ${title}`);
    wire.step(scenario, title, text, { withLatestMessage });
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

function isUnauthorized(error: unknown): boolean {
    if (error instanceof UnauthorizedError) return true;
    if (typeof error === 'object' && error !== null && 'data' in error) {
        return isUnauthorized((error as { data?: { cause?: unknown } }).data?.cause);
    }
    return false;
}

async function expectUnauthorized(request: () => Promise<unknown>, description: string): Promise<void> {
    try {
        await request();
    } catch (error) {
        if (isUnauthorized(error)) return;
        throw error;
    }
    throw new Error(`expected ${description} to be rejected with 401`);
}

async function expectClosed(subscription: McpSubscription, timeoutMs: number): Promise<'local' | 'graceful' | 'remote'> {
    let timer: ReturnType<typeof setTimeout> | undefined;
    const timedOut = new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error(`stream did not close within ${timeoutMs}ms`)), timeoutMs);
    });
    try {
        return await Promise.race([subscription.closed, timedOut]);
    } finally {
        clearTimeout(timer);
    }
}

class DemoTokens {
    private current?: TokenSet;

    constructor(
        private readonly tokenUrl: URL,
        private readonly clientId: string,
        private readonly fetch: FetchLike
    ) {}

    async start(scopes = 'files:read'): Promise<void> {
        this.current = await this.request(
            new URLSearchParams({ grant_type: 'client_credentials', client_id: this.clientId, scope: scopes })
        );
        log(`token(${this.clientId}): client_credentials [${this.current.scope}] exp=${this.current.expires_in}s`);
    }

    async refresh(): Promise<void> {
        const refreshToken = this.current?.refresh_token;
        if (refreshToken === undefined) throw new Error('cannot refresh before initial token');
        this.current = await this.request(new URLSearchParams({ grant_type: 'refresh_token', refresh_token: refreshToken }));
        log(`token(${this.clientId}): refresh_token [${this.current.scope}] exp=${this.current.expires_in}s`);
    }

    async stepUp(scopes: string): Promise<void> {
        log(`auth UX: asking user for ${scopes}; auto-confirming in demo`);
        this.current = await this.request(
            new URLSearchParams({ grant_type: 'client_credentials', client_id: this.clientId, scope: scopes })
        );
        log(`token(${this.clientId}): stepped up [${this.current.scope}] exp=${this.current.expires_in}s`);
    }

    token(): string {
        if (this.current === undefined) throw new Error('token requested before initialization');
        return this.current.access_token;
    }

    expiresIn(): number {
        if (this.current === undefined) throw new Error('token lifetime requested before initialization');
        return this.current.expires_in;
    }

    /** When the current access token expires, from its `exp` claim. */
    expiresAt(): number {
        const claims = JSON.parse(Buffer.from(this.token().split('.')[1] ?? '', 'base64url').toString('utf8')) as { exp?: unknown };
        if (typeof claims.exp !== 'number') throw new Error('access token has no exp claim');
        return claims.exp * 1000;
    }

    private async request(body: URLSearchParams): Promise<TokenSet> {
        const response = await this.fetch(this.tokenUrl, {
            method: 'POST',
            headers: { 'content-type': 'application/x-www-form-urlencoded' },
            body
        });
        if (!response.ok) throw new Error(`token endpoint failed: ${response.status} ${await response.text()}`);
        return (await response.json()) as TokenSet;
    }
}

/** Gets a token and connects a client, current or proposed, to `endpoint`. */
async function connect(
    scenario: string,
    options: { clientId: string; endpoint: URL; proposed: boolean; refreshOnUnauthorized?: boolean }
): Promise<DemoClient> {
    const fetch = wire.fetch(scenario, options.proposed ? 'Client (proposed)' : 'Client (current)');
    const tokens = new DemoTokens(tokenUrl, options.clientId, fetch);
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
            authProvider: {
                token: async () => tokens.token(),
                ...(options.refreshOnUnauthorized === true && { onUnauthorized: async () => tokens.refresh() })
            },
            onInsufficientScope: 'throw'
        })
    );
    const demo: DemoClient = { client, tokens, events: [] };
    client.setNotificationHandler('notifications/resources/updated', notification => {
        const uri = notification.params.uri;
        if (typeof uri === 'string') demo.events.push({ at: Date.now(), uri });
    });
    return demo;
}

async function readHrAccessRevoked(client: Client): Promise<boolean> {
    const result = await client.callTool({ name: 'demo-status', arguments: {} });
    const text = result.content?.[0]?.type === 'text' ? result.content[0].text : '{}';
    return (JSON.parse(text) as { hrAccessRevoked?: boolean }).hrAccessRevoked === true;
}

async function updateWithFreshToken(subscription: McpSubscription, tokens: DemoTokens, label: string): Promise<void> {
    const started = performance.now();
    await tokens.refresh();
    const result = await subscription.update();
    log(`[3] ${label}: subscriptions/update ok authorizedUntil=${result.authorizedUntil}`);
    events.initialUpdate ||= label === 'first reminder';
    timing.firstReminderToUpdateMs ??= Math.round(performance.now() - started);
}

function printTimingReport(): void {
    console.log('\nTiming report (scenario 3)');
    console.log('| Metric | Value |');
    console.log('| --- | --- |');
    console.log(`| Token lifetime | ${timing.tokenLifetimeSeconds}s |`);
    console.log(`| Reminder lead times before the deadline | ${timing.reminderLeadTimesMs.join(', ') || 'none'} ms |`);
    console.log(
        `| Shortest lead before the deadline | ${timing.reminderLeadTimesMs.length > 0 ? Math.min(...timing.reminderLeadTimesMs) : 'n/a'} ms |`
    );
    console.log(`| First reminder to successful update (refresh + update) | ${timing.firstReminderToUpdateMs ?? 'n/a'} ms |`);
    console.log(`| Any reminder before the deadline with under 250 ms left | ${timing.tooLittleTimeToRefresh ? 'yes' : 'no'} |`);
    console.log(`| Pause duration | ${timing.pauseDurationMs ?? 'n/a'} ms |`);
    console.log(`| Reminders during the pause | ${timing.pausedReminders} |`);
}

/** Scenario 1: a current client with the current server. The stream outlives the token and the user's access. */
async function currentServerScenario(): Promise<void> {
    step(
        CURRENT_SERVER,
        'Step 1: Opening a stream',
        'The client gets a token that is valid for 20 seconds, and opens a stream on `config.json` and `case-114.md` with it.'
    );
    const demo = await connect(CURRENT_SERVER, { clientId: 'today-client', endpoint: currentUrl, proposed: false });
    const subscription = await demo.client.listen({ resourceSubscriptions: [PROJECT_URI, HR_URI] }, { timeout: 15_000 });
    check.equal(subscription.authorizedUntil, undefined, 'a current server reports no authorizedUntil');
    const expiresAt = demo.tokens.expiresAt();
    await waitUntilWallClock(expiresAt + 1500);

    step(
        CURRENT_SERVER,
        'Step 2: The token expires; the stream does not',
        'The token has expired. The client reads `config.json` with it, and the server refuses the request with `401`, yet the stream the same token opened keeps delivering.'
    );
    await expectUnauthorized(() => demo.client.readResource({ uri: PROJECT_URI }), 'a read with an expired token');
    await waitFor(() => demo.events.some(event => event.at > expiresAt + 500), 'a notification after the token expired', 10_000);
    log('[1] the token has expired: a read gets 401, but the stream keeps delivering');

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
    log('[1] the user lost access to case-114.md, but the stream still delivers its changes');
    await demo.tokens.refresh();
    await subscription.close();
    check.equal(await expectClosed(subscription, 5000), 'local', 'the client closes the stream');
    await demo.client.close();
}

/** Scenario 2: a proposed client with the current server. Its opt-in is ignored. */
async function proposedClientCurrentServerScenario(): Promise<void> {
    step(
        PROPOSED_CLIENT_CURRENT_SERVER,
        'Step 1: The opt-in is ignored',
        'The client opts in to lifecycle notifications and asks for an end time. The current server knows neither: the acknowledgment has no `lifecycle`, no `handle`, and no `authorizedUntil`. The client cannot update the stream in place, so it falls back to reopening it on its own schedule, from its token’s expiry; the demo simply closes it.'
    );
    const demo = await connect(PROPOSED_CLIENT_CURRENT_SERVER, { clientId: 'proposed-client', endpoint: currentUrl, proposed: true });
    const expiresAt = new Date(Date.now() + 120_000).toISOString();
    const subscription = await demo.client.listen(
        { lifecycle: true, resourceSubscriptions: [PROJECT_URI] },
        { expiresAt, timeout: 15_000 }
    );
    check.equal(subscription.handle, undefined, 'a current server issues no handle');
    check.equal(subscription.authorizedUntil, undefined, 'a current server reports no authorizedUntil');
    check.equal(subscription.honoredFilter.lifecycle, undefined, 'a current server does not honor lifecycle');
    log('[2] the current server ignored lifecycle and expiresAt; no handle, so the client cannot update in place');
    await subscription.close();
    check.equal(await expectClosed(subscription, 5000), 'local', 'the client closes the stream');
    await demo.client.close();
}

/** Scenario 3: a proposed client with the proposed server. */
async function proposedScenario(): Promise<void> {
    step(
        PROPOSED,
        'Step 1: Opting in',
        'The client gets a token that is valid for 20 seconds and opens a stream on `config.json` and `case-114.md`. It opts in to lifecycle notifications and asks for the stream to end after two minutes. The acknowledgment carries a `handle` for updating the stream in place.'
    );
    const demo = await connect(PROPOSED, { clientId: 'demo-client', endpoint: mcpUrl, proposed: true, refreshOnUnauthorized: true });
    const { client, tokens } = demo;
    timing.tokenLifetimeSeconds = tokens.expiresIn();

    let projectUpdates = 0;
    let hrUpdates = 0;
    let firstPauseReminderAt: number | undefined;
    let firstReminderHandled = false;
    let skipping = false;
    let stepUpScheduled = false;

    client.setNotificationHandler('notifications/resources/updated', notification => {
        const uri = notification.params.uri;
        if (uri === PROJECT_URI) projectUpdates++;
        if (uri === HR_URI) hrUpdates++;
        if (events.lateUpdate && uri === PROJECT_URI) events.heldOrMissed = true;
    });

    const streamDurationMs = tokens.expiresIn() <= 30 ? 120_000 : tokens.expiresIn() * 3000;
    const expiresAt = new Date(Date.now() + streamDurationMs).toISOString();
    const subscription = await client.listen(
        { lifecycle: true, resourceSubscriptions: [PROJECT_URI, HR_URI] },
        { expiresAt, timeout: 15_000 }
    );
    check.ok(subscription.handle, 'ack should include a subscription handle');
    check.ok(subscription.authorizedUntil, 'ack should include authorizedUntil');
    log(
        `[3] listen ack: handle=${subscription.handle} authorizedUntil=${subscription.authorizedUntil} expiresAt=${subscription.expiresAt}`
    );

    // Scenario 1 waits for the user's loss of access to the HR case; this client can see it in the demo's status.
    void waitFor(
        asyncFlag(() => readHrAccessRevoked(client)),
        'HR access change',
        ACCESS_TOKEN_SECONDS * 2000
    ).then(() => {
        hrRevokedAt = Date.now();
        step(
            PROPOSED,
            'Step 3: Losing access to one resource',
            'The user loses access to `case-114.md`. The server stops writing its changes at once, without telling the client yet. With its next reminder, it tells the client, with `access_reduced`, that the stream no longer carries that resource.'
        );
    });

    subscription.onlifecycle = notification => {
        void (async () => {
            const params = notification.params;
            log(`[3] lifecycle: ${params.type} ${JSON.stringify(params)}`);
            switch (params.type) {
                case 'reauthorization_required': {
                    const authorizedUntil =
                        'authorizedUntil' in params && typeof params.authorizedUntil === 'string'
                            ? Date.parse(params.authorizedUntil)
                            : Date.now();
                    const lead = authorizedUntil - Date.now();
                    // Only reminders sent before the deadline measure lead time; reminders during a pause come after it by design.
                    if (lead > 0) {
                        timing.reminderLeadTimesMs.push(Math.round(lead));
                        if (lead < 250) timing.tooLittleTimeToRefresh = true;
                    }
                    if (params.reason === 'token_expiry') {
                        if (events.lateUpdate) {
                            await updateWithFreshToken(subscription, tokens, 'maintenance reminder');
                            return;
                        }
                        if (!firstReminderHandled && lead > 0) {
                            firstReminderHandled = true;
                            step(
                                PROPOSED,
                                'Step 2: A reminder, answered by an update',
                                'Shortly before the deadline, the server sends a reminder. The client refreshes its token and updates the stream in place: the same stream carries on, with no gap.',
                                true
                            );
                            await updateWithFreshToken(subscription, tokens, 'first reminder');
                            return;
                        }
                        if (lead > 0) {
                            if (!skipping) {
                                skipping = true;
                                step(
                                    PROPOSED,
                                    'Step 4: Missing the deadline pauses the stream',
                                    'The client lets the next deadline pass. The stream pauses instead of ending: it carries nothing but reminders, at most once a minute. A minute later the client updates it, and the stream resumes on the same connection, first delivering the notifications the server held for it.',
                                    true
                                );
                            }
                            log('[3] client intentionally skips this token deadline to demonstrate pausing');
                            return;
                        }
                        timing.pausedReminders++;
                        events.paused = true;
                        firstPauseReminderAt ??= performance.now();
                        if (timing.pausedReminders < 2) {
                            log('[3] client remains paused long enough to observe a repeated reminder');
                            return;
                        }
                        await updateWithFreshToken(subscription, tokens, 'late paused reminder');
                        events.lateUpdate = true;
                        timing.pauseDurationMs ??= Math.round(performance.now() - firstPauseReminderAt);
                        return;
                    }
                    if (params.reason === 'insufficient_authorization' && !stepUpScheduled) {
                        stepUpScheduled = true;
                        events.insufficientReminder = true;
                        step(
                            PROPOSED,
                            'Step 5: A policy change requires more scope',
                            'The server starts to require the scope `files:audit`, and sends a reminder at once. The client updates the stream with its current token and gets a `403` challenge; only then does it ask the user, get a token with the wider scope, and update again.',
                            true
                        );
                        try {
                            await subscription.update();
                            throw new Error('under-scoped update unexpectedly succeeded');
                        } catch (error) {
                            if (!(error instanceof InsufficientScopeError)) throw error;
                            events.insufficientRejected = true;
                            log(`[3] update rejected with insufficient_scope; required=${error.requiredScope ?? '(none)'}`);
                        }
                        await tokens.stepUp('files:read files:audit');
                        const result = await subscription.update();
                        events.stepUp = true;
                        log(`[3] step-up update ok authorizedUntil=${result.authorizedUntil}`);
                        step(
                            PROPOSED,
                            'Step 6: The stream reaches its end time',
                            'The client keeps updating the stream before each deadline. At the `expiresAt` it asked for, the stream ends with the completion result.'
                        );
                    }
                    break;
                }
                case 'access_reduced': {
                    events.accessReduced = true;
                    const removed = 'removed' in params ? params.removed : {};
                    log(`[3] access reduced; removed=${JSON.stringify(removed)}`);
                    break;
                }
                case 'missed': {
                    events.heldOrMissed = true;
                    log('[3] server reported missed notifications; client would resynchronize');
                    break;
                }
            }
        })().catch(error => {
            console.error(error);
            process.exitCode = 1;
        });
    };

    await waitFor(() => events.initialUpdate, 'first reauthorization update', ACCESS_TOKEN_SECONDS * 1000);
    await waitFor(() => projectUpdates > 0 && hrUpdates > 0, 'initial resource updates', 15_000);
    await waitFor(() => events.accessReduced, 'access_reduced lifecycle notification', ACCESS_TOKEN_SECONDS * 2000);
    await waitFor(() => events.lateUpdate, 'late paused update', Math.max(90_000, ACCESS_TOKEN_SECONDS * 5000));
    await waitFor(
        () => events.insufficientReminder && events.insufficientRejected && events.stepUp,
        'insufficient_authorization step-up',
        30_000
    );
    await waitFor(() => events.heldOrMissed || projectUpdates > 2, 'held notification or missed marker after resume', 15_000);

    log('[3] waiting for requested stream expiry');
    const closed = await subscription.closed;
    events.expiredGracefully = closed === 'graceful';
    log(`[3] subscription closed: ${closed}`);
    await client.close();

    check.equal(closed, 'graceful', 'stream should end at requested expiry');
    check.ok(events.initialUpdate, 'handled a token-expiry reminder with refresh + update');
    check.ok(events.accessReduced, 'observed access_reduced for HR case');
    check.ok(events.paused && events.lateUpdate, 'observed pause and late update');
    check.ok(events.insufficientReminder && events.insufficientRejected && events.stepUp, 'observed insufficient_authorization step-up');
    check.ok(projectUpdates > 0, 'received project resource notifications');
    check.ok(hrUpdates > 0, 'received HR notifications before access was reduced');
}

/** Scenario 4: a current client with the proposed server. No lifecycle features; the stream stops at its deadline. */
async function currentClientScenario(): Promise<void> {
    step(
        CURRENT_CLIENT,
        'Step 1: The stream stops at its deadline',
        'The client gets a token and opens a stream on `config.json`, without opting in to anything. The acknowledgment carries new fields, which this client does not know about, and the server sends it no lifecycle notifications. At the deadline the server closes the stream without a response: to this client, an ordinary disconnect.'
    );
    const demo = await connect(CURRENT_CLIENT, { clientId: 'legacy-client', endpoint: mcpUrl, proposed: false });
    const first = await demo.client.listen({ resourceSubscriptions: [PROJECT_URI] }, { timeout: 15_000 });
    check.ok(first.authorizedUntil, 'the proposed server reports authorizedUntil to every client');
    check.equal(first.honoredFilter.lifecycle, undefined, 'lifecycle is not acknowledged unless requested');
    check.equal(await expectClosed(first, ACCESS_TOKEN_SECONDS * 1500), 'remote', 'the stream closes as an ordinary disconnect');
    check.equal(first.endReason, undefined, 'a current client gets no AuthorizationEnded');

    step(
        CURRENT_CLIENT,
        'Step 2: Reconnecting, as today',
        'The client reconnects with its expired token and gets `401`. It refreshes the token, reconnects, and closes the stream when it is done.'
    );
    await expectUnauthorized(
        () => demo.client.listen({ resourceSubscriptions: [PROJECT_URI] }, { timeout: 15_000 }),
        'reconnecting with an expired token'
    );
    await demo.tokens.refresh();
    const second = await demo.client.listen({ resourceSubscriptions: [PROJECT_URI] }, { timeout: 15_000 });
    await second.close();
    check.equal(await expectClosed(second, 5000), 'local', 'the client closes the reopened stream');
    log('[4] the stream stopped at its deadline as an ordinary disconnect; reconnecting got 401, then refresh and reconnect worked');
    await demo.client.close();
}

const story: Story = {
    title: 'Wire log: Subscription Lifecycle demo',
    intro: "The demo's server has two endpoints, which run at the same time: `/mcp` implements the proposals, Subscription Lifecycle and the Authorization Lifetime SEP it builds on, and `/mcp-current` runs without them, as today's SDK does. Clients with and without the proposals use them at once. This log groups what each pairing sent and received.",
    notes: [
        'In this log, "the proposal" means both drafts: Subscription Lifecycle, and the Authorization Lifetime SEP it builds on. A proposed client speaks the next protocol revision with a server that supports it, and opts in to lifecycle notifications.',
        'Left out, because the proposals change nothing in them: version discovery (`server/discover`), the reads a client makes to resynchronize, and the demo’s own status tool.',
        '"Lifetime §" refers to sections of the Authorization Lifetime SEP, and "Lifecycle §" to sections of the Subscription Lifecycle SEP.'
    ],
    scenarios: [
        {
            id: CURRENT_SERVER,
            label: '1',
            title: 'Current client with current MCP server',
            client: 'Current',
            server: 'Current',
            change: 'Nothing changes: the stream keeps delivering after its token expires and after the user loses access to a resource, while a new request with the same token is refused. This is the gap the proposals close.',
            intro: "Today's behavior, for comparison."
        },
        {
            id: PROPOSED_CLIENT_CURRENT_SERVER,
            label: '2',
            title: 'Proposed client with current MCP server',
            client: 'Proposed',
            server: 'Current',
            change: "Version discovery makes the client speak `2026-07-28` with this server, and its opt-in is ignored: the acknowledgment has no `lifecycle`, `handle`, or `authorizedUntil`, so the client cannot update the stream in place and falls back to reopening it on its own schedule. On the server's side, this is scenario 1.",
            intro: 'The client supports the next protocol revision, but this server does not, so the two use `2026-07-28`. The client still opts in to lifecycle notifications and asks for an end time; the server knows neither.'
        },
        {
            id: PROPOSED,
            label: '3',
            title: 'Proposed client with proposed MCP server',
            client: 'Proposed',
            server: 'Proposed',
            change: "The whole proposal: reminders before the deadline, updates in place instead of new streams, `access_reduced` when access is lost, a pause instead of an end when the client misses its deadline, a step-up through the update's `403`, and an end at the time the client asked for.",
            intro: 'The client and the server both implement the proposals, so the client speaks the next protocol revision, `‹vNext›`, and opts in to lifecycle notifications.'
        },
        {
            id: CURRENT_CLIENT,
            label: '4',
            title: 'Current client with proposed MCP server',
            client: 'Current',
            server: 'Proposed',
            change: 'No lifecycle features, which the client did not ask for: no reminders and no pausing. The acknowledgment carries new fields, which the client ignores. As under Authorization Lifetime, the stream stops at its deadline as an ordinary disconnect, and the client reconnects as it does today.',
            intro: 'The client speaks the current protocol revision, `2026-07-28`, and asks for nothing new.'
        }
    ]
};

const { url } = parseExampleArgs();
const mcpUrl = new URL(url);
const tokenUrl = new URL('/token', mcpUrl);
const currentUrl = new URL(CURRENT_SERVER_PATH, mcpUrl);

const wire = new WireLog(wireLogPath('subscription-lifecycle'), story);
console.log(`Wire log, written when the demo ends: ${wire.file}`);

/** When scenario 3 saw the user lose access to the HR case. Scenario 1 waits for it. */
let hrRevokedAt: number | undefined;

try {
    await Promise.all([currentServerScenario(), proposedClientCurrentServerScenario(), proposedScenario(), currentClientScenario()]);
    wire.write();
    printTimingReport();
    console.log('\nPASS subscription lifecycle demo');
} catch (error) {
    wire.write();
    printTimingReport();
    console.error('\nFAIL subscription lifecycle demo');
    console.error(error);
    process.exitCode = 1;
}
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
