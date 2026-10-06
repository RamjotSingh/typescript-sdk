/**
 * Wire log for the Subscription Lifecycle demo.
 *
 * Each client sends its requests through `WireLog.fetch(scenario, from)`, which records every HTTP
 * exchange, including each message the server writes to a `subscriptions/listen` stream. When the
 * demo ends, `write()` renders the record as Markdown, grouped by scenario: one section per request
 * and its response, with the lines that are new in the proposal marked with `+`.
 */
import { mkdirSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';

import type { FetchLike } from '@modelcontextprotocol/client';
import { CLIENT_CAPABILITIES_META_KEY, PROTOCOL_VERSION_META_KEY } from '@modelcontextprotocol/client';

/** The demo server's endpoint that runs without the proposal, as today's SDK does. */
export const CURRENT_SERVER_PATH = '/mcp-current';

/** A pairing of a client and an MCP server, each with or without the proposal. */
export interface Scenario {
    id: string;
    /** The scenario numbers the section covers, such as `3` or `1 and 2`. */
    label: string;
    title: string;
    client: string;
    server: string;
    /** What changes for this pairing, for the table at the top. */
    change: string;
    intro: string;
}

/** What the log says around the scenarios. */
export interface Story {
    title: string;
    intro: string;
    /** Bullets added to "How to read this log". */
    notes: string[];
    scenarios: Scenario[];
}

type JsonObject = Record<string, unknown>;
type EndHow = 'final' | 'server' | 'client' | 'error';
type StreamEvent = { at: number; message: JsonObject } | { at: number; end: EndHow };

/** One HTTP exchange, as a client saw it. */
interface Exchange {
    scenario: string;
    from: string;
    to: string;
    at: number;
    method: string;
    url: URL;
    headers: Headers;
    rpc?: JsonObject;
    form?: URLSearchParams;
    response?: { at: number; status: number; statusText: string; headers: Headers; text: string; json?: JsonObject };
    stream?: { name: string; events: StreamEvent[] };
    failure?: { at: number; text: string };
}

interface Step {
    scenario: string;
    at: number;
    title: string;
    text: string;
    /** The step began with the stream message that the client was handling when it started the step. */
    withLatestMessage: boolean;
}

interface TokenInfo {
    scenario: string;
    number: number;
    subject?: string;
    scope?: string;
    expiresAt?: number;
}

/** A line of a request or response, and whether it is new in the proposal. */
type Line = [isNew: boolean, text: string];

type Routine = { kind: 'routine'; from: number; to: number; counts: Map<string, number>; afterExpiry: boolean };
type Item =
    | { kind: 'message'; at: number; message: JsonObject }
    | Routine
    | { kind: 'expired'; at: number }
    | { kind: 'end'; at: number; how: EndHow };

type Block = { kind: 'step'; step: Step } | { kind: 'exchange' | 'continued'; x: Exchange; number: number; items: Item[] };

/** What the explanations need beyond the exchange itself. */
interface Context {
    time(at: number): string;
    token(x: Exchange): TokenInfo | undefined;
    issued(x: Exchange): TokenInfo | undefined;
    number(x: Exchange): number | undefined;
    openStreams(x: Exchange): Exchange[];
    /** The exchanges of `x`'s scenario that the log shows, in order. */
    others(x: Exchange): Exchange[];
}

const AUTHORIZATION_SERVER = 'Authorization server';
const PROPOSED_SERVER = 'MCP server (proposed)';
const CURRENT_SERVER = 'MCP server (current)';

const HOW_TO_READ = [
    'Each request has its own heading. Under it: who sent it to whom, what it does and what came of it, then the request and the response.',
    'In requests and responses, lines that start with `+` are new in the proposal. Everything else is as it is today.',
    'A subscription\'s response is a stream that stays open. Its messages are listed as they arrived, with routine `notifications/resources/updated` messages counted rather than shown. When the client sends other requests while a stream is open, the stream\'s later messages appear under a "continued" heading, in time order.',
    'Requests show their `_meta` envelope (protocol version, client information, and capabilities) as the specification will have it. A proposed client talking to a proposed server sends the protocol revision that adopts the proposal, shown as `‹vNext›`; with a current server, version discovery makes both sides use `2026-07-28`. The SDK cannot send an unreleased revision, so on the wire the prototype sends `2026-07-28` and declares the stand-in capability `experimental["io.modelcontextprotocol/subscription-lifetime"]`; the log shows `‹vNext›` instead, and leaves the stand-in out.',
    'Times are seconds since the demo started. Access tokens are numbered within each scenario, in the order the authorization server issued them.'
];

/** Where the demo writes its wire log: `$DEMO_WIRE_LOG`, or a file in the system temp folder. */
export function wireLogPath(story: string): string {
    return process.env.DEMO_WIRE_LOG ?? path.join(tmpdir(), 'mcp-subscription-demos', `${story}-wire-log.md`);
}

export class WireLog {
    private readonly started = Date.now();
    private readonly exchanges: Exchange[] = [];
    private readonly steps: Step[] = [];
    private readonly tokens = new Map<string, TokenInfo>();
    private written = false;

    constructor(
        readonly file: string,
        private readonly story: Story
    ) {
        process.once('exit', () => this.write());
    }

    /** A fetch that records each exchange under `scenario`, as sent by `from`. */
    fetch(scenario: string, from: string): FetchLike {
        return async (input, init) => {
            const x = describeRequest(scenario, from, new URL(String(input)), init);
            this.exchanges.push(x);
            let response: Response;
            try {
                response = await globalThis.fetch(input, init);
            } catch (error) {
                const aborted = error instanceof Error && error.name === 'AbortError';
                x.failure = { at: Date.now(), text: aborted ? 'the client abandoned the request' : String(error) };
                throw error;
            }
            return this.record(x, response);
        };
    }

    /**
     * Starts a step of `scenario`: a heading and a short narrative. With `withLatestMessage`, the step
     * begins with the stream message the client was handling, which arrived just before.
     */
    step(scenario: string, title: string, text: string, options?: { withLatestMessage?: boolean }): void {
        this.steps.push({ scenario, at: Date.now(), title, text, withLatestMessage: options?.withLatestMessage === true });
    }

    /** Renders the log. The demo calls it when it finishes; it also runs on exit, if the demo fails first. */
    write(): void {
        if (this.written) return;
        this.written = true;
        mkdirSync(path.dirname(this.file), { recursive: true });
        writeFileSync(this.file, this.render());
    }

    private async record(x: Exchange, response: Response): Promise<Response> {
        const base = { at: Date.now(), status: response.status, statusText: response.statusText, headers: new Headers(response.headers) };
        if (response.headers.get('content-type')?.includes('text/event-stream') && response.body) {
            x.response = { ...base, text: '' };
            x.stream = { name: String(x.rpc?.id ?? '?'), events: [] };
            return new Response(watch(x.stream.events, response.body), responseInit(response));
        }
        const text = await response.text();
        const json = parseObject(text);
        x.response = { ...base, text, json };
        if (json && x.to === AUTHORIZATION_SERVER && response.ok) this.registerToken(x.scenario, json);
        return new Response(text.length > 0 ? text : undefined, responseInit(response));
    }

    private registerToken(scenario: string, json: JsonObject): void {
        const token = json.access_token;
        if (typeof token !== 'string' || this.tokens.has(token)) return;
        const claims = jwtClaims(token);
        this.tokens.set(token, {
            scenario,
            number: [...this.tokens.values()].filter(info => info.scenario === scenario).length + 1,
            subject: typeof claims.sub === 'string' ? claims.sub : undefined,
            scope: typeof claims.scope === 'string' ? claims.scope : undefined,
            expiresAt: typeof claims.exp === 'number' ? claims.exp * 1000 : undefined
        });
    }

    private render(): string {
        const lines = [
            `# ${this.story.title}`,
            '',
            `Recorded ${new Date(this.started).toISOString()}. ${this.story.intro}`,
            '',
            '## Which pairings see a change',
            '',
            '| Scenario | Client | MCP server | What changes |',
            '| --- | --- | --- | --- |'
        ];
        for (const scenario of this.story.scenarios) {
            const link = `[${scenario.label}](#${slug(scenarioHeading(scenario))})`;
            lines.push(`| ${link} | ${scenario.client} | ${scenario.server} | ${scenario.change} |`);
        }
        lines.push(
            '',
            '## How to read this log',
            '',
            ...[...HOW_TO_READ, ...this.story.notes].map(item => `- ${item}`),
            '',
            '## Scenarios',
            ''
        );
        for (const scenario of this.story.scenarios) {
            lines.push(`### ${scenarioHeading(scenario)}`, '', scenario.intro, '');
            for (const block of this.timeline(scenario.id)) lines.push(...this.renderBlock(block), '');
        }
        return lines.join('\n');
    }

    /** Orders a scenario's steps, requests, and stream messages into blocks, as described in `HOW_TO_READ`. */
    private timeline(scenario: string): Block[] {
        const shown = this.exchanges.filter(x => x.scenario === scenario && !isLeftOut(x));
        const numbers = new Map(shown.map((x, index) => [x, index + 1]));
        type Event = { at: number; order: number } & (
            | { kind: 'step'; step: Step }
            | { kind: 'request'; x: Exchange }
            | { kind: 'item'; x: Exchange; item: Item }
        );
        const events: Event[] = [];
        for (const x of shown) {
            events.push({ at: x.at, order: events.length, kind: 'request', x });
            // Only a current server lets a stream outlive the token that opened it.
            const expiresAt = x.to === CURRENT_SERVER ? this.token(x)?.expiresAt : undefined;
            for (const event of x.stream?.events ?? []) {
                events.push({ at: event.at, order: events.length, kind: 'item', x, item: toItem(event, expiresAt) });
            }
            const end = streamEnd(x);
            if (x.stream && expiresAt !== undefined && (end === undefined || end.at > expiresAt + 1000)) {
                events.push({ at: expiresAt, order: events.length, kind: 'item', x, item: { kind: 'expired', at: expiresAt } });
            }
        }
        for (const step of this.steps) {
            if (step.scenario !== scenario) continue;
            // A step that a message started goes just before that message; any other step, before whatever happened at the same time.
            const anchor = step.withLatestMessage ? latestMessage(events, step.at) : undefined;
            events.push(
                anchor ? { at: anchor.at, order: anchor.order - 0.5, kind: 'step', step } : { at: step.at, order: -1, kind: 'step', step }
            );
        }
        events.sort((a, b) => a.at - b.at || a.order - b.order);

        const blocks: Block[] = [];
        const pending = new Map<Exchange, Item[]>();
        let current: Block | undefined;
        const flush = (): void => {
            for (const [x, items] of pending) blocks.push({ kind: 'continued', x, number: numbers.get(x) ?? 0, items });
            pending.clear();
        };
        for (const event of events) {
            if (event.kind === 'step') {
                flush();
                current = { kind: 'step', step: event.step };
                blocks.push(current);
            } else if (event.kind === 'request') {
                flush();
                current = { kind: 'exchange', x: event.x, number: numbers.get(event.x) ?? 0, items: [] };
                blocks.push(current);
            } else if (current !== undefined && current.kind !== 'step' && current.x === event.x) {
                addItem(current.items, event.item);
            } else if (event.item.kind === 'routine') {
                const items = pending.get(event.x) ?? [];
                pending.set(event.x, items);
                addItem(items, event.item);
            } else {
                const items = pending.get(event.x) ?? [];
                pending.delete(event.x);
                addItem(items, event.item);
                current = { kind: 'continued', x: event.x, number: numbers.get(event.x) ?? 0, items };
                blocks.push(current);
            }
        }
        flush();
        return blocks;
    }

    private renderBlock(block: Block): string[] {
        if (block.kind === 'step') return [`#### ${block.step.title}`, '', block.step.text];
        const { x } = block;
        if (block.kind === 'continued') {
            return [
                `##### Request ${block.number}, continued: stream \`${x.stream?.name ?? '?'}\``,
                '',
                `${x.to} → ${x.from}, on the stream opened by Request ${block.number}`,
                '',
                ...this.renderItems(x, block.items)
            ];
        }
        const context = this.context();
        const lines = [
            `##### Request ${block.number}: ${exchangeTitle(x)}`,
            '',
            `${x.from} → ${x.to}`,
            '',
            explain(x, context),
            '',
            `**Request** · ${context.time(x.at)}`,
            '',
            ...fence(this.requestLines(x))
        ];
        if (x.failure) {
            lines.push('', `**Response** · ${context.time(x.failure.at)}: none; ${x.failure.text}.`);
        } else if (x.response) {
            lines.push('', `**Response** · ${context.time(x.response.at)}`, '', ...fence(this.responseLines(x)));
            if (x.stream) {
                lines.push(
                    '',
                    'The response is a stream that stays open. Its messages, as they arrived:',
                    '',
                    ...this.renderItems(x, block.items)
                );
            }
        }
        return lines;
    }

    private renderItems(x: Exchange, items: Item[]): string[] {
        const context = this.context();
        const lines: string[] = [];
        for (const item of items) {
            switch (item.kind) {
                case 'routine': {
                    const when = item.from === item.to ? context.time(item.from) : `${context.time(item.from)} to ${context.time(item.to)}`;
                    lines.push(`**${when}** · ${routineText(item)}`, '');
                    break;
                }
                case 'expired': {
                    lines.push(`**${context.time(item.at)}** · **The token that opened the stream expires. The stream stays open.**`, '');
                    break;
                }
                case 'message': {
                    const allNew = isNewMessage(item.message);
                    const body = jsonLines(item.message, key => isNewMessageKey(item.message, key), allNew);
                    lines.push(`**${context.time(item.at)}** · ${caption(x, item.message, item.at)}`, '', ...fence(body), '');
                    break;
                }
                case 'end': {
                    lines.push(`**${context.time(item.at)}** · ${endText(x, item.how)}`, '');
                    break;
                }
            }
        }
        return lines.slice(0, -1);
    }

    private requestLines(x: Exchange): Line[] {
        const allNew = isNewRequest(x);
        const lines: Line[] = [[allNew, `${x.method} ${x.url.pathname}`]];
        const token = bearer(x);
        if (token !== undefined) lines.push([allNew, `Authorization: Bearer ‹${this.tokenLabel(token, x.at)}›`]);
        for (const [name, label] of [
            ['mcp-method', 'Mcp-Method'],
            ['mcp-name', 'Mcp-Name']
        ] as const) {
            const value = x.headers.get(name);
            if (value) lines.push([allNew, `${label}: ${value}`]);
        }
        if (x.form) {
            lines.push([false, '']);
            for (const [key, value] of x.form) lines.push([allNew, `${key}=${key === 'refresh_token' ? '‹refresh token›' : value}`]);
        } else if (x.rpc) {
            lines.push([false, ''], ...jsonLines(specEnvelope(x), key => isNewRequestKey(x, key), allNew));
        }
        return lines;
    }

    private responseLines(x: Exchange): Line[] {
        const response = x.response;
        if (!response) return [];
        const allNew = isNewResponse(x);
        const lines: Line[] = [[allNew, `${response.status} ${response.statusText}`]];
        if (x.stream) lines.push([false, 'Content-Type: text/event-stream']);
        const challenge = response.headers.get('www-authenticate');
        if (challenge) lines.push([allNew, `WWW-Authenticate: ${challenge}`]);
        if (response.json) lines.push([false, ''], ...jsonLines(this.redact(response.json), () => false, allNew));
        else if (response.text.length > 0) lines.push([false, ''], [allNew, response.text]);
        return lines;
    }

    private context(): Context {
        return {
            time: at => `+${((at - this.started) / 1000).toFixed(2)} s`,
            token: x => this.token(x),
            issued: x => {
                const token = x.response?.json?.access_token;
                return typeof token === 'string' ? this.tokens.get(token) : undefined;
            },
            number: x => this.exchanges.filter(other => other.scenario === x.scenario && !isLeftOut(other)).indexOf(x) + 1 || undefined,
            others: x => this.exchanges.filter(other => other.scenario === x.scenario && !isLeftOut(other)),
            openStreams: x =>
                this.exchanges.filter(
                    other =>
                        other !== x &&
                        other.scenario === x.scenario &&
                        other.stream !== undefined &&
                        other.at < x.at &&
                        bearer(other) === bearer(x) &&
                        (streamEnd(other)?.at ?? Number.POSITIVE_INFINITY) > x.at
                )
        };
    }

    private token(x: Exchange): TokenInfo | undefined {
        const token = bearer(x);
        return token === undefined ? undefined : this.tokens.get(token);
    }

    private tokenLabel(token: string, at: number): string {
        const info = this.tokens.get(token);
        if (!info) return 'a token not issued in this log';
        const parts = [`token #${info.number}`];
        if (info.subject) parts.push(info.subject);
        if (info.scope) parts.push(info.scope);
        if (info.expiresAt !== undefined) {
            parts.push(info.expiresAt > at ? `expires in ${seconds(info.expiresAt - at)}` : `expired ${seconds(at - info.expiresAt)} ago`);
        }
        return parts.join(', ');
    }

    private redact(json: JsonObject): JsonObject {
        const copy: JsonObject = { ...json };
        if (typeof copy.access_token === 'string') copy.access_token = `‹token #${this.tokens.get(copy.access_token)?.number ?? '?'}›`;
        if (typeof copy.refresh_token === 'string') copy.refresh_token = '‹refresh token›';
        return copy;
    }
}

// ---------------------------------------------------------------------------------------------
// What the Subscription Lifecycle proposal, and the Authorization Lifetime proposal it builds on,
// change, and how the log explains it.
// ---------------------------------------------------------------------------------------------

/** The draft SEP's `AuthorizationEnded` error code. */
const AUTHORIZATION_ENDED = -32_028;
/** The client capability that stands in for the draft protocol version. */
const DRAFT_CAPABILITY = 'io.modelcontextprotocol/subscription-lifetime';
/** Acknowledgment fields the proposals add. */
const NEW_ACK_KEYS = new Set(['authorizedUntil', 'expiresAt', 'lastUpdatedAt', 'streamId', 'lifecycle']);

function isLeftOut(x: Exchange): boolean {
    const method = x.rpc?.method;
    if (method === 'server/discover') return true;
    if (method === 'tools/call') return isObject(x.rpc?.params) && x.rpc.params.name === 'demo-status';
    return method === 'resources/read' && x.response !== undefined && x.response.status < 300;
}

function isNewRequest(x: Exchange): boolean {
    return x.rpc?.method === 'subscriptions/update';
}

function isNewRequestKey(x: Exchange, key: string): boolean {
    if (key === PROTOCOL_VERSION_META_KEY) return speaksNext(x);
    return x.rpc?.method === 'subscriptions/listen' && (key === 'lifecycle' || key === 'expiresAt');
}

function isNewResponse(x: Exchange): boolean {
    return x.rpc?.method === 'subscriptions/update' && x.response !== undefined && x.response.status < 300;
}

function isNewMessage(message: JsonObject): boolean {
    if (message.method === 'notifications/subscriptions/lifecycle') return true;
    return isObject(message.error) && message.error.code === AUTHORIZATION_ENDED;
}

function isNewMessageKey(message: JsonObject, key: string): boolean {
    return message.method === 'notifications/subscriptions/acknowledged' && NEW_ACK_KEYS.has(key);
}

function exchangeTitle(x: Exchange): string {
    if (x.form) {
        if (x.form.get('grant_type') === 'refresh_token') return 'Refresh the access token';
        return scopeOf(x).includes('files:audit') ? 'Get an access token with more scope' : 'Get an access token';
    }
    switch (x.rpc?.method) {
        case 'subscriptions/listen': {
            return 'Open a subscription';
        }
        case 'subscriptions/update': {
            return 'Update the subscription in place';
        }
        case 'notifications/cancelled': {
            return 'Cancel a subscription';
        }
        case 'resources/read': {
            return 'Read a resource';
        }
        default: {
            return `\`${String(x.rpc?.method ?? x.method)}\``;
        }
    }
}

function explain(x: Exchange, context: Context): string {
    if (x.form) return explainToken(x, context);
    const params = isObject(x.rpc?.params) ? x.rpc.params : {};
    switch (x.rpc?.method) {
        case 'subscriptions/listen': {
            return explainListen(x, params, context);
        }
        case 'subscriptions/update': {
            return explainUpdate(x, params, context);
        }
        case 'notifications/cancelled': {
            return `Cancels stream \`${String(params.requestId)}\`, which the client no longer needs. The server ends the stream and answers \`202\`.`;
        }
        case 'resources/read': {
            const sentences = [`Reads \`${fileName(params.uri)}\`.`];
            if (x.response?.status === 401) sentences.push('The token has expired, so the server refuses the request with `401`.');
            const open = context.openStreams(x);
            if (open.length > 0) {
                const names = open.map(other => `stream \`${other.stream?.name}\` (Request ${context.number(other) ?? '?'})`);
                sentences.push(`**Yet ${joinList(names)}, opened with the same token, is still delivering.**`);
            }
            return sentences.join(' ');
        }
        default: {
            return '';
        }
    }
}

function explainToken(x: Exchange, context: Context): string {
    const issued = context.issued(x);
    const lifetime = x.response?.json?.expires_in;
    const result = issued ? ` The authorization server issues token #${issued.number}, valid for ${String(lifetime)} s.` : '';
    if (x.response?.json?.error === 'invalid_grant') {
        return 'The client tries to refresh its token without the user, but the authorization server has revoked the grant. The client stops here and reports the subscription lost, without prompting the user (Lifetime §7 rule 4).';
    }
    if (x.form?.get('grant_type') === 'refresh_token') {
        return `Refreshes the access token without involving the user (Lifetime §6 rule 2).${result}`;
    }
    if (scopeOf(x).includes('files:audit')) {
        return `Asks for the wider scope that the server's \`403\` challenge named. A real client first asks the user, naming both the MCP server and the authorization server; the demo confirms automatically (Lifetime §7 rules 1 and 2).${result}`;
    }
    return `Gets an access token for ${x.form?.get('subject') ?? 'the user'} with scope \`${scopeOf(x).join(' ')}\`.${result}`;
}

function explainListen(x: Exchange, params: JsonObject, context: Context): string {
    const sentences = [`Opens stream \`${String(x.rpc?.id)}\` for ${describeFilter(params.notifications)}.`];
    if (declaresDraft(x.rpc) && x.to === CURRENT_SERVER) {
        sentences.push(
            'The client supports the next protocol revision, but version discovery showed that this server does not, so it speaks `2026-07-28` here.'
        );
    }
    const optedIn = isObject(params.notifications) && params.notifications.lifecycle === true;
    const asks: string[] = [];
    if (optedIn) asks.push('lifecycle notifications, with `lifecycle: true` (Lifecycle §3.1)');
    if (typeof params.expiresAt === 'string') {
        asks.push(`an end time, \`expiresAt\`, ${seconds(Date.parse(params.expiresAt) - x.at)} away (Lifecycle §2)`);
    }
    if (asks.length > 0) sentences.push(`**New:** the client asks for ${joinList(asks)}.`);
    const status = x.response?.status;
    if (status === 401) {
        sentences.push(
            'The token has expired, so the server refuses the request with `401` before any stream opens (Lifetime §5 rule 3). The client refreshes its token and tries again, as it does today.'
        );
        return sentences.join(' ');
    }
    if (!x.stream) return sentences.join(' ');
    const ack = ackOf(x);
    if (x.to === CURRENT_SERVER) {
        sentences.push(
            optedIn
                ? "The current server ignores what the client asked for: the acknowledgment has no `lifecycle`, no `streamId`, and no `authorizedUntil`. The client cannot update the stream in place, and nothing tells it when the stream will stop, so it falls back to reopening the stream on its own schedule, from its token's expiry (Lifecycle §3.1)."
                : 'The acknowledgment has no `authorizedUntil`: a current server does not tie the stream to the token that opened it.'
        );
        const expiresAt = context.token(x)?.expiresAt;
        const late = x.stream.events.filter(event => 'message' in event && expiresAt !== undefined && event.at > expiresAt).length;
        if (expiresAt !== undefined && late > 0) {
            sentences.push(`**After its token expired at ${context.time(expiresAt)}, the stream still delivered ${late} notifications.**`);
        }
    } else {
        const fields = ['authorizedUntil', 'expiresAt', 'lastUpdatedAt', 'streamId'].filter(key => ack !== undefined && key in ack);
        const list = joinList(fields.map(key => `\`${key}\``));
        sentences.push(
            declaresDraft(x.rpc)
                ? `**New:** the acknowledgment confirms \`lifecycle\` and carries ${list}.`
                : `**New:** the acknowledgment carries ${list}, which this client ignores. It did not ask for lifecycle notifications, so it gets none.`
        );
    }
    sentences.push(endSummary(x, context));
    return sentences.join(' ');
}

function explainUpdate(x: Exchange, params: JsonObject, context: Context): string {
    const target = context.others(x).find(other => ackOf(other)?.streamId === params.streamId);
    const name = target ? `stream \`${target.stream?.name}\` (Request ${context.number(target) ?? '?'})` : 'the stream';
    const sentences = [
        `**New:** re-authorizes ${name} in place with the token on this request. \`streamId\` names the stream, and \`Mcp-Name\` carries it, so that a load balancer can route the request to the server instance holding the stream (Lifecycle §4).`
    ];
    const response = x.response;
    const challenge = response?.headers.get('www-authenticate') ?? '';
    if (response?.status === 403 && challenge.includes('insufficient_scope')) {
        sentences.push(
            'The token lacks the scope the server now requires, so the server refuses the update with `403` and a `WWW-Authenticate` challenge, as it would any request. The stream carries on unchanged (Lifecycle §4 rules 2 and 4). The challenge, not the reminder, tells the client what to obtain (Lifetime §7 rule 1).'
        );
    } else if (response?.status === 200 && isObject(response.json?.result)) {
        const until = response.json.result.authorizedUntil;
        if (typeof until === 'string') {
            sentences.push(
                `The stream carries on, on the same connection, under the new token; its deadline is now ${seconds(Date.parse(until) - response.at)} away (Lifecycle §4 rule 5).`
            );
        }
        if (target && wasPaused(target, x, context)) {
            sentences.push(
                '**The stream was paused, so it resumes: the notifications the server held for it follow at once, on the same stream (Lifecycle §5 rules 4 and 5).**'
            );
        }
    } else if (response) {
        sentences.push('The server refuses the update; a refused update changes nothing (Lifecycle §4 rule 4).');
    }
    return sentences.join(' ');
}

/** Whether `stream` was paused when `update` was sent: its last reminder before then came after its deadline. */
function wasPaused(stream: Exchange, update: Exchange, context: Context): boolean {
    let last: { at: number; until: number } | undefined;
    for (const event of stream.stream?.events ?? []) {
        if (!('message' in event) || event.at >= update.at || !isReminder(event.message)) continue;
        const params = isObject(event.message.params) ? event.message.params : {};
        last = { at: event.at, until: typeof params.authorizedUntil === 'string' ? Date.parse(params.authorizedUntil) : Number.NaN };
    }
    if (last === undefined || !(last.until <= last.at)) return false;
    const since = last.at;
    return !context
        .others(update)
        .some(
            other =>
                other.rpc?.method === 'subscriptions/update' && other.at > since && other.at < update.at && other.response?.status === 200
        );
}

function endSummary(x: Exchange, context: Context): string {
    const end = streamEnd(x);
    if (end === undefined) return 'The stream was still open when the demo ended.';
    const when = context.time(end.at);
    switch (end.end) {
        case 'final': {
            const last = x.stream?.events.findLast(event => 'message' in event);
            const error = last && 'message' in last && isObject(last.message.error) ? last.message.error : undefined;
            const reason = error && isObject(error.data) ? error.data.reason : undefined;
            if (reason !== undefined)
                return `**New:** the stream ends at ${when} with \`AuthorizationEnded\`, reason \`${String(reason)}\`.`;
            return reachedExpiry(x, end.at)
                ? `The stream ends at ${when}, at its \`expiresAt\`, with the completion result.`
                : `The stream ends at ${when} with the completion result.`;
        }
        case 'client': {
            return `The client closes the stream at ${when}.`;
        }
        case 'server': {
            return x.to === PROPOSED_SERVER
                ? `**New:** the server closes the stream at ${when}, at its deadline, without a response.`
                : `The stream closes at ${when} without a response.`;
        }
        case 'error': {
            return `The connection fails at ${when}.`;
        }
    }
}

function caption(x: Exchange, message: JsonObject, at: number): string {
    const params = isObject(message.params) ? message.params : {};
    if (message.method === 'notifications/subscriptions/acknowledged') return ackCaption(x, params, at);
    if (message.method === 'notifications/subscriptions/lifecycle') return lifecycleCaption(x, params, at);
    if (isObject(message.error) && message.error.code === AUTHORIZATION_ENDED) {
        const reason = isObject(message.error.data) ? message.error.data.reason : undefined;
        return `**Last message: \`AuthorizationEnded\`, reason \`${String(reason)}\`.** ${reasonText(reason)}`;
    }
    if (isObject(message.result) && message.result.resultType === 'complete') {
        return reachedExpiry(x, at)
            ? '**Last message: the completion result.** The stream reached the `expiresAt` the client asked for, and ends normally, paused or not (Lifecycle §2 rule 3).'
            : '**Last message: the completion result.** The server ended the stream for an operational reason, not an authorization one.';
    }
    return `\`${String(message.method)}\``;
}

function ackCaption(x: Exchange, params: JsonObject, at: number): string {
    const notifications = isObject(params.notifications) ? params.notifications : {};
    const lifecycle = notifications.lifecycle === true;
    const sentences = ['**Acknowledgment.**'];
    if (lifecycle)
        sentences.push('`lifecycle: true` confirms that the server will send lifecycle notifications on this stream (Lifecycle §3.1).');
    if (typeof params.streamId === 'string')
        sentences.push('`streamId` names the stream for `subscriptions/update` (Lifecycle §4 rule 1).');
    if (typeof params.authorizedUntil === 'string') {
        sentences.push(
            `\`authorizedUntil\`, the authorization deadline, is ${seconds(Date.parse(params.authorizedUntil) - at)} away (Lifetime §4).`
        );
        if (lifecycle && typeof params.streamId === 'string') {
            sentences.push('If the client has not updated the stream by then, the stream pauses rather than ends (Lifecycle §5).');
        }
    } else {
        const requested = isObject(x.rpc?.params) && isObject(x.rpc.params.notifications) && x.rpc.params.notifications.lifecycle === true;
        sentences.push(
            requested
                ? 'No `lifecycle`, `streamId`, or `authorizedUntil`: the current server ignored the opt-in.'
                : 'No `authorizedUntil`: nothing tells the client when the stream will stop.'
        );
    }
    if (typeof params.expiresAt === 'string') {
        const asked = isObject(x.rpc?.params) && typeof x.rpc.params.expiresAt === 'string';
        sentences.push(
            `\`expiresAt\`: the stream ends ${seconds(Date.parse(params.expiresAt) - at)} from now, ${asked ? 'as the client asked' : "the server's maximum"} (Lifecycle §2).`
        );
    }
    if (typeof params.lastUpdatedAt === 'string') {
        sentences.push('`lastUpdatedAt` lets the client discard reminders that a later update has superseded (Lifecycle §3.2).');
    }
    if (x.to === PROPOSED_SERVER && !declaresDraft(x.rpc)) sentences.push('This client predates both proposals and ignores these fields.');
    const requested = isObject(x.rpc?.params) ? x.rpc.params.notifications : undefined;
    const missing = notAcknowledged(requested, params.notifications);
    if (missing.length > 0) {
        sentences.push(
            `**New:** ${joinList(missing)} was requested but is not acknowledged: the token no longer covers it (Lifetime §3 rule 5).`
        );
    }
    return sentences.join(' ');
}

function lifecycleCaption(x: Exchange, params: JsonObject, at: number): string {
    switch (params.type) {
        case 'reauthorization_required': {
            const until = typeof params.authorizedUntil === 'string' ? Date.parse(params.authorizedUntil) : Number.NaN;
            if (params.reason === 'insufficient_authorization') {
                return `**Reminder, reason \`insufficient_authorization\`.** The server's policy now requires more than the token provides, starting ${seconds(until - at)} from now. The deadline moved earlier, so the reminder comes at once (Lifecycle §3.3). It does not say what is required: the client sends an update with its current token, and the \`403\` answer says.`;
            }
            if (until <= at) {
                return `**Reminder on a paused stream.** The deadline passed ${seconds(at - until)} ago without an update, so the stream now carries nothing but these reminders, at most once a minute, until the client updates it (Lifecycle §5).`;
            }
            return `**Reminder.** The stream's authorization ends ${seconds(until - at)} from now. To keep the stream running, the client updates it before then (Lifecycle §3.3).`;
        }
        case 'access_reduced': {
            const held = (x.stream?.events ?? []).some(
                event => 'message' in event && event.at >= at && event.at - at < 100 && isReminder(event.message)
            );
            const timing = held
                ? ' The server held this notice until its next reminder, so its timing does not reveal when the resource changed (§3.4 rule 3).'
                : '';
            return `**\`access_reduced\`.** The stream no longer carries ${describeFilter(params.removed)}: the token no longer covers it, so the server removed it for good (Lifecycle §3.4).${timing}`;
        }
        case 'missed': {
            return '**`missed`.** Notifications may have been dropped; the client resynchronizes (Lifecycle §3.5).';
        }
        default: {
            return `**Lifecycle notification \`${String(params.type)}\`.** The client does not recognize this type, so it ignores it (Lifecycle §3.2).`;
        }
    }
}

function reasonText(reason: unknown): string {
    switch (reason) {
        case 'token_expiry': {
            return 'The stream reached its authorization deadline. The client refreshes its token without the user, opens a new stream, and resynchronizes (Lifetime §5, §6).';
        }
        case 'insufficient_authorization': {
            return 'The server now requires more than the token provides. The client sends a request with its current token, and the `401` or `403` answer says what is required (Lifetime §5, §7 rule 1).';
        }
        case 'revoked': {
            return "The stream's authorization was revoked, or none of its resources is authorized any more. The client must not prompt the user because of it: it may try once without the user, then reports the loss (Lifetime §5, §7 rule 4).";
        }
        default: {
            return 'The client does not recognize the reason, so it treats it as `token_expiry` (Lifetime §5 client rule 1).';
        }
    }
}

function endText(x: Exchange, how: EndHow): string {
    switch (how) {
        case 'final': {
            return 'The stream closes after its last message.';
        }
        case 'client': {
            return 'The client closes the stream.';
        }
        case 'error': {
            return 'The connection fails.';
        }
        case 'server': {
            if (x.to !== PROPOSED_SERVER) return 'The stream closes without a response.';
            return declaresDraft(x.rpc)
                ? 'The stream closes without a response: an unexpected disconnect.'
                : "**New: the stream closes without a response.** The stream's authorization ended, and this client speaks the current protocol revision, so the server ends the stream the way that revision understands: as an ordinary disconnect (Lifetime §5 rule 4).";
        }
    }
}

function ackOf(x: Exchange): JsonObject | undefined {
    for (const event of x.stream?.events ?? []) {
        if ('message' in event && event.message.method === 'notifications/subscriptions/acknowledged' && isObject(event.message.params)) {
            return event.message.params;
        }
    }
    return undefined;
}

function isReminder(message: JsonObject): boolean {
    return (
        message.method === 'notifications/subscriptions/lifecycle' &&
        isObject(message.params) &&
        message.params.type === 'reauthorization_required'
    );
}

function reachedExpiry(x: Exchange, at: number): boolean {
    const expiresAt = ackOf(x)?.expiresAt;
    return typeof expiresAt === 'string' && at >= Date.parse(expiresAt) - 1000;
}

// ---------------------------------------------------------------------------------------------
// Helpers.
// ---------------------------------------------------------------------------------------------

function describeRequest(scenario: string, from: string, url: URL, init: RequestInit | undefined): Exchange {
    const x: Exchange = {
        scenario,
        from,
        to: serverLabel(url),
        at: Date.now(),
        method: init?.method ?? 'GET',
        url,
        headers: new Headers(init?.headers)
    };
    const body = typeof init?.body === 'string' ? init.body : init?.body instanceof URLSearchParams ? init.body.toString() : undefined;
    if (body === undefined) return x;
    if (x.to === AUTHORIZATION_SERVER) x.form = new URLSearchParams(body);
    else x.rpc = parseObject(body);
    return x;
}

function serverLabel(url: URL): string {
    if (url.pathname === '/token') return AUTHORIZATION_SERVER;
    return url.pathname === CURRENT_SERVER_PATH ? CURRENT_SERVER : PROPOSED_SERVER;
}

/** Passes a stream's bytes through unchanged, recording each message as it arrives. */
function watch(events: StreamEvent[], body: ReadableStream<Uint8Array>): ReadableStream<Uint8Array> {
    const reader = body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';
    let sawFinal = false;
    let ended = false;
    const end = (how: EndHow): void => {
        if (ended) return;
        ended = true;
        events.push({ at: Date.now(), end: sawFinal ? 'final' : how });
    };
    return new ReadableStream<Uint8Array>({
        pull: async controller => {
            try {
                const { done, value } = await reader.read();
                if (done) {
                    end('server');
                    controller.close();
                    return;
                }
                buffer += decoder.decode(value, { stream: true }).replaceAll('\r\n', '\n');
                for (let index = buffer.indexOf('\n\n'); index !== -1; index = buffer.indexOf('\n\n')) {
                    const message = parseObject(sseData(buffer.slice(0, index)));
                    buffer = buffer.slice(index + 2);
                    if (!message) continue;
                    if ('result' in message || 'error' in message) sawFinal = true;
                    events.push({ at: Date.now(), message });
                }
                controller.enqueue(value);
            } catch (error) {
                end(error instanceof Error && error.name === 'AbortError' ? 'client' : 'error');
                controller.error(error);
            }
        },
        cancel: async reason => {
            end('client');
            await reader.cancel(reason);
        }
    });
}

function toItem(event: StreamEvent, expiresAt: number | undefined): Item {
    if ('end' in event) return { kind: 'end', at: event.at, how: event.end };
    const { message } = event;
    if (message.method === 'notifications/resources/updated') {
        const uri = isObject(message.params) ? message.params.uri : undefined;
        return {
            kind: 'routine',
            from: event.at,
            to: event.at,
            counts: new Map([[fileName(uri), 1]]),
            afterExpiry: expiresAt !== undefined && event.at > expiresAt
        };
    }
    return { kind: 'message', at: event.at, message };
}

/** The latest notable stream message that arrived at most 50 ms before `at`. */
function latestMessage<T extends { at: number; order: number; kind: string }>(events: T[], at: number): T | undefined {
    let latest: T | undefined;
    for (const event of events) {
        if (event.kind !== 'item' || !('item' in event)) continue;
        const { item } = event as T & { item: Item };
        if (item.kind !== 'message' || event.at > at || at - event.at > 50) continue;
        if (latest === undefined || event.at > latest.at || (event.at === latest.at && event.order > latest.order)) latest = event;
    }
    return latest;
}

function addItem(items: Item[], item: Item): void {
    const last = items.at(-1);
    if (item.kind === 'routine' && last?.kind === 'routine' && last.afterExpiry === item.afterExpiry) {
        last.to = item.to;
        for (const [name, count] of item.counts) last.counts.set(name, (last.counts.get(name) ?? 0) + count);
        return;
    }
    items.push(item);
}

function routineText(item: Routine): string {
    const counts = [...item.counts].map(([name, count]) => `\`${name}\` ×${count}`);
    const after = item.afterExpiry ? ' — **after the token expired**' : '';
    return `Resource changes (\`notifications/resources/updated\`): ${joinList(counts)}${after}`;
}

function streamEnd(x: Exchange): { at: number; end: EndHow } | undefined {
    for (const event of x.stream?.events ?? []) if ('end' in event) return event;
    return undefined;
}

function bearer(x: Exchange): string | undefined {
    const authorization = x.headers.get('authorization');
    return authorization?.startsWith('Bearer ') ? authorization.slice('Bearer '.length) : undefined;
}

function scopeOf(x: Exchange): string[] {
    return (x.form?.get('scope') ?? '').split(' ').filter(Boolean);
}

function declaresDraft(rpc: JsonObject | undefined): boolean {
    const meta = isObject(rpc?.params) && isObject(rpc.params._meta) ? rpc.params._meta : {};
    const capabilities = meta[CLIENT_CAPABILITIES_META_KEY];
    return isObject(capabilities) && isObject(capabilities.experimental) && DRAFT_CAPABILITY in capabilities.experimental;
}

/** The placeholder for the protocol revision that adopts the proposal. */
const NEXT_REVISION = '‹vNext›';

/** Whether `x` is a proposed client talking to a proposed server, which would speak the next protocol revision. */
function speaksNext(x: Exchange): boolean {
    return declaresDraft(x.rpc) && x.to === PROPOSED_SERVER;
}

/**
 * The request with its `_meta` envelope as the specification will have it: the next protocol revision where
 * the client and server would both speak it, and without the prototype's stand-in capability.
 */
function specEnvelope(x: Exchange): JsonObject {
    const rpc = x.rpc ?? {};
    if (!isObject(rpc.params) || !isObject(rpc.params._meta)) return rpc;
    const meta: JsonObject = { ...rpc.params._meta };
    if (speaksNext(x) && PROTOCOL_VERSION_META_KEY in meta) meta[PROTOCOL_VERSION_META_KEY] = NEXT_REVISION;
    if (CLIENT_CAPABILITIES_META_KEY in meta) meta[CLIENT_CAPABILITIES_META_KEY] = withoutStandIn(meta[CLIENT_CAPABILITIES_META_KEY]);
    return { ...rpc, params: { ...rpc.params, _meta: meta } };
}

function withoutStandIn(capabilities: unknown): unknown {
    if (!isObject(capabilities) || !isObject(capabilities.experimental)) return capabilities;
    const experimental = Object.fromEntries(Object.entries(capabilities.experimental).filter(([key]) => key !== DRAFT_CAPABILITY));
    const rest = Object.fromEntries(Object.entries(capabilities).filter(([key]) => key !== 'experimental'));
    return Object.keys(experimental).length > 0 ? { ...rest, experimental } : rest;
}

function jsonLines(value: unknown, isNewKey: (key: string) => boolean, allNew: boolean): Line[] {
    return pretty(value)
        .split('\n')
        .map((text): Line => {
            const key = /^\s*"([^"]+)":/.exec(text)?.[1];
            return [allNew || (key !== undefined && isNewKey(key)), text];
        });
}

function fence(lines: Line[]): string[] {
    return ['```diff', ...lines.map(([isNew, text]) => `${isNew ? '+' : ' '}${text}`), '```'];
}

function describeFilter(filter: unknown): string {
    if (!isObject(filter)) return 'nothing';
    const parts = Array.isArray(filter.resourceSubscriptions) ? filter.resourceSubscriptions.map(uri => `\`${fileName(uri)}\``) : [];
    if (filter.toolsListChanged === true) parts.push('tool list changes');
    if (filter.promptsListChanged === true) parts.push('prompt list changes');
    if (filter.resourcesListChanged === true) parts.push('resource list changes');
    return parts.length > 0 ? joinList(parts) : 'nothing';
}

function notAcknowledged(requested: unknown, acknowledged: unknown): string[] {
    const wanted = isObject(requested) && Array.isArray(requested.resourceSubscriptions) ? requested.resourceSubscriptions : [];
    const got = isObject(acknowledged) && Array.isArray(acknowledged.resourceSubscriptions) ? acknowledged.resourceSubscriptions : [];
    return wanted.filter(uri => !got.includes(uri)).map(uri => `\`${fileName(uri)}\``);
}

function scenarioHeading(scenario: Scenario): string {
    return `${scenario.label.includes(' and ') ? 'Scenarios' : 'Scenario'} ${scenario.label} - ${scenario.title}`;
}

/** The anchor GitHub gives a heading. */
function slug(heading: string): string {
    return heading
        .toLowerCase()
        .replaceAll(/[^\p{L}\p{N}\s_-]/gu, '')
        .replaceAll(/\s/g, '-');
}

function fileName(uri: unknown): string {
    return String(uri).split('/').at(-1) ?? String(uri);
}

function joinList(items: string[]): string {
    if (items.length <= 2) return items.join(' and ');
    return `${items.slice(0, -1).join(', ')}, and ${items.at(-1)}`;
}

function seconds(ms: number): string {
    return `${(ms / 1000).toFixed(1)} s`;
}

function sseData(frame: string): string {
    return frame
        .split('\n')
        .filter(line => line.startsWith('data:'))
        .map(line => line.slice('data:'.length).trimStart())
        .join('\n');
}

function jwtClaims(token: string): JsonObject {
    try {
        const claims: unknown = JSON.parse(Buffer.from(token.split('.')[1] ?? '', 'base64url').toString('utf8'));
        return isObject(claims) ? claims : {};
    } catch {
        return {};
    }
}

function parseObject(text: string): JsonObject | undefined {
    try {
        const value: unknown = JSON.parse(text);
        return isObject(value) ? value : undefined;
    } catch {
        return undefined;
    }
}

function pretty(value: unknown): string {
    return JSON.stringify(value, undefined, 2);
}

function responseInit(response: Response): ResponseInit {
    return { status: response.status, statusText: response.statusText, headers: response.headers };
}

function isObject(value: unknown): value is JsonObject {
    return typeof value === 'object' && value !== null && !Array.isArray(value);
}
