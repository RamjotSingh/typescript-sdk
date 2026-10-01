/**
 * Wire log for the Authorization Lifetime demo.
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

    /** Starts a step of `scenario`: a heading and a short narrative. */
    step(scenario: string, title: string, text: string): void {
        this.steps.push({ scenario, at: Date.now(), title, text });
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
        for (const step of this.steps) {
            if (step.scenario === scenario) events.push({ at: step.at, order: events.length, kind: 'step', step });
        }
        for (const x of shown) {
            events.push({ at: x.at, order: events.length, kind: 'request', x });
            const expiresAt = this.token(x)?.expiresAt;
            for (const event of x.stream?.events ?? []) {
                events.push({ at: event.at, order: events.length, kind: 'item', x, item: toItem(event, expiresAt) });
            }
            const end = streamEnd(x);
            if (x.stream && expiresAt !== undefined && (end === undefined || end.at > expiresAt + 1000)) {
                events.push({ at: expiresAt, order: events.length, kind: 'item', x, item: { kind: 'expired', at: expiresAt } });
            }
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
        if (response.json) lines.push([false, ''], ...jsonLines(this.redact(response.json), key => isNewResponseKey(x, key), allNew));
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
// What the Authorization Lifetime proposal changes, and how the log explains it.
// ---------------------------------------------------------------------------------------------

/** The draft SEP's `AuthorizationEnded` error code. */
const AUTHORIZATION_ENDED = -32_028;
/** The client capability that stands in for the draft protocol version. */
const DRAFT_CAPABILITY = 'io.modelcontextprotocol/subscription-lifetime';

function isLeftOut(x: Exchange): boolean {
    const method = x.rpc?.method;
    if (method === 'server/discover') return true;
    if (method === 'tools/call') return isObject(x.rpc?.params) && x.rpc.params.name === 'demo-status';
    return method === 'resources/read' && x.response !== undefined && x.response.status < 300;
}

function isNewRequest(_x: Exchange): boolean {
    return false;
}

function isNewRequestKey(x: Exchange, key: string): boolean {
    return key === PROTOCOL_VERSION_META_KEY && speaksNext(x);
}

function isNewResponse(_x: Exchange): boolean {
    return false;
}

function isNewResponseKey(x: Exchange, key: string): boolean {
    return key === 'denied' && deniedOf(x) !== undefined;
}

function isNewMessage(message: JsonObject): boolean {
    return isObject(message.error) && message.error.code === AUTHORIZATION_ENDED;
}

function isNewMessageKey(message: JsonObject, key: string): boolean {
    return message.method === 'notifications/subscriptions/acknowledged' && key === 'authorizedUntil';
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
        return 'The client tries to refresh its token without the user, but the authorization server has revoked the grant. The client stops here and reports the subscription lost, without prompting the user (§7 rule 4).';
    }
    if (x.form?.get('grant_type') === 'refresh_token') {
        return `Refreshes the access token without involving the user. One refresh serves every stream the old token authorized (§6 rule 2).${result}`;
    }
    if (scopeOf(x).includes('files:audit')) {
        return `Asks for the wider scope that the server's \`403\` challenge named. A real client first asks the user, naming both the MCP server and the authorization server; the demo confirms automatically (§7 rules 1 and 2).${result}`;
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
    const status = x.response?.status;
    const challenge = x.response?.headers.get('www-authenticate') ?? '';
    if (status === 401) {
        sentences.push(
            'The token has expired, so the server refuses the request with `401` before any stream opens: a stream that has not started gets an HTTP status, not `AuthorizationEnded` (§5 rule 3). The client refreshes its token and tries again, as it does today.'
        );
    } else if (status === 403 && challenge.includes('insufficient_scope')) {
        sentences.push(
            'The server refuses it with `403` and a `WWW-Authenticate` challenge that names the scope it now requires. The challenge answers a request the client chose to send, so only now may the client involve the user (§7 rule 1).'
        );
    } else if (deniedOf(x) !== undefined) {
        sentences.push(explainDenied(x, params));
    } else if (x.stream) {
        if (x.to === CURRENT_SERVER) {
            sentences.push(
                'The acknowledgment has no `authorizedUntil`: a current server does not tie the stream to the token that opened it.'
            );
            const expiresAt = context.token(x)?.expiresAt;
            const late = x.stream.events.filter(event => 'message' in event && expiresAt !== undefined && event.at > expiresAt).length;
            if (expiresAt !== undefined && late > 0) {
                sentences.push(
                    `**After its token expired at ${context.time(expiresAt)}, the stream still delivered ${late} notifications.**`
                );
            }
        } else if (declaresDraft(x.rpc)) {
            sentences.push("**New:** the acknowledgment carries `authorizedUntil`, the stream's authorization deadline.");
        } else {
            sentences.push('**New:** the acknowledgment carries `authorizedUntil`, which this client ignores.');
        }
        sentences.push(endSummary(x, context));
    }
    return sentences.join(' ');
}

/** The entries that the error answering a `subscriptions/listen` request names as not permitted, if it does. */
function deniedOf(x: Exchange): unknown {
    const error = x.rpc?.method === 'subscriptions/listen' ? x.response?.json?.error : undefined;
    return isObject(error) && isObject(error.data) ? error.data.denied : undefined;
}

/** Explains the refusal of a stream whose filter lists entries the user may not see. */
function explainDenied(x: Exchange, params: JsonObject): string {
    const denied = deniedOf(x);
    const names = describeFilter(denied);
    const kept = urisNotIn(params.notifications, denied);
    if (kept.length === 0) {
        return `**New:** the user can no longer read ${names}, so instead of an acknowledgment the server answers with error \`-32602\` and names ${names} in \`error.data.denied\` (§3 rule 5). After \`revoked\`, this was the one attempt the client may make without the user, so it reports the subscription lost (§7 rule 4).`;
    }
    return `**New:** the user can no longer read ${names}, so the server refuses the whole stream: instead of an acknowledgment, it answers with error \`-32602\` and names ${names} in \`error.data.denied\` (§3 rule 5). The refusal is a JSON-RPC error, the same on every transport, so on HTTP the status is \`200\`. A stream opens with everything it asks for or not at all, so the client always knows what it covers. The client reopens the stream on ${joinList(kept)}, and reports the change in coverage (§6 rule 4).`;
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
            return reason === undefined
                ? `The stream ends at ${when} with the completion result.`
                : `**New:** the stream ends at ${when} with \`AuthorizationEnded\`, reason \`${String(reason)}\`.`;
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
    if (message.method === 'notifications/subscriptions/acknowledged') {
        const sentences = ['**Acknowledgment.**'];
        if (typeof params.authorizedUntil === 'string') {
            sentences.push(
                `\`authorizedUntil\` is the stream's authorization deadline, ${seconds(Date.parse(params.authorizedUntil) - at)} away: the server writes nothing to the stream after it (§3 rule 1, §4).`
            );
            if (!declaresDraft(x.rpc)) sentences.push('This client predates the proposal and ignores it.');
        } else {
            sentences.push('No `authorizedUntil`: nothing tells the client when the stream will stop.');
        }
        return sentences.join(' ');
    }
    if (isObject(message.error) && message.error.code === AUTHORIZATION_ENDED) {
        const reason = isObject(message.error.data) ? message.error.data.reason : undefined;
        return `**Last message: \`AuthorizationEnded\`, reason \`${String(reason)}\`.** ${reasonText(reason)}`;
    }
    if (isObject(message.result) && message.result.resultType === 'complete') {
        return '**Last message: the completion result.** The server ended the stream for an operational reason, not an authorization one.';
    }
    return `\`${String(message.method)}\``;
}

function reasonText(reason: unknown): string {
    switch (reason) {
        case 'token_expiry': {
            return 'The stream reached its authorization deadline. The client refreshes its token without the user, opens a new stream, and resynchronizes (§5, §6).';
        }
        case 'insufficient_authorization': {
            return 'The server now requires more than the token provides. The error does not say what: the client sends a request with its current token, and the `401` or `403` answer says (§5, §7 rule 1).';
        }
        case 'revoked': {
            return "The stream's authorization was revoked, or none of its resources is authorized any more. The client must not prompt the user because of it: it may try once without the user, then reports the loss (§5, §7 rule 4).";
        }
        default: {
            return 'The client does not recognize the reason, so it treats it as `token_expiry` (§5 client rule 1).';
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
                : "**New: the stream closes without a response.** The stream's authorization ended, and this client speaks the current protocol revision, so the server ends the stream the way that revision understands: as an ordinary disconnect (§5 rule 4).";
        }
    }
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

/** JSON lines, marked new where `isNewKey` holds for their key; a new key's object or array value is new as a whole. */
function jsonLines(value: unknown, isNewKey: (key: string) => boolean, allNew: boolean): Line[] {
    let newBlockIndent: number | undefined;
    return pretty(value)
        .split('\n')
        .map((text): Line => {
            const indent = text.length - text.trimStart().length;
            if (newBlockIndent !== undefined) {
                if (indent === newBlockIndent) newBlockIndent = undefined;
                return [true, text];
            }
            const key = /^\s*"([^"]+)":/.exec(text)?.[1];
            const isNew = key !== undefined && isNewKey(key);
            if (isNew && /[[{]$/.test(text)) newBlockIndent = indent;
            return [allNew || isNew, text];
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

/** The resources `requested` lists that `other` does not, as file names. */
function urisNotIn(requested: unknown, other: unknown): string[] {
    const wanted = isObject(requested) && Array.isArray(requested.resourceSubscriptions) ? requested.resourceSubscriptions : [];
    const listed = isObject(other) && Array.isArray(other.resourceSubscriptions) ? other.resourceSubscriptions : [];
    return wanted.filter(uri => !listed.includes(uri)).map(uri => `\`${fileName(uri)}\``);
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
