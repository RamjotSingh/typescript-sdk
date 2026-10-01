# Authorization lifetime demo

Runnable prototype story for the Authorization Lifetime for Subscription Streams SEP: streams report `authorizedUntil`, data stops at the authorization deadline, authorization ends are distinguishable for clients that implement the proposal, and other clients fall back to reconnecting after an ordinary disconnect.

The story is HTTP-only because it includes a toy authorization server and an MCP resource server behind bearer verification. It is self-verifying: the client exits non-zero if any expected step is missing.

## Run it

From the SDK repo root:

```powershell
pnpm --filter @mcp-examples/authorization-lifetime start:fast
```

POSIX shells use the same command. The fast run uses 20 second access tokens and takes about one minute.

Two-terminal form:

```powershell
$env:DEMO_ACCESS_TOKEN_SECONDS='20'
pnpm --filter @mcp-examples/authorization-lifetime server -- --http --port 3000
pnpm --filter @mcp-examples/authorization-lifetime client -- --http http://127.0.0.1:3000/mcp
```

Default mode omits `DEMO_ACCESS_TOKEN_SECONDS` and uses 120 second tokens.

## Scenarios

The server has two endpoints, which run at the same time: `/mcp` implements the proposal, and `/mcp-current` runs without it, as today's SDK does. The client runs three scenarios at once, each pairing a client and a server with or without the proposal. A proposed client declares the draft capability `io.modelcontextprotocol/subscription-lifetime`, which stands in for the new protocol version.

| Scenario | Client              | MCP server | What changes                                                                                                                                                               |
| -------- | ------------------- | ---------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1 and 2  | Current or proposed | Current    | Nothing: the stream keeps delivering after its token expires, and after the user loses access to a resource, while a new request with the same token is refused (the gap). |
| 3        | Proposed            | Proposed   | The whole proposal.                                                                                                                                                        |
| 4        | Current             | Proposed   | The stream stops at its deadline, as an ordinary disconnect; the client reconnects as it does today.                                                                       |

Scenarios 1 and 2 behave the same, so the demo runs them once, with a current client:

1. The client opens a stream on `config.json` and `case-114.md`. The acknowledgment has no `authorizedUntil`.
2. The token expires. A read with it gets HTTP 401, but the stream keeps delivering.
3. The user loses access to `case-114.md`, but the stream keeps delivering its changes.

Scenario 3, with a proposed client and server:

1. Stream A watches `[config.json, hr/case-114.md]` and stream B watches `[hr/case-114.md]`; each acknowledgment carries `authorizedUntil` (lifetime §4 rule 1).
2. A and B re-establish shortly before the first deadline with exactly one refresh shared across both streams, then cancel the old streams and resynchronize (§6 rules 2, 4, 5). This relies on the acknowledgment marking where delivery on the new stream starts, which the lifetime SEP proposes in a separate PR.
3. HR access is revoked during an HR change. A keeps config updates, HR notifications stop, and B stays open past the change instead of revealing timing (§3 rules 2, 5).
4. The second deadline is missed: A ends with `token_expiry`, B ends with `revoked`, and no notifications arrive after `authorizedUntil` (§3 rule 1, §4 rule 3, §5). A refreshes and reopens with its old filter. The server refuses the whole stream: instead of an acknowledgment it answers with a `-32602` error that names `case-114.md` in `error.data.denied`, so the client reopens A on `config.json` alone and reports the change in coverage. B makes one silent attempt, is refused the same way, and reports loss without prompting (§3 rule 5, §6 rule 4, §7 rule 4).
5. A policy change requires `files:audit`: A ends with `insufficient_authorization`; only the follow-up request receives the 403 challenge, then the client logs auto-confirmed consent naming both the MCP server and authorization server before step-up (§3 rule 3, §5, §7 rules 1-2).
6. The authorization server revokes the subject's grant and the MCP server revokes the stream. A ends at once with `revoked`; the client makes one silent refresh attempt, gets `invalid_grant`, and does not prompt (§5 reason table, §7 rule 4).

Scenario 4, with a current client and the proposed server:

1. The client opens a stream on `config.json`. The acknowledgment carries `authorizedUntil`, which the client ignores. At its deadline the stream closes without an `AuthorizationEnded` frame (§5 rule 4).
2. The client reconnects with the stale token and gets HTTP 401, then refreshes, reconnects, and resynchronizes (§5 rule 3, Backward Compatibility).

| Scenario and step             | Lifetime SEP rule exercised                                                                              |
| ----------------------------- | -------------------------------------------------------------------------------------------------------- |
| 1 and 2                       | None: the behavior the SEP changes                                                                       |
| 3.1 Ack deadlines             | §4 rule 1 (`authorizedUntil` in the acknowledgment)                                                      |
| 3.2 Pre-deadline re-establish | §6 rules 2, 4, 5 (one refresh per token, resync, randomized pre-deadline replacement)                    |
| 3.3 HR access reduction       | §3 rules 2, 5 (per-notification check; partial loss continues; total loss is not revealed at the change) |
| 3.4 Missed deadline, recovery | §3 rules 1, 5, §4 rule 3, §5, §6 rule 4, §7 rule 4 (a refusal that names what is not permitted)          |
| 3.5 Step-up                   | §3 rule 3, §5 `insufficient_authorization`, §7 rules 1-2                                                 |
| 3.6 Grant revocation          | §5 `revoked` reason table, §7 rule 4                                                                     |
| 4 Current client, new server  | §5 rules 3-4 and Backward Compatibility                                                                  |

## Wire log

The console shows the story step by step. The wire log shows what crossed the wire, grouped by scenario. It opens with the table above. Within each scenario, every request has its own heading: who sent it to whom, what it does and what came of it, then the request and the response. Lines that are new in the proposal start with `+`. A subscription's response is a stream, so its messages are listed as they arrived, with routine resource changes counted, and continue under later headings when other requests come in between.

The client writes the log as Markdown when the demo ends, to `mcp-subscription-demos/authorization-lifetime-wire-log.md` in the system temp folder, or to the path in `DEMO_WIRE_LOG`, and prints the path when it starts and when it finishes. Request envelopes (`_meta`) show the protocol version the specification would carry: `‹vNext›` when a proposed client talks to the proposed server, where the prototype actually sends `2026-07-28` and the stand-in capability. Version discovery, reads that only resynchronize, and the demo's own `demo-status` tool are left out. The logging is a fetch wrapper the clients pass to their transports, in [`wireLog.ts`](wireLog.ts).

## Known prototype deviations

- The draft protocol version is simulated by `experimental["io.modelcontextprotocol/subscription-lifetime"]`.
- The authorization server is a toy HS256 JWT issuer in the same process; it auto-consents to `files:audit`.
- Revocation and policy changes are scripted with the prototype control API rather than external introspection/shared signals.
- `/mcp-current` is the same SDK handler created without the proposal's options and given no `authInfo`, which is how today's SDK runs.
- The demo is single-process; distributed routing of streams and authorization signals is not shown.
