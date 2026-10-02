# Subscription lifecycle demo

Runnable prototype story for the Subscription Lifecycle SEP. This SEP builds on the Authorization Lifetime SEP: deadlines and authorization-ended reasons are inherited, while this story demonstrates the lifecycle additions: lifecycle opt-in, reminders on the SEP's default schedule with jitter, in-place updates, pausing, held notifications, `access_reduced`, and stream expiry. For the Authorization Lifetime SEP alone, see `examples/authorization-lifetime`.

The story is HTTP-only because it includes a toy authorization server and an MCP resource server behind bearer verification. It is self-verifying: the client exits non-zero if any expected lifecycle step is missing.

## Run it

From the SDK repo root:

```powershell
pnpm --filter @mcp-examples/subscription-lifecycle start:fast
```

POSIX shells use the same command. The fast run uses 20 second access tokens and takes about two minutes because lifecycle SEP §5 repeats reminders on paused streams no more often than once per minute.

Two-terminal form:

```powershell
$env:DEMO_ACCESS_TOKEN_SECONDS='20'
pnpm --filter @mcp-examples/subscription-lifecycle server -- --http --port 3000
pnpm --filter @mcp-examples/subscription-lifecycle client -- --http http://127.0.0.1:3000/mcp
```

Default mode omits `DEMO_ACCESS_TOKEN_SECONDS` and uses 120 second tokens.

## Scenarios

The server has two endpoints, which run at the same time: `/mcp` implements both proposals, and `/mcp-current` runs without them, as today's SDK does. The client runs four scenarios at once, each pairing a client and a server with or without the proposals. A proposed client declares the draft capability `io.modelcontextprotocol/subscription-lifetime`, which stands in for the new protocol version, and opts in to lifecycle notifications.

| Scenario | Client   | MCP server | What changes                                                                                                                                                                     |
| -------- | -------- | ---------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1        | Current  | Current    | Nothing: the stream keeps delivering after its token expires, and after the user loses access to a resource (the gap).                                                           |
| 2        | Proposed | Current    | The opt-in is ignored: no `lifecycle`, `handle`, or `authorizedUntil` in the acknowledgment, so the client cannot update the stream in place and falls back to reopening it.     |
| 3        | Proposed | Proposed   | The whole proposal: reminders, updates in place, `access_reduced`, pausing, step-up through the update's `403`, and the end the client asked for.                                |
| 4        | Current  | Proposed   | No lifecycle features, which the client did not ask for. As under Authorization Lifetime, the stream stops at its deadline as an ordinary disconnect, and the client reconnects. |

A client that implements Authorization Lifetime but does not opt in to lifecycle notifications gets exactly the Authorization Lifetime behavior; `examples/authorization-lifetime` shows it.

Scenario 3, with a proposed client and server:

1. The client requests `lifecycle: true` and `expiresAt`; the acknowledgment includes lifecycle fields and a handle (lifecycle §2, §3.1, §4).
2. A pre-deadline `reauthorization_required` reminder arrives; the client refreshes once and calls `subscriptions/update` (lifecycle §3.3, §4; inherited lifetime deadline from lifetime §4).
3. HR access is revoked: the server stops writing HR changes at once, and reports the removed entry with `access_reduced` at its next reminder (lifecycle §3.4; inherited per-notification authorization from lifetime §3 rule 2).
4. The client intentionally misses one deadline; the stream pauses instead of ending, repeats a reminder, then resumes after a late update, and the held project notifications arrive at once (lifecycle §5, §3.5).
5. A policy change requires `files:audit`; an under-scoped update gets `403 insufficient_scope`, the demo "asks the user", obtains a wider token, and updates successfully (lifecycle §3.3 server rule 4 and client rule 2, §4 rule 2; inherited `insufficient_authorization` reason and consent rules from lifetime §5 and §7).
6. The stream ends gracefully at its requested `expiresAt` (lifecycle §2).

Scenarios 1, 2, and 4 are short: scenario 1 shows today's gap, scenario 2 a current server ignoring the opt-in, and scenario 4 a current client getting an ordinary disconnect at the deadline, then a `401`, a refresh, and a reconnect.

## Wire log

The console shows the story step by step. The wire log shows what crossed the wire, grouped by scenario. It opens with the table above. Within each scenario, every request has its own heading: who sent it to whom, what it does and what came of it, then the request and the response. Lines that are new in the proposals start with `+`. A subscription's response is a stream, so its messages are listed as they arrived, with routine resource changes counted, and continue under later headings when other requests come in between; after the late update in scenario 3, the held notifications show up as a burst.

The client writes the log as Markdown when the demo ends, to `mcp-subscription-demos/subscription-lifecycle-wire-log.md` in the system temp folder, or to the path in `DEMO_WIRE_LOG`, and prints the path when it starts and when it finishes. Request envelopes (`_meta`) show the protocol version the specification would carry: `‹vNext›` when a proposed client talks to the proposed server, where the prototype actually sends `2026-07-28` and the stand-in capability. Version discovery and reads that only resynchronize are left out. The logging is a fetch wrapper the clients pass to their transports, in [`wireLog.ts`](wireLog.ts).

## SEP coverage (scenario 3)

| Scenario                                                                           | Lifetime SEP rule inherited                                  | Lifecycle SEP rule                             |
| ---------------------------------------------------------------------------------- | ------------------------------------------------------------ | ---------------------------------------------- |
| Bearer gate returns `401` with `WWW-Authenticate` for missing/invalid/expired JWTs | §3 stream authorization; §6 re-establishing a stream         | §4 rule 2 update authorizes like any request   |
| Ack includes `authorizedUntil` from `AuthInfo.expiresAt`                           | §4 deadline in the acknowledgment                            | §3.3 reminders use the same deadline           |
| Client asks for `expiresAt` and receives a graceful end there                      | Existing graceful closure remains for non-authorization ends | §2 expiry                                      |
| Client requests `lifecycle: true` and receives a handle                            | n/a                                                          | §3.1 opt-in; §4 updating in place              |
| Client refreshes and calls `subscriptions/update` after a reminder                 | §6 fresh authorization                                       | §3.3 reminders; §4 update                      |
| Per-notification ACL removes the HR resource                                       | §3 rule 2 per-notification access check                      | §3.4 `access_reduced`                          |
| Client misses a deadline and the stream pauses                                     | §5 `token_expiry` would end a non-lifecycle stream           | §5 pausing                                     |
| Server holds/drops while paused, then resumes or reports `missed`                  | §3 rule 7 notifications must not move to another stream      | §3.5 `missed`; §5 rules 4-5 held notifications |
| Policy raises required scope to `files:audit`                                      | §5 `insufficient_authorization`; §7 user consent             | §3.3 server rule 4, client rule 2; §4 rule 2   |

## Timing report (scenario 3)

Fast runs use 20 second access tokens and the server's default lifecycle reminder schedule: for lifetimes under ten minutes, lifecycle §3.3 rule 2 schedules reminders at 10%, 5%, 3%, 2%, and 1% of the authorization lifetime, with first-reminder jitter. The 1498 ms entry is the reminder sent at once when the policy change moves the deadline earlier (lifecycle §3.3 server rule 4).

| Metric                                                   | Observed value                                |
| -------------------------------------------------------- | --------------------------------------------- |
| Token lifetime                                           | 20 s                                          |
| Reminder lead times before the deadline                  | 1902, 2162, 983, 579, 392, 191, 1498, 2050 ms |
| Shortest lead before the deadline                        | 191 ms                                        |
| First reminder to successful update (refresh and update) | 13 ms                                         |
| Any reminder before the deadline with under 250 ms left  | yes                                           |
| Pause duration                                           | 61047 ms                                      |
| Reminders during the pause                               | 2                                             |

## Known prototype deviations

- The draft protocol version is simulated by `experimental["io.modelcontextprotocol/subscription-lifetime"]`.
- The authorization server is a toy HS256 JWT issuer in the same process; it auto-consents to `files:audit`.
- Access and policy changes are scripted with `handler.subscriptions` rather than external introspection/shared signals.
- `/mcp-current` is the same SDK handler created without the proposals' options and given no `authInfo`, which is how today's SDK runs.
- The demo is single-process; distributed routing of lifecycle handles is not shown.
