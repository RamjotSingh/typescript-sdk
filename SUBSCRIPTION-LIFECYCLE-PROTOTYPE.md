# Subscription Lifecycle: prototype

This branch prototypes a draft MCP proposal (SEP), [Subscription Lifecycle: Reminders, In-Place Updates, and Pausing](https://github.com/RamjotSingh/transports-wg/blob/sep/subscription-lifecycle/proposals/XXXX-subscription-lifecycle.md).

The proposal builds on another draft, [Authorization Lifetime for Subscription Streams](https://github.com/RamjotSingh/transports-wg/blob/sep/authorization-lifetime/proposals/XXXX-authorization-lifetime-for-subscription-streams.md), and so does this branch: it is the Authorization Lifetime branch, `poc/authorization-lifetime`, plus one commit. That commit is exactly what this proposal adds: [compare the two branches](https://github.com/RamjotSingh/typescript-sdk/compare/poc/authorization-lifetime...poc/subscription-lifecycle). [AUTHORIZATION-LIFETIME-PROTOTYPE.md](AUTHORIZATION-LIFETIME-PROTOTYPE.md) describes the part underneath.

Under Authorization Lifetime, a stream ends at its authorization deadline and the client opens a new one. This proposal lets clients that opt in keep their streams. With `lifecycle: true` in the listen filter, the client receives lifecycle notifications: reminders before the deadline (`reauthorization_required`), `access_reduced` when entries are removed, and `missed` when notifications may have been lost. The acknowledgment carries a `streamId`, and `subscriptions/update` re-authorizes the live stream in place. A stream that misses its deadline is paused rather than ended, and resumes when it is updated. The client chooses when the stream ends with `expiresAt`, within the server's maximum.

## Run the demo

Requires Node 20 or later and pnpm 10. From the repository root:

```sh
pnpm install
pnpm build:all
pnpm --filter @mcp-examples/subscription-lifecycle start:fast
```

The demo takes about two minutes with 20-second access tokens, because reminders on a paused stream are at least a minute apart (`start` uses two-minute tokens). It is self-verifying: a failed check exits non-zero, and a passing run ends with `PASS`. Its [README](examples/subscription-lifecycle/README.md) maps each step to the rule it exercises, marks the steps inherited from Authorization Lifetime, and reports measured reminder timings. The Authorization Lifetime demo, `@mcp-examples/authorization-lifetime`, also runs on this branch.

The demo runs the proposals side by side with today's behavior: its server has a second endpoint, `/mcp-current`, that runs without them, and the client runs four scenarios at once, pairing current and proposed clients with current and proposed servers. It writes a wire log grouped by those scenarios, with a heading per request, the request and its response, and every line the proposals add marked with `+`. It prints the log's path when it starts and when it finishes.

## What to review

| Area          | Where                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| ------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Server        | [`listenRouter.ts`](packages/server/src/server/listenRouter.ts): lifecycle notifications, the default reminder schedule with jitter, stream IDs, `subscriptions/update`, pausing and held notifications, `access_reduced` and `missed`, `expiresAt`, and `lastUpdatedAt`. [`createMcpHandler.ts`](packages/server/src/server/createMcpHandler.ts) routes `subscriptions/update`; [`inboundClassification.ts`](packages/core-internal/src/shared/inboundClassification.ts) takes its `Mcp-Name` header from the `streamId`, so load balancers can route the update to the instance holding the stream. |
| Server option | `createMcpHandler(factory, { subscriptionLifetime: { … }, subscriptionLifecycle: { maxStreamLifetimeMs, inPlaceUpdates, pause, hold, reminderLeadsMs, pausedReminderIntervalMs } })`. `subscriptionLifecycle` requires `subscriptionLifetime`; without it, the server implements Authorization Lifetime alone.                                                                                                                                                                                                                                                                                        |
| Client        | [`client.ts`](packages/client/src/client/client.ts): `listen(filter, { expiresAt })`, `McpSubscription.update()`, `onlifecycle`, the `expiresAt`, `streamId`, and `lastUpdatedAt` properties, and dropping reminders that an update result has superseded.                                                                                                                                                                                                                                                                                                                                            |
| Wire          | The `lifecycle` filter flag, `expiresAt`, `lastUpdatedAt`, `streamId`, `notifications/subscriptions/lifecycle`, and `subscriptions/update` ([`schemas.ts`](packages/core/src/schemas.ts), [`buildSchemas.ts`](packages/core-internal/src/wire/rev2026-07-28/buildSchemas.ts)).                                                                                                                                                                                                                                                                                                                        |
| Tests         | This branch adds 45 tests to [`server/test/server/subscriptionLifetime.test.ts`](packages/server/test/server/subscriptionLifetime.test.ts) (66 in all) and 8 to [`client/test/client/subscriptionLifetime.test.ts`](packages/client/test/client/subscriptionLifetime.test.ts) (11 in all). The tests from the Authorization Lifetime branch are unchanged.                                                                                                                                                                                                                                            |

Run the tests named for this proposal with:

```sh
pnpm --filter @modelcontextprotocol/server test -- test/server/subscriptionLifetime.test.ts -t "lifecycle SEP"
pnpm --filter @modelcontextprotocol/client test -- test/client/subscriptionLifetime.test.ts -t "lifecycle SEP"
```

Build, lint, typecheck, and every package's tests pass. On Windows, three existing tests that need symlink privileges or a catchable `SIGTERM` fail for platform reasons, in code the branch does not touch.

## Prototype deviations

- The SDK has no draft protocol revision. A request counts as being at the draft version when its client capabilities include `experimental["io.modelcontextprotocol/subscription-lifetime"]`.
- The demo's authorization server is an in-process toy (HS256 JWTs) that consents to `files:audit` automatically, standing in for the user's confirmation.
- Access and policy changes come from the prototype control API, `handler.subscriptions`, rather than introspection or shared signals.
- One process: routing `subscriptions/update` to the instance holding a stream, across several server instances, is not shown.
