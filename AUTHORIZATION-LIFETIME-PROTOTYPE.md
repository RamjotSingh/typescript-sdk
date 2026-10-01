# Authorization Lifetime for Subscription Streams: prototype

This branch prototypes a draft MCP proposal (SEP), Authorization Lifetime for Subscription Streams. A link to the proposal will be added here when it is filed.

A `subscriptions/listen` stream is one HTTP request that can stay open for hours, while the access token that authorized it may last minutes. The proposal bounds each stream by its authorization. The server checks every notification against the stream's authorization before writing it, stops delivering at the authorization deadline, reports that deadline in the acknowledgment as `authorizedUntil`, and ends the stream with an `AuthorizationEnded` error (`-32023`) whose `reason` (`token_expiry`, `insufficient_authorization`, or `revoked`) tells the client whether to refresh its token, involve the user, or stop. Clients on older protocol versions see the stream close without a response.

The branch is one commit on `main` at `433eb413` (2026-09-30).

## Run the demo

Requires Node 20 or later and pnpm 10. From the repository root:

```sh
pnpm install
pnpm build:all
pnpm --filter @mcp-examples/authorization-lifetime start:fast
```

The demo takes about a minute with 20-second access tokens (`start` uses two-minute tokens). It is self-verifying: a failed check exits non-zero, and a passing run ends with `PASS`. Its [README](examples/authorization-lifetime/README.md) maps each step to the rule it exercises.

The demo runs the proposal side by side with today's behavior: its server has a second endpoint, `/mcp-current`, that runs without the proposal, and the client runs three scenarios at once, pairing current and proposed clients with current and proposed servers. It writes a wire log grouped by those scenarios, with a heading per request, the request and its response, and every line the proposal adds marked with `+`. It prints the log's path when it starts and when it finishes.

## What to review

| Area          | Where                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| ------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Server        | [`listenRouter.ts`](packages/server/src/server/listenRouter.ts): the authorization deadline (token expiry, a policy cap, or a new scope requirement), the per-notification access check, the refusal of a stream whose filter is not wholly authorized (a `-32602` error that names the entries in `data.denied`, instead of an acknowledgment), `AuthorizationEnded` or a close without a response, the end deferred until the deadline when the last entry is lost during a change, and the `revoke` signal. [`createMcpHandler.ts`](packages/server/src/server/createMcpHandler.ts) passes `authInfo` to the router. |
| Server option | `createMcpHandler(factory, { subscriptionLifetime: { authorize, maxAuthorizationLifetimeMs } })`. `authorize(authInfo, target)` returns `'allow'`, `'deny'`, or `'unavailable'`.                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| Client        | [`client.ts`](packages/client/src/client/client.ts): `McpSubscription.authorizedUntil` and `endReason`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| Wire          | `authorizedUntil` in `notifications/subscriptions/acknowledged`; the `AuthorizationEnded` error code and its reasons ([`schemas.ts`](packages/core/src/schemas.ts), [`constants.ts`](packages/core/src/constants.ts), [`buildSchemas.ts`](packages/core-internal/src/wire/rev2026-07-28/buildSchemas.ts)).                                                                                                                                                                                                                                                                                                              |
| Tests         | [`server/test/server/subscriptionLifetime.test.ts`](packages/server/test/server/subscriptionLifetime.test.ts) (21 tests) and [`client/test/client/subscriptionLifetime.test.ts`](packages/client/test/client/subscriptionLifetime.test.ts) (3 tests). Test names cite the rule they check.                                                                                                                                                                                                                                                                                                                              |

Run the tests with:

```sh
pnpm --filter @modelcontextprotocol/server test -- test/server/subscriptionLifetime.test.ts
pnpm --filter @modelcontextprotocol/client test -- test/client/subscriptionLifetime.test.ts
```

Build, lint, typecheck, and every package's tests pass. On Windows, three existing tests that need symlink privileges or a catchable `SIGTERM` fail for platform reasons, in code the branch does not touch.

## Prototype deviations

- The SDK has no draft protocol revision. A request counts as being at the draft version when its client capabilities include `experimental["io.modelcontextprotocol/subscription-lifetime"]`; only that decides whether a stream ends with `AuthorizationEnded` or closes without a response.
- The demo's authorization server is an in-process toy (HS256 JWTs) that consents to `files:audit` automatically, standing in for the user's confirmation.
- Revocation and policy changes come from a prototype control API, `handler.subscriptions` (`revoke`, `recheckAccess`, `requireScopes`, `list`), rather than introspection or shared signals. `requireScopes` applies to open streams; the demo server's bearer check applies the same requirement to new requests.
- One process: streams and authorization signals are not shared across server instances.
