import { describe, expect, test } from "bun:test"
import { Clock, Deferred, Effect, Fiber, Layer } from "effect"
import { TestClock } from "effect/testing"
import { HttpClient, HttpClientError, HttpClientResponse } from "effect/unstable/http"
import { Credential } from "@opencode/core/credential"
import { Integration } from "@opencode/core/integration"
import { ForkSubscriptionUsage } from "@opencode/core/fork-subscription-usage"
import { AppNodeBuilder } from "@opencode/core/effect/app-node-builder"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { LayerNodePlatform } from "@opencode/util/effect/app-node-platform"
import { testEffect } from "./lib/effect"

const it = testEffect(Layer.empty)
const oauth = (updates: Partial<Credential.OAuth> = {}): Credential.OAuth =>
  Credential.OAuth.make({
    type: "oauth",
    methodID: Integration.MethodID.make("chatgpt-browser"),
    access: "mock-access",
    refresh: "mock-refresh",
    expires: 9000000000000,
    metadata: { accountID: "mock-account" },
    ...updates,
  })
const openai = {
  plan_type: "plus",
  rate_limit: { primary_window: { used_percent: 25, limit_window_seconds: 18000, reset_at: 1700000000 } },
}

const fixture = (values: Record<string, Credential.Value> = { openai: oauth() }) =>
  Effect.gen(function* () {
    const state = {
      saved: Object.fromEntries(
        Object.entries(values).map(([id, value]) => [
          id,
          new Credential.Info({
            id: Credential.ID.make(`cred_${id}`),
            integrationID: Integration.ID.make(id),
            label: "mock",
            value,
          }),
        ]),
      ),
      body: openai as unknown,
      raw: undefined as string | undefined,
      status: 200,
      headers: {} as Record<string, string>,
      calls: 0,
      writes: 0,
      refreshes: 0,
      network: false,
      before: Effect.void as Effect.Effect<void>,
      refresh: undefined as ((value: Credential.OAuth) => Effect.Effect<Credential.OAuth, unknown>) | undefined,
      requests: [] as Array<Parameters<typeof HttpClientResponse.fromWeb>[0]>,
      signals: [] as AbortSignal[],
    }
    const stored = Layer.mock(Credential.Service)({
      get: (id) => Effect.sync(() => Object.values(state.saved).find((item) => item.id === id)),
      list: (id) => Effect.sync(() => Object.values(state.saved).filter((item) => item.integrationID === id)),
      compareAndSet: (expected, value) =>
        Effect.sync(() => {
          const entry = Object.entries(state.saved).find(([, item]) => item.id === expected.id)
          if (
            entry &&
            JSON.stringify(entry[1].value) === JSON.stringify(expected.value) &&
            entry[1].integrationID === expected.integrationID
          ) {
            state.writes++
            state.saved[entry[0]] = new Credential.Info({ ...entry[1], value })
            return value
          }
          return entry?.[1].value
        }),
    })
    const http = HttpClient.make((request, _url, signal) =>
      Effect.gen(function* () {
        state.calls++
        state.requests.push(request)
        state.signals.push(signal)
        yield* state.before
        if (state.network)
          return yield* new HttpClientError.HttpClientError({
            reason: new HttpClientError.TransportError({ request, description: "mock secret failure" }),
          })
        return HttpClientResponse.fromWeb(
          request,
          new Response(state.raw ?? JSON.stringify(state.body), {
            status: state.status,
            headers: { "content-type": "application/json", ...state.headers },
          }),
        )
      }),
    )
    const context = yield* Layer.build(
      AppNodeBuilder.build(LayerNode.group([ForkSubscriptionUsage.node, Integration.node]), [
        Credential.node.replace(stored),
        LayerNodePlatform.httpClient.replace(Layer.succeed(HttpClient.HttpClient, http)),
      ]),
    )
    const integration = yield* Integration.Service.pipe(Effect.provide(context))
    yield* integration.transform((editor) =>
      editor.method.update({
        integrationID: Integration.ID.make("openai"),
        method: { type: "oauth", id: Integration.MethodID.make("chatgpt-browser"), label: "Mock OAuth" },
        authorize: () => Effect.die("must never log in during tests"),
        refresh: (value) =>
          Effect.suspend(() => {
            state.refreshes++
            return state.refresh ? state.refresh(value) : Effect.succeed(value)
          }),
      }),
    )
    const usage = yield* ForkSubscriptionUsage.Service.pipe(Effect.provide(context))
    return { state, get: usage.get, integration }
  })

describe("subscription normalization", () => {
  test("OpenAI percentages, durations and epoch conversion", () => {
    expect(ForkSubscriptionUsage.normalize(openai, 0)).toEqual({
      plan: "plus",
      windows: [
        {
          id: "primary_window",
          label: "5-hour",
          usedPercent: 25,
          remainingPercent: 75,
          windowSeconds: 18000,
          resetsAt: 1700000000000,
        },
      ],
    })
    expect(
      ForkSubscriptionUsage.normalize(
        {
          rate_limit: {
            secondary_window: { used_percent: 120, limit_window_seconds: 604800, reset_after_seconds: 10 },
          },
        },
        1000,
      ).windows[0],
    ).toMatchObject({ label: "Weekly", usedPercent: 100, remainingPercent: 0, resetsAt: 11000 })
  })
  for (const value of [null, [], 1, "oops", {}, { rate_limit: null }])
    test(`missing payload ${JSON.stringify(value)}`, () => {
      expect(ForkSubscriptionUsage.normalize(value, 0).windows).toEqual([])
    })
  test("strict numeric values and malformed reset/duration omission", () => {
    for (const value of [null, "20", NaN, Infinity, true]) {
      expect(
        ForkSubscriptionUsage.normalize({ rate_limit: { primary_window: { used_percent: value } } }, 0).windows,
      ).toEqual([])
    }
    expect(
      ForkSubscriptionUsage.normalize(
        {
          plan_type: "mock-secret",
          rate_limit: { primary_window: { used_percent: 0, reset_at: Infinity, limit_window_seconds: -1 } },
        },
        0,
      ),
    ).toEqual({ windows: [{ id: "primary_window", label: "Primary", usedPercent: 0, remainingPercent: 100 }] })
  })
})

describe("V2 subscription credentials and cache", () => {
  it.effect("absent and key credentials never resolve or call upstream", () =>
    Effect.gen(function* () {
      const cases: Record<string, Credential.Value>[] = [
        {},
        { openai: Credential.Key.make({ type: "key", key: "mock-key" }) },
      ]
      for (const values of cases) {
        const f = yield* fixture(values)
        const result = yield* f.get()
        expect(result.providers.map((p) => p.status)).toEqual([values.openai ? "unsupported_auth" : "not_connected"])
        expect(f.state.calls).toBe(0)
        expect(f.state.refreshes).toBe(0)
      }
    }),
  )
  for (const methodID of ["unrelated-oauth", "chatgpt-browser", "chatgpt-headless"])
    it.effect(`OpenAI subscription method boundary ${methodID}`, () =>
      Effect.gen(function* () {
        const f = yield* fixture({ openai: oauth({ methodID: Integration.MethodID.make(methodID) }) })
        expect((yield* f.get()).providers[0]?.status).toBe(methodID === "unrelated-oauth" ? "unsupported_auth" : "ok")
        expect(f.state.calls).toBe(methodID === "unrelated-oauth" ? 0 : 1)
      }),
    )
  it.effect("fixed URLs, exact saved account metadata and genuine UA", () =>
    Effect.gen(function* () {
      const f = yield* fixture()
      expect((yield* f.get()).providers.map((item) => item.status)).toEqual(["ok"])
      expect(f.state.requests.map((r) => r.url)).toEqual(["https://chatgpt.com/backend-api/wham/usage"])
      expect(f.state.requests[0]?.headers).toMatchObject({
        authorization: "Bearer mock-access",
        "chatgpt-account-id": "mock-account",
        accept: "application/json",
        "user-agent": "opencode/unknown/unknown/opencode",
      })
    }),
  )
  it.effect("concurrent quota requests coalesce and successful cache lasts five minutes", () =>
    Effect.gen(function* () {
      const f = yield* fixture()
      const started = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      f.state.before = Deferred.succeed(started, undefined).pipe(Effect.andThen(Deferred.await(release)))
      const pending = yield* Effect.all(
        Array.from({ length: 10 }, () => f.get()),
        { concurrency: "unbounded" },
      ).pipe(Effect.forkChild)
      yield* Deferred.await(started)
      yield* Deferred.succeed(release, undefined)
      yield* Fiber.join(pending)
      expect(f.state.calls).toBe(1)
      yield* TestClock.adjust(299999)
      yield* f.get()
      expect(f.state.calls).toBe(1)
      yield* TestClock.adjust(1)
      yield* f.get()
      expect(f.state.calls).toBe(2)
    }),
  )
  for (const status of [401, 403, 429, 500])
    it.effect(`${status} safe stale fallback, backoff and recovery`, () =>
      Effect.gen(function* () {
        const f = yield* fixture()
        const first = (yield* f.get()).providers[0]
        yield* TestClock.adjust(300000)
        f.state.status = status
        f.state.body = { message: "mock secret account details" }
        const result = (yield* f.get()).providers[0]
        expect(result).toMatchObject({
          status: status === 401 ? "auth_required" : status === 429 ? "rate_limited" : "unavailable",
          windows: first?.windows,
          updatedAt: first?.updatedAt,
          stale: true,
        })
        expect(JSON.stringify(result)).not.toContain("mock")
        yield* f.get()
        expect(f.state.calls).toBe(2)
        const now = yield* Clock.currentTimeMillis
        yield* TestClock.adjust((result?.retryAt ?? now) - now)
        f.state.status = 200
        f.state.body = openai
        expect((yield* f.get()).providers[0]?.stale).toBe(false)
      }),
    )
  for (const retry of ["900", "Thu, 01 Jan 1970 00:15:00 GMT", "invalid", "0"])
    it.effect(`Retry-After ${retry}`, () =>
      Effect.gen(function* () {
        const f = yield* fixture()
        f.state.status = 429
        f.state.headers = { "retry-after": retry }
        expect((yield* f.get()).providers[0]?.retryAt).toBe(retry === "0" || retry === "invalid" ? 300000 : 900000)
        yield* f.get()
        expect(f.state.calls).toBe(1)
      }),
    )
  for (const field of ["access", "refresh", "expires", "metadata", "id"] as const)
    it.effect(`credential identity change ${field} isolates snapshots`, () =>
      Effect.gen(function* () {
        const f = yield* fixture()
        yield* f.get()
        const saved = f.state.saved.openai
        if (!saved) return yield* Effect.die("missing mock credential")
        f.state.saved.openai = new Credential.Info({
          ...saved,
          ...(field === "id"
            ? { id: Credential.ID.make("cred_other") }
            : {
                value: oauth({
                  [field]:
                    field === "expires"
                      ? 8999999999999
                      : field === "metadata"
                        ? { accountID: "mock-other" }
                        : "mock-changed",
                }),
              }),
        })
        f.state.status = 401
        expect((yield* f.get()).providers[0]?.windows).toEqual([])
        expect(f.state.calls).toBe(2)
      }),
    )
  for (const raw of ["null", "{}", "not json", '{"rate_limit":{"primary_window":{"used_percent":"25"}}}'])
    it.effect(`malformed upstream ${raw} keeps last success`, () =>
      Effect.gen(function* () {
        const f = yield* fixture()
        const first = (yield* f.get()).providers[0]
        yield* TestClock.adjust(300000)
        f.state.raw = raw
        expect((yield* f.get()).providers[0]).toMatchObject({
          windows: first?.windows,
          updatedAt: first?.updatedAt,
          stale: true,
        })
        yield* f.get()
        expect(f.state.calls).toBe(2)
      }),
    )
  it.effect("network errors retain snapshot and do not leak causes", () =>
    Effect.gen(function* () {
      const f = yield* fixture()
      const first = (yield* f.get()).providers[0]
      yield* TestClock.adjust(300000)
      f.state.network = true
      const result = (yield* f.get()).providers[0]
      expect(result).toMatchObject({ status: "error", windows: first?.windows, stale: true, retryAt: 360000 })
      expect(JSON.stringify(result)).not.toContain("mock")
    }),
  )
  it.effect("timeout aborts request, retains snapshot and backs off", () =>
    Effect.gen(function* () {
      const f = yield* fixture()
      const first = (yield* f.get()).providers[0]
      yield* TestClock.adjust(300000)
      const started = yield* Deferred.make<void>()
      f.state.before = Deferred.succeed(started, undefined).pipe(Effect.andThen(Effect.never))
      const pending = yield* f.get().pipe(Effect.forkChild)
      yield* Deferred.await(started)
      yield* TestClock.adjust(10000)
      expect((yield* Fiber.join(pending)).providers[0]).toMatchObject({
        status: "error",
        windows: first?.windows,
        stale: true,
        retryAt: 370000,
      })
      expect(f.state.signals[1]?.aborted).toBe(true)
      yield* f.get()
      expect(f.state.calls).toBe(2)
    }),
  )
  it.effect("expired refresh is reused, persisted, coalesced with model resolution and cached", () =>
    Effect.gen(function* () {
      const f = yield* fixture({ openai: oauth({ expires: 0 }) })
      const started = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      f.state.refresh = () =>
        Deferred.succeed(started, undefined).pipe(
          Effect.andThen(Deferred.await(release)),
          Effect.as(oauth({ access: "mock-refreshed", refresh: "mock-rotated" })),
        )
      const quota = yield* f.get().pipe(Effect.forkChild)
      yield* Deferred.await(started)
      const connection = yield* f.integration.connection.active(Integration.ID.make("openai"))
      if (!connection) return yield* Effect.die("missing mock connection")
      const model = yield* f.integration.connection.resolve(connection).pipe(Effect.forkChild)
      yield* Deferred.succeed(release, undefined)
      expect((yield* Fiber.join(quota)).providers[0]?.status).toBe("ok")
      expect((yield* Fiber.join(model))?.type).toBe("oauth")
      expect(f.state.refreshes).toBe(1)
      expect(f.state.writes).toBe(1)
      expect(f.state.requests[0]?.headers.authorization).toBe("Bearer mock-refreshed")
      yield* f.get()
      expect(f.state.calls).toBe(1)
    }),
  )
  it.effect("disconnect purges snapshots even when the same credential reconnects", () =>
    Effect.gen(function* () {
      const f = yield* fixture()
      yield* f.get()
      const saved = f.state.saved.openai
      if (!saved) return yield* Effect.die("missing mock credential")
      delete f.state.saved.openai
      expect((yield* f.get()).providers[0]?.status).toBe("not_connected")
      f.state.saved.openai = saved
      f.state.status = 500
      expect((yield* f.get()).providers[0]?.windows).toEqual([])
    }),
  )
  it.effect("late old-account responses cannot contaminate active account cache", () =>
    Effect.gen(function* () {
      const f = yield* fixture()
      const started = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      f.state.before = Deferred.succeed(started, undefined).pipe(Effect.andThen(Deferred.await(release)))
      const old = yield* f.get().pipe(Effect.forkChild)
      yield* Deferred.await(started)
      const saved = f.state.saved.openai
      if (!saved) return yield* Effect.die("missing mock credential")
      f.state.saved.openai = new Credential.Info({
        ...saved,
        id: Credential.ID.make("cred_new_account"),
        value: oauth({ metadata: { accountID: "mock-other" } }),
      })
      f.state.before = Effect.void
      f.state.status = 401
      expect((yield* f.get()).providers[0]?.windows).toEqual([])
      f.state.status = 200
      yield* Deferred.succeed(release, undefined)
      expect((yield* Fiber.join(old)).providers[0]?.status).toBe("ok")
      expect((yield* f.get()).providers[0]).toMatchObject({ status: "auth_required", windows: [] })
    }),
  )
  it.effect("cancelled quota caller leaves bounded request and recoverable cache", () =>
    Effect.gen(function* () {
      const f = yield* fixture()
      const started = yield* Deferred.make<void>()
      f.state.before = Deferred.succeed(started, undefined).pipe(Effect.andThen(Effect.never))
      const pending = yield* f.get().pipe(Effect.forkChild)
      yield* Deferred.await(started)
      yield* Fiber.interrupt(pending)
      yield* TestClock.adjust(10000)
      expect(f.state.signals[0]?.aborted).toBe(true)
      f.state.before = Effect.void
      yield* TestClock.adjust(60000)
      expect((yield* f.get()).providers[0]?.status).toBe("ok")
    }),
  )
  it.effect("refresh changing account metadata discards prior successful windows", () =>
    Effect.gen(function* () {
      const f = yield* fixture({ openai: oauth({ expires: 300001 }) })
      yield* f.get()
      yield* TestClock.adjust(300000)
      f.state.refresh = () => Effect.succeed(oauth({ metadata: { accountID: "mock-other" } }))
      f.state.status = 500
      expect((yield* f.get()).providers[0]?.windows).toEqual([])
      expect(f.state.requests[1]?.headers["chatgpt-account-id"]).toBe("mock-other")
    }),
  )
  it.effect("quota HTTP tracing does not propagate account or token headers", () =>
    Effect.gen(function* () {
      const f = yield* fixture()
      yield* f.get().pipe(Effect.withSpan("test-parent"))
      expect(f.state.requests[0]?.headers.traceparent).toBeUndefined()
    }),
  )
  it.effect("active environment connection is unsupported without resolving its secret", () =>
    Effect.gen(function* () {
      const integration = Layer.mock(Integration.Service)({
        revision: () => 0,
        oauth: {
          connect: () => Effect.die("unused"),
          status: () => Effect.die("unused"),
          complete: () => Effect.die("unused"),
          cancel: () => Effect.die("unused"),
        },
        command: {
          connect: () => Effect.die("unused"),
          status: () => Effect.die("unused"),
          cancel: () => Effect.die("unused"),
        },
        connection: {
          active: () => Effect.succeed({ type: "env" as const, name: "MOCK_API_KEY" }),
          resolve: () => Effect.die("must not read an environment secret"),
          key: () => Effect.die("unused"),
          activate: () => Effect.die("unused"),
          update: () => Effect.die("unused"),
          remove: () => Effect.die("unused"),
        },
      })
      const context = yield* Layer.build(
        ForkSubscriptionUsage.layer.pipe(
          Layer.provide([
            integration,
            Layer.mock(Credential.Service)({}),
            Layer.succeed(
              HttpClient.HttpClient,
              HttpClient.make(() => Effect.die("unexpected outbound request")),
            ),
          ]),
        ),
      )
      const usage = yield* ForkSubscriptionUsage.Service.pipe(Effect.provide(context))
      expect((yield* usage.get()).providers.map((p) => p.status)).toEqual(["unsupported_auth"])
    }),
  )
  it.effect("failed refresh returns safe auth_required and prevents refresh storms", () =>
    Effect.gen(function* () {
      const f = yield* fixture({ openai: oauth({ expires: 0 }) })
      f.state.refresh = () => Effect.fail(new Error("mock secret refresh error"))
      const result = (yield* f.get()).providers[0]
      expect(result).toMatchObject({ status: "auth_required", windows: [], stale: true })
      expect(JSON.stringify(result)).not.toContain("mock")
      yield* f.get()
      expect(f.state.refreshes).toBe(1)
      expect(f.state.calls).toBe(0)
    }),
  )
  it.effect("refresh failure after successful read preserves same-account stale data", () =>
    Effect.gen(function* () {
      const f = yield* fixture({ openai: oauth({ expires: 300001 }) })
      const first = (yield* f.get()).providers[0]
      yield* TestClock.adjust(300000)
      f.state.refresh = () => Effect.fail(new Error("mock-secret"))
      expect((yield* f.get()).providers[0]).toMatchObject({
        status: "auth_required",
        windows: first?.windows,
        updatedAt: first?.updatedAt,
        stale: true,
      })
      expect(f.state.calls).toBe(1)
    }),
  )
  it.effect("expired credential without usable refresh requires reconnect", () =>
    Effect.gen(function* () {
      const f = yield* fixture({ openai: oauth({ expires: 0 }) })
      expect((yield* f.get()).providers[0]?.status).toBe("auth_required")
      expect(f.state.calls).toBe(0)
    }),
  )
})
