import { expect } from "bun:test"
import { Deferred, Effect, Exit, Fiber, Layer, Scope } from "effect"
import { Credential } from "@opencode/core/credential"
import { Integration } from "@opencode/core/integration"
import { IntegrationRefresh } from "@opencode/core/integration/refresh"
import { Database } from "@opencode/core/database/database"
import { CredentialTable } from "@opencode/core/credential/sql"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { eq } from "drizzle-orm"
import { testEffect } from "./lib/effect"

const it = testEffect(Layer.empty)
const dbIt = testEffect(LayerNode.compile(LayerNode.group([Credential.node, Database.node])))
const value = (access: string) =>
  Credential.OAuth.make({
    type: "oauth",
    methodID: Integration.MethodID.make("chatgpt-browser"),
    access,
    refresh: "mock-refresh",
    expires: 0,
  })
const fixture = Effect.gen(function* () {
  const initial = new Credential.Info({
    id: Credential.ID.make("cred_mock"),
    integrationID: Integration.ID.make("openai"),
    label: "Mock",
    value: value("mock-old"),
  })
  const state = { saved: initial as Credential.Info | undefined, writes: 0, calls: 0 }
  const stored = Layer.mock(Credential.Service)({
    get: () => Effect.sync(() => state.saved),
    compareAndSet: (expected, value) =>
      Effect.sync(() => {
        if (
          state.saved?.integrationID === expected.integrationID &&
          JSON.stringify(state.saved.value) === JSON.stringify(expected.value)
        ) {
          state.writes++
          state.saved = new Credential.Info({ ...state.saved, value })
          return value
        }
        return state.saved?.value
      }),
  })
  const context = yield* Layer.build(IntegrationRefresh.layer.pipe(Layer.provide(stored)))
  const service = yield* IntegrationRefresh.Service.pipe(Effect.provide(context))
  return { state, initial, service }
})

for (const change of ["replace", "delete", "delete-and-replace"] as const)
  dbIt.effect(`atomic refresh persistence loses to ${change} after credential validation`, () =>
    Effect.gen(function* () {
      const credentials = yield* Credential.Service
      const database = yield* Database.Service
      const initial = yield* credentials.create({
        integrationID: Integration.ID.make("openai"),
        label: "Original",
        value: value("mock-old"),
      })
      yield* database.db
        .update(CredentialTable)
        .set({ active: true, method_id: "mock-method", connector_id: "mock-connector" })
        .where(eq(CredentialTable.id, initial.id))
        .run()
      const validated = yield* Deferred.make<void>()
      const persist = yield* Deferred.make<void>()
      const context = yield* Layer.build(
        IntegrationRefresh.layer.pipe(
          Layer.provide(
            Layer.succeed(
              Credential.Service,
              Credential.Service.of({
                ...credentials,
                compareAndSet: Effect.fnUntraced(function* (expected, refreshed) {
                  // Reproduce the reviewer's last successful validation, then pause immediately before persistence.
                  expect((yield* credentials.get(expected.id))?.value).toEqual(expected.value)
                  yield* Deferred.succeed(validated, undefined)
                  yield* Deferred.await(persist)
                  return yield* credentials.compareAndSet(expected, refreshed)
                }),
              }),
            ),
          ),
        ),
      )
      const coordinator = yield* IntegrationRefresh.Service.pipe(Effect.provide(context))
      const pending = yield* coordinator
        .resolve(initial.id, () => Effect.succeed(value("mock-obsolete-rotation")))
        .pipe(Effect.forkChild)
      yield* Deferred.await(validated)
      if (change === "replace") yield* credentials.update(initial.id, { label: "Manual", value: value("mock-manual") })
      if (change !== "replace") yield* credentials.remove(initial.id)
      if (change === "delete-and-replace")
        yield* database.db
          .insert(CredentialTable)
          .values({
            id: initial.id,
            integration_id: initial.integrationID,
            label: "Manual",
            value: value("mock-manual"),
            active: true,
            method_id: "mock-method",
            connector_id: "mock-connector",
          })
          .run()
      yield* Deferred.succeed(persist, undefined)
      expect(yield* Fiber.join(pending)).toEqual(change === "delete" ? undefined : value("mock-manual"))
      const row = yield* database.db.select().from(CredentialTable).where(eq(CredentialTable.id, initial.id)).get()
      if (change === "delete") {
        expect(row).toBeUndefined()
        return
      }
      expect(row).toMatchObject({
        value: value("mock-manual"),
        label: "Manual",
        active: true,
        method_id: "mock-method",
        connector_id: "mock-connector",
      })
    }),
  )

it.effect("shared credential refresh single-flight persists once for concurrent consumers", () =>
  Effect.gen(function* () {
    const f = yield* fixture
    const started = yield* Deferred.make<void>()
    const release = yield* Deferred.make<void>()
    const refresh = () =>
      Effect.gen(function* () {
        f.state.calls++
        yield* Deferred.succeed(started, undefined)
        yield* Deferred.await(release)
        return value("mock-new")
      })
    const pending = yield* Effect.all(
      Array.from({ length: 10 }, () => f.service.resolve(f.initial.id, refresh)),
      { concurrency: "unbounded" },
    ).pipe(Effect.forkChild)
    yield* Deferred.await(started)
    yield* Deferred.succeed(release, undefined)
    const results = yield* Fiber.join(pending)
    expect(results.every((item) => item?.type === "oauth" && item.access === "mock-new")).toBe(true)
    expect(f.state.calls).toBe(1)
    expect(f.state.writes).toBe(1)
  }),
)
it.effect("shared refresh failures are coalesced and later resolution can retry", () =>
  Effect.gen(function* () {
    const f = yield* fixture
    const started = yield* Deferred.make<void>()
    const release = yield* Deferred.make<void>()
    const refresh = () =>
      Effect.gen(function* () {
        f.state.calls++
        yield* Deferred.succeed(started, undefined)
        yield* Deferred.await(release)
        return yield* Effect.fail("mock-failure")
      })
    const pending = yield* Effect.all(
      Array.from({ length: 10 }, () => f.service.resolve(f.initial.id, refresh).pipe(Effect.result)),
      { concurrency: "unbounded" },
    ).pipe(Effect.forkChild)
    yield* Deferred.await(started)
    yield* Deferred.succeed(release, undefined)
    expect((yield* Fiber.join(pending)).every((item) => item._tag === "Failure")).toBe(true)
    expect(f.state.calls).toBe(1)
    expect(f.state.writes).toBe(0)
    expect(yield* f.service.resolve(f.initial.id, () => Effect.succeed(value("mock-retry")))).toEqual(
      value("mock-retry"),
    )
    expect(f.state.writes).toBe(1)
  }),
)
it.effect("cancelling one borrower cannot cancel another borrower's token refresh", () =>
  Effect.gen(function* () {
    const f = yield* fixture
    const started = yield* Deferred.make<void>()
    const release = yield* Deferred.make<void>()
    const refresh = (saved: Credential.Info) =>
      Effect.gen(function* () {
        if (saved.value.type === "oauth" && saved.value.access === "mock-new") return saved.value
        f.state.calls++
        yield* Deferred.succeed(started, undefined)
        yield* Deferred.await(release)
        return value("mock-new")
      })
    const first = yield* f.service.resolve(f.initial.id, refresh).pipe(Effect.forkChild)
    yield* Deferred.await(started)
    const second = yield* f.service.resolve(f.initial.id, refresh).pipe(Effect.forkChild)
    yield* Fiber.interrupt(first)
    yield* Deferred.succeed(release, undefined)
    expect(yield* Fiber.join(second)).toEqual(value("mock-new"))
    expect(f.state.calls).toBe(1)
    expect(f.state.writes).toBe(1)
  }),
)
it.effect("changed credential during refresh is not overwritten by old refresh", () =>
  Effect.gen(function* () {
    const f = yield* fixture
    const started = yield* Deferred.make<void>()
    const release = yield* Deferred.make<void>()
    const pending = yield* f.service
      .resolve(f.initial.id, () =>
        Deferred.succeed(started, undefined).pipe(
          Effect.andThen(Deferred.await(release)),
          Effect.as(value("mock-obsolete")),
        ),
      )
      .pipe(Effect.forkChild)
    yield* Deferred.await(started)
    f.state.saved = new Credential.Info({ ...f.initial, value: value("mock-manual") })
    expect(yield* f.service.resolve(f.initial.id, (saved) => Effect.succeed(saved.value))).toEqual(value("mock-manual"))
    yield* Deferred.succeed(release, undefined)
    expect(yield* Fiber.join(pending)).toEqual(value("mock-manual"))
    expect(f.state.writes).toBe(0)
  }),
)
it.effect("removed credential during refresh is not recreated", () =>
  Effect.gen(function* () {
    const f = yield* fixture
    const started = yield* Deferred.make<void>()
    const release = yield* Deferred.make<void>()
    const pending = yield* f.service
      .resolve(f.initial.id, () =>
        Deferred.succeed(started, undefined).pipe(
          Effect.andThen(Deferred.await(release)),
          Effect.as(value("mock-obsolete")),
        ),
      )
      .pipe(Effect.forkChild)
    yield* Deferred.await(started)
    f.state.saved = undefined
    yield* Deferred.succeed(release, undefined)
    expect(yield* Fiber.join(pending)).toBeUndefined()
    expect(f.state.writes).toBe(0)
  }),
)

it.effect("shared refresh work is cancelled when its owning service scope closes", () =>
  Effect.gen(function* () {
    const owner = yield* Scope.fork(yield* Effect.scope)
    const started = yield* Deferred.make<void>()
    const cancelled = yield* Deferred.make<void>()
    const saved = new Credential.Info({
      id: Credential.ID.make("cred_scope"),
      integrationID: Integration.ID.make("openai"),
      label: "Mock",
      value: value("mock-old"),
    })
    const context = yield* Layer.build(
      IntegrationRefresh.layer.pipe(
        Layer.provide(Layer.mock(Credential.Service)({ get: () => Effect.succeed(saved) })),
      ),
    ).pipe(Scope.provide(owner))
    const service = yield* IntegrationRefresh.Service.pipe(Effect.provide(context))
    const pending = yield* service
      .resolve(saved.id, () =>
        Deferred.succeed(started, undefined).pipe(
          Effect.andThen(Effect.never),
          Effect.onInterrupt(() => Deferred.succeed(cancelled, undefined)),
        ),
      )
      .pipe(Effect.forkChild)
    yield* Deferred.await(started)
    yield* Scope.close(owner, Exit.void)
    yield* Deferred.await(cancelled)
    expect(Exit.isFailure(yield* Fiber.await(pending))).toBe(true)
  }),
)
