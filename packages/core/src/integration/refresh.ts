export * as IntegrationRefresh from "./refresh.js"

import { createHash } from "node:crypto"
import { Context, Effect, Fiber, Layer, Semaphore } from "effect"
import { makeGlobalNode } from "@opencode/util/effect/app-node"
import { Credential } from "../credential.js"

export interface Interface {
  readonly resolve: (
    id: Credential.ID,
    refresh: (credential: Credential.Info) => Effect.Effect<Credential.Value, unknown>,
  ) => Effect.Effect<Credential.Value | undefined, unknown>
}
export class Service extends Context.Service<Service, Interface>()("@opencode/IntegrationRefresh") {}
const identity = (credential: Credential.Info) =>
  createHash("sha256")
    .update(JSON.stringify([credential.integrationID, credential.value]))
    .digest("hex")

/** Shared across locations and all Integration.resolve callers; owns no OAuth protocol. */
export const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const credentials = yield* Credential.Service
    const scope = yield* Effect.scope
    const lock = Semaphore.makeUnsafe(1)
    const pending = new Map<
      Credential.ID,
      { key: string; read: Effect.Effect<Credential.Value | undefined, unknown> }
    >()
    return Service.of({
      resolve: Effect.fn("IntegrationRefresh.resolve")(function* (id, refresh) {
        const read = yield* lock.withPermit(
          Effect.gen(function* () {
            const saved = yield* credentials.get(id)
            if (!saved) return Effect.succeed(undefined)
            const key = identity(saved)
            const previous = pending.get(id)
            if (previous?.key === key) return previous.read
            const operation = Effect.gen(function* () {
              const value = yield* refresh(saved)
              if (value === saved.value) return value
              // The database predicate and write are atomic, including against manual credential updates.
              return yield* credentials.compareAndSet(saved, value)
            }).pipe(
              Effect.ensuring(
                Effect.sync(() => {
                  if (pending.get(id)?.key === key) pending.delete(id)
                }),
              ),
            )
            // Cancellation of one borrower does not cancel token rotation for every other borrower.
            // The global service scope still owns and interrupts the work at shutdown.
            const start = yield* Effect.cached(operation.pipe(Effect.forkIn(scope)))
            const read = start.pipe(Effect.flatMap(Fiber.join))
            pending.set(id, { key, read })
            return read
          }),
        )
        return yield* read
      }),
    })
  }),
)

export const node = makeGlobalNode({ service: Service, layer, deps: [Credential.node] })
