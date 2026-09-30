export * as ForkSubscriptionUsage from "./fork-subscription-usage.js"

import { createHash } from "node:crypto"
import { Clock, Context, Effect, Layer, Schema, Semaphore } from "effect"
import { HttpClient } from "effect/unstable/http"
import { ForkSubscriptionUsage } from "@opencode/schema/fork-subscription-usage"
import { makeLocationNode } from "@opencode/util/effect/app-node"
import { LayerNodePlatform } from "@opencode/util/effect/app-node-platform"
import { Credential } from "./credential.js"
import { Integration } from "./integration.js"
import { App } from "./app.js"

const isRecord = Schema.is(Schema.Record(Schema.String, Schema.Unknown))
const record = (value: unknown): Record<string, unknown> => (isRecord(value) && !Array.isArray(value) ? value : {})
const numeric = (value: unknown): value is number => typeof value === "number" && Number.isFinite(value)
const timestamp = (value: number) => Number.isSafeInteger(value) && value >= 0 && value <= 8640000000000000

/** Unstable format verified against openai/codex 27c05a52. */
export function normalize(payload: unknown, now: number) {
  const data = record(payload)
  const entries = ["primary_window", "secondary_window"].map((id) => ({
    id,
    label: id === "primary_window" ? "Primary" : "Secondary",
    value: record(data.rate_limit)[id],
  }))
  const windows = entries.flatMap((entry) => {
    const value = record(entry.value)
    const percent = value.used_percent
    if (!numeric(percent)) return []
    const usedPercent = Math.max(0, Math.min(100, percent))
    const seconds = value.limit_window_seconds
    const windowSeconds = numeric(seconds) && Number.isSafeInteger(seconds) && seconds > 0 ? seconds : undefined
    const reset =
      numeric(value.reset_at) && value.reset_at >= 0
        ? value.reset_at * 1000
        : numeric(value.reset_after_seconds) && value.reset_after_seconds >= 0
          ? now + value.reset_after_seconds * 1000
          : NaN
    const label =
      windowSeconds === 18000
        ? "5-hour"
        : windowSeconds === 604800
          ? "Weekly"
          : windowSeconds
            ? `${entry.label} · ${windowSeconds / 3600}-hour`
            : entry.label
    return [
      {
        id: entry.id,
        label,
        usedPercent,
        remainingPercent: 100 - usedPercent,
        ...(windowSeconds !== undefined && { windowSeconds }),
        ...(timestamp(reset) && { resetsAt: reset }),
      },
    ]
  })
  const plan =
    typeof data.plan_type === "string" &&
    [
      "free",
      "go",
      "plus",
      "pro",
      "pro_lite",
      "team",
      "business",
      "enterprise",
      "edu",
      "education",
      "edu_plus",
      "edu_pro",
    ].includes(data.plan_type)
      ? data.plan_type
      : undefined
  return { windows, ...(plan && { plan }) }
}

type Entry = {
  key: string
  next: number
  snapshot?: ForkSubscriptionUsage.Provider
  read?: Effect.Effect<ForkSubscriptionUsage.Provider>
}
const fingerprint = (credential: Credential.Info) =>
  createHash("sha256")
    .update(JSON.stringify([credential.id, credential.integrationID, credential.value]))
    .digest("hex")

export interface Interface {
  readonly get: () => Effect.Effect<ForkSubscriptionUsage.Result>
}
export class Service extends Context.Service<Service, Interface>()("@opencode/ForkSubscriptionUsage") {}

export const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const credentials = yield* Credential.Service
    const integration = yield* Integration.Service
    const http = yield* HttpClient.HttpClient
    const client = HttpClient.withScope(http)
    const app = yield* App.Metadata
    const cache: { entry?: Entry } = {}
    const lock = Semaphore.makeUnsafe(1)
    const get = Effect.fn("ForkSubscriptionUsage.provider")(function* () {
      const providerID = "openai" as const
      const connection = yield* integration.connection.active(Integration.ID.make("openai"))
      const saved = connection?.type === "credential" ? yield* credentials.get(connection.id) : undefined
      const supported =
        saved?.value.type === "oauth" &&
        saved.integrationID === Integration.ID.make(providerID) &&
        ["chatgpt-browser", "chatgpt-headless"].includes(saved.value.methodID)
      if (!connection || connection.type !== "credential" || !saved || saved.value.type !== "oauth" || !supported) {
        delete cache.entry
        const connected = Boolean(connection && (connection.type === "env" || saved))
        return {
          providerID,
          status: connected ? "unsupported_auth" : "not_connected",
          windows: [],
          stale: false,
          message: connected
            ? "Subscription usage requires a ChatGPT OAuth connection."
            : "Connect this provider to view subscription usage.",
        } satisfies ForkSubscriptionUsage.Provider
      }
      const now = yield* Clock.currentTimeMillis
      const key = fingerprint(saved)
      const read = yield* lock.withPermit(
        Effect.gen(function* () {
          const previous = cache.entry
          const entry: Entry = previous?.key === key ? previous : { key, next: 0 }
          cache.entry = entry
          if (entry.read && now < entry.next) return entry.read
          const failure = (
            status: ForkSubscriptionUsage.Provider["status"],
            message: string,
            retryAt: number,
          ): ForkSubscriptionUsage.Provider => ({
            ...entry.snapshot,
            providerID,
            status,
            windows: entry.snapshot?.windows ?? [],
            stale: true,
            message,
            retryAt,
          })
          // Resolve inside the coalesced operation: Integration owns refreshing and persisting tokens.
          const request = Effect.gen(function* () {
            const resolved = yield* integration.connection.resolve(connection).pipe(Effect.result)
            const time = yield* Clock.currentTimeMillis
            if (resolved._tag === "Failure")
              return failure("auth_required", "Reconnect this provider to view subscription usage.", time + 300000)
            const info = resolved.success
            if (!info || info.type !== "oauth" || !info.access || info.expires <= time)
              return failure("auth_required", "Reconnect this provider to view subscription usage.", time + 300000)
            if (!["chatgpt-browser", "chatgpt-headless"].includes(info.methodID))
              return failure(
                "unsupported_auth",
                "Subscription usage requires a ChatGPT OAuth connection.",
                time + 300000,
              )
            // Normal refresh may rotate tokens; update the cache identity to the value just persisted.
            // Changed account metadata cannot inherit the previous account's successful windows.
            if (JSON.stringify(saved.value.metadata) !== JSON.stringify(info.metadata)) entry.snapshot = undefined
            entry.key = fingerprint(new Credential.Info({ ...saved, value: info }))
            return yield* Effect.gen(function* () {
              const accountID = info.metadata?.accountID
              const response = yield* client.get("https://chatgpt.com/backend-api/wham/usage", {
                headers: {
                  Authorization: `Bearer ${info.access}`,
                  Accept: "application/json",
                  "User-Agent": App.useragent(app),
                  ...(typeof accountID === "string" && { "ChatGPT-Account-Id": accountID }),
                },
              })
              const received = yield* Clock.currentTimeMillis
              if (response.status === 401)
                return failure(
                  "auth_required",
                  "Reconnect this provider to view subscription usage.",
                  received + 300000,
                )
              if (response.status === 403)
                return failure(
                  "unavailable",
                  "Subscription usage is not available for this connection.",
                  received + 300000,
                )
              if (response.status === 429) {
                const raw = response.headers["retry-after"]?.trim()
                const retry =
                  raw && /^\d+(?:\.\d+)?$/.test(raw) ? received + Number(raw) * 1000 : raw ? Date.parse(raw) : NaN
                return failure(
                  "rate_limited",
                  "The provider is rate limiting usage requests. Try again later.",
                  Math.max(received + 300000, timestamp(retry) ? retry : 0),
                )
              }
              if (response.status !== 200)
                return failure("unavailable", "Subscription usage is temporarily unavailable.", received + 60000)
              const payload = yield* response.json
              const updatedAt = yield* Clock.currentTimeMillis
              const normalized = normalize(payload, updatedAt)
              if (!normalized.windows.length)
                return failure(
                  "unavailable",
                  "The provider did not return supported usage windows.",
                  updatedAt + 300000,
                )
              const result: ForkSubscriptionUsage.Provider = {
                providerID,
                status: "ok",
                ...normalized,
                updatedAt,
                stale: false,
              }
              entry.snapshot = result
              return result
            }).pipe(Effect.scoped, Effect.timeout("10 seconds"))
          }).pipe(
            Effect.provideService(HttpClient.TracerDisabledWhen, () => true),
            Effect.catch(() =>
              Clock.currentTimeMillis.pipe(
                Effect.map((time) =>
                  failure("error", "Unable to read subscription usage. Try again later.", time + 60000),
                ),
              ),
            ),
            Effect.tap((result) =>
              Effect.sync(() => {
                entry.next = result.retryAt ?? (result.updatedAt ?? now) + 300000
                entry.read = Effect.succeed(result)
              }),
            ),
            Effect.onInterrupt(() =>
              Effect.sync(() => {
                if (cache.entry === entry) cache.entry = { key: entry.key, next: 0, snapshot: entry.snapshot }
              }),
            ),
          )
          entry.read = yield* Effect.cached(request)
          entry.next = Infinity
          return entry.read
        }),
      )
      return yield* read
    })
    return Service.of({
      get: Effect.fn("ForkSubscriptionUsage.get")(function* () {
        return { providers: [yield* get()] }
      }),
    })
  }),
)

export const node = makeLocationNode({
  service: Service,
  layer,
  deps: [Credential.node, Integration.node, LayerNodePlatform.httpClient, App.node],
})
