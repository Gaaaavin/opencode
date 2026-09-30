export * as ForkSubscriptionUsage from "./fork-subscription-usage.js"

import { Schema } from "effect"
import { optional } from "./schema.js"

export interface Window extends Schema.Schema.Type<typeof Window> {}
export const Window = Schema.Struct({
  id: Schema.String,
  label: Schema.String,
  usedPercent: Schema.Number,
  remainingPercent: Schema.Number,
  resetsAt: optional(Schema.Number),
  windowSeconds: optional(Schema.Number),
}).annotate({ identifier: "ForkSubscriptionUsage.Window" })

export interface Provider extends Schema.Schema.Type<typeof Provider> {}
export const Provider = Schema.Struct({
  providerID: Schema.Literals(["openai"]),
  status: Schema.Literals([
    "ok",
    "not_connected",
    "unsupported_auth",
    "auth_required",
    "unavailable",
    "rate_limited",
    "error",
  ]),
  windows: Schema.Array(Window),
  plan: optional(Schema.String),
  updatedAt: optional(Schema.Number),
  stale: Schema.Boolean,
  message: optional(Schema.String),
  retryAt: optional(Schema.Number),
}).annotate({ identifier: "ForkSubscriptionUsage.Provider" })

export interface Result extends Schema.Schema.Type<typeof Result> {}
export const Result = Schema.Struct({ providers: Schema.Tuple([Provider]) }).annotate({
  identifier: "ForkSubscriptionUsage.Result",
})
