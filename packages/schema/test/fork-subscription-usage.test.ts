import { expect, test } from "bun:test"
import { Schema } from "effect"
import { ForkSubscriptionUsage } from "@opencode/schema/fork-subscription-usage"

test("subscription contract requires exactly one OpenAI result", () => {
  const decode = Schema.decodeUnknownSync(ForkSubscriptionUsage.Result)
  const provider = {
    providerID: "openai",
    status: "not_connected",
    windows: [],
    stale: false,
  } satisfies ForkSubscriptionUsage.Provider
  expect(decode({ providers: [provider] })).toEqual({ providers: [provider] })
  for (const providers of [[], [provider, provider], [{ ...provider, providerID: "other" }]])
    expect(() => decode({ providers })).toThrow()
})

test("subscription contract omits undefined optional properties", () => {
  const provider = Schema.encodeSync(ForkSubscriptionUsage.Provider)({
    providerID: "openai",
    status: "ok",
    stale: false,
    plan: undefined,
    updatedAt: undefined,
    message: undefined,
    retryAt: undefined,
    windows: [
      {
        id: "primary_window",
        label: "5-hour",
        usedPercent: 25,
        remainingPercent: 75,
        resetsAt: undefined,
        windowSeconds: undefined,
      },
    ],
  })
  expect(Object.keys(provider)).toEqual(["providerID", "status", "windows", "stale"])
  expect(Object.keys(provider.windows[0] ?? {})).toEqual(["id", "label", "usedPercent", "remainingPercent"])
})
test("subscription schemas have distinct stable public identifiers", () => {
  expect(
    [ForkSubscriptionUsage.Window, ForkSubscriptionUsage.Provider, ForkSubscriptionUsage.Result].map(
      (schema) => schema.ast.annotations?.identifier,
    ),
  ).toEqual(["ForkSubscriptionUsage.Window", "ForkSubscriptionUsage.Provider", "ForkSubscriptionUsage.Result"])
})
