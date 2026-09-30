import { ForkSubscriptionUsage } from "@opencode/core/fork-subscription-usage"
import { Plugin } from "@opencode/core/plugin"
import { Effect } from "effect"
import { HttpApiBuilder } from "effect/unstable/httpapi"
import { Api } from "../api"

export const ForkSubscriptionUsageHandler = HttpApiBuilder.group(Api, "server.forkSubscriptionUsage", (handlers) =>
  handlers.handle(
    "fork.subscriptionUsage",
    Effect.fn(function* () {
      yield* Plugin.awaitActivation
      const usage = yield* ForkSubscriptionUsage.Service
      return yield* usage.get()
    }),
  ),
)
