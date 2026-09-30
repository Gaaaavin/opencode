import { ForkSubscriptionUsage } from "@opencode/schema/fork-subscription-usage"
import { HttpApiEndpoint, HttpApiGroup, OpenApi } from "effect/unstable/httpapi"
import { LocationQuery } from "./location.js"

/** Fork extension; raw result intentionally has no Location.response envelope. */
export const ForkSubscriptionUsageGroup = HttpApiGroup.make("server.forkSubscriptionUsage").add(
  HttpApiEndpoint.get("fork.subscriptionUsage", "/api/fork/subscription-usage", {
    query: LocationQuery,
    success: ForkSubscriptionUsage.Result,
  }).annotateMerge(
    OpenApi.annotations({ identifier: "fork.subscriptionUsage", summary: "Get subscription quota usage" }),
  ),
)
