import { Money } from "@opencode/schema/money"
import { Agent } from "@opencode/schema/agent"
import { Session } from "@opencode/schema/session"
import { OpenAIResponses } from "@opencode/ai/protocols/openai-responses"
import { LLMRequest } from "@opencode/ai"
import { LLMClient, RequestExecutor } from "@opencode/ai/route"
import { ModelResolver } from "@opencode/core/model-resolver"
import { Config } from "@opencode/core/config"
import { ConfigProviderPlugin } from "@opencode/core/config/plugin/provider"
import { Document } from "@opencode/schema/config"
import { describe, expect } from "bun:test"
import { ConfigProvider, DateTime, Effect, Layer, Stream } from "effect"
import { HttpClientRequest, HttpClientResponse } from "effect/unstable/http"
import { Credential } from "@opencode/core/credential"
import { Integration } from "@opencode/core/integration"
import { Location } from "@opencode/core/location"
import { Model } from "@opencode/core/model"
import { Plugin } from "@opencode/core/plugin"
import { PluginHost } from "@opencode/core/plugin/host"
import { PluginHooks } from "@opencode/core/plugin/hooks"
import { GithubCopilotPlugin } from "@opencode/core/plugin/provider/github-copilot"
import { OpenAIPlugin } from "@opencode/core/plugin/provider/openai"
import { Project } from "@opencode/core/project"
import { Provider } from "@opencode/core/provider"
import { AbsolutePath } from "@opencode/core/schema"
import { SessionModelRequest } from "@opencode/core/session/model-request"
import { SessionModelTransport } from "@opencode/core/session/model-transport"
import { SessionRunnerModel } from "@opencode/core/session/runner/model"
import { testEffect } from "../lib/effect"
import { drain } from "../lib/clock"
import { PluginTestLayer } from "./fixture"

const it = testEffect(PluginTestLayer)

const astraCost = [
  {
    input: Money.USDPerMillionTokens.make(10),
    output: Money.USDPerMillionTokens.make(50),
    cache: { read: Money.USDPerMillionTokens.make(1), write: Money.USDPerMillionTokens.make(12.5) },
  },
  {
    tier: { type: "context", size: 272_000 },
    input: Money.USDPerMillionTokens.make(20),
    output: Money.USDPerMillionTokens.make(75),
    cache: { read: Money.USDPerMillionTokens.make(2), write: Money.USDPerMillionTokens.make(25) },
  },
] satisfies Model.Info["cost"]

const astraFastCost = [
  {
    input: Money.USDPerMillionTokens.make(20),
    output: Money.USDPerMillionTokens.make(100),
    cache: { read: Money.USDPerMillionTokens.make(2), write: Money.USDPerMillionTokens.make(25) },
  },
  {
    tier: { type: "context", size: 272_000 },
    input: Money.USDPerMillionTokens.make(40),
    output: Money.USDPerMillionTokens.make(150),
    cache: { read: Money.USDPerMillionTokens.make(4), write: Money.USDPerMillionTokens.make(50) },
  },
] satisfies Model.Info["cost"]

const addPlugin = Effect.fn(function* () {
  const plugin = yield* Plugin.Service
  const host = yield* PluginHost.make(plugin)
  yield* OpenAIPlugin.effect(host)
})

const addGithubCopilotPlugin = Effect.fn(function* () {
  const plugin = yield* Plugin.Service
  const host = yield* PluginHost.make(plugin)
  yield* GithubCopilotPlugin.effect(host)
})

function required<T>(value: T | undefined): T {
  if (value === undefined) throw new Error("Expected value")
  return value
}

const request = Effect.fn(function* (providerID: Provider.ID, baseURL: string) {
  const hooks = yield* PluginHooks.Service
  const event = yield* hooks.trigger("session", "model.request", {
    sessionID: Session.ID.make("ses_test"),
    agent: Agent.ID.make("build"),
    model: Model.Ref.make({ providerID, id: Model.ID.make("gpt-5.5") }),
    kind: "primary",
    baseURL,
    headers: {},
  })
  return {
    baseURL: event.baseURL,
    headers: event.headers,
    hasHttpHooks:
      (yield* hooks.has("session", "http.request", providerID)) ||
      (yield* hooks.has("session", "http.response", providerID)),
  }
})

describe("OpenAIPlugin", () => {
  for (const method of ["chatgpt-browser", "chatgpt-headless", "key"]) {
    for (const mode of [undefined, "priority", "provider-body", "model-body"]) {
      const tier = mode === "provider-body" || mode === "model-body" ? "priority" : mode
      it.effect(`prices and prepares ${method} ${mode ?? "standard"} on HTTP and WebSocket`, () =>
        Effect.gen(function* () {
          const catalog = yield* Provider.Service
          const models = yield* Model.Service
          const credentials = yield* Credential.Service
          const id = Model.ID.make(
            mode === "priority"
              ? "gpt-6-astra-fast"
              : mode === "model-body"
                ? "gpt-6-astra-custom"
                : "gpt-6-astra",
          )
          yield* catalog.transform((catalog) => {
            catalog.update(Provider.ID.openai, (draft) => {
              draft.package = "@opencode/ai/providers/openai"
            })
            catalog.models.update(
              Provider.ID.openai,
              mode === "model-body" ? Model.ID.make("gpt-6-astra") : id,
              (draft) => {
                draft.modelID = Model.ID.make("gpt-6-astra")
                draft.name = "GPT-6 Astra"
                draft.cost = structuredClone(mode === "priority" ? astraFastCost : astraCost)
                draft.limit = { context: 1_050_000, output: 128_000 }
                draft.body = tier === "priority" ? { service_tier: "priority" } : {}
              },
            )
          })
          yield* credentials.create({
            integrationID: Integration.ID.make("openai"),
            value:
              method === "key"
                ? Credential.Key.make({ type: "key", key: "sk-test" })
                : Credential.OAuth.make({
                    type: "oauth",
                    methodID: Integration.MethodID.make(method),
                    access: "chatgpt-token",
                    refresh: "refresh",
                    expires: Date.now() + 60_000,
                    metadata: { accountID: "acct_test" },
                  }),
          })
          yield* addPlugin()
          if (mode === "provider-body" || mode === "model-body") {
            const plugin = yield* Plugin.Service
            const host = yield* PluginHost.make(plugin)
            yield* ConfigProviderPlugin.Plugin.effect(host).pipe(
              Effect.provide(
                Config.testLayer([
                  new Document({
                    type: "document",
                    info: {
                      providers: {
                        openai:
                          mode === "provider-body"
                            ? { body: { service_tier: "priority" } }
                            : {
                                models: {
                                  "gpt-6-astra-custom": {
                                    modelID: Model.ID.make("gpt-6-astra"),
                                    body: { service_tier: "priority" },
                                  },
                                },
                              },
                      },
                    },
                  }),
                ]),
              ),
            )
          }
          const resolver = yield* ModelResolver.Service
          const model = required(yield* resolver.resolve(Model.Ref.make({ providerID: Provider.ID.openai, id })))
          expect(required(yield* models.get(Provider.ID.openai, id)).limit.output).toBe(128_000)
          expect(String(model.model.id)).toBe("gpt-6-astra")
          expect(model.cost).toEqual(mode === "priority" ? astraFastCost : astraCost)
          const requests = yield* SessionModelRequest.Service
          const prepared = yield* requests.primary({
            session: Session.Info.make({
              id: Session.ID.make("ses_output_caps"),
              projectID: Project.ID.global,
              cost: Money.USD.zero,
              tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
              time: { created: DateTime.makeUnsafe(0), updated: DateTime.makeUnsafe(0) },
              location: Location.Ref.make({ directory: AbsolutePath.make("/project") }),
            }),
            agent: Agent.ID.make("build"),
            model,
            tools: { definitions: [], execute: () => Effect.die("unused tool execution") },
            system: [],
            messages: [],
          })
          expect(prepared.request.generation?.maxTokens).toBe(128_000)
          for (const override of [undefined, 1024]) {
            for (const transport of ["http", "websocket", "fallback"]) {
              const request =
                override === undefined
                  ? prepared.request
                  : LLMRequest.update(prepared.request, {
                      http: {
                        ...prepared.request.http,
                        body: { ...prepared.request.http?.body, max_output_tokens: override },
                      },
                    })
              const check = (body: Record<string, unknown>) => {
                expect(body.model).toBe("gpt-6-astra")
                if (method === "key") expect(body.max_output_tokens).toBe(override ?? 128_000)
                if (method !== "key") expect(body).not.toHaveProperty("max_output_tokens")
                if (tier) expect(body.service_tier).toBe(tier)
              }
              const completed = JSON.stringify({ type: "response.completed", response: { id: "resp_test" } })
              yield* LLMClient.stream(request, {
                webSocket:
                  transport !== "http"
                    ? {
                        execute: (exchange) =>
                          Effect.gen(function* () {
                            expect(exchange.connect.headers).not.toHaveProperty("x-codex-routing-hint")
                            check(JSON.parse((yield* exchange.driver.create(undefined)).message))
                            return {
                              frames: transport === "fallback" ? exchange.fallback() : Stream.make(completed),
                              complete: Effect.void,
                            }
                          }),
                      }
                    : undefined,
              }).pipe(
                Stream.runDrain,
                Effect.provide(
                  LLMClient.layer.pipe(
                    Layer.provide(
                      Layer.succeed(RequestExecutor.Service, {
                        execute: (sent) =>
                          Effect.gen(function* () {
                            expect(transport).not.toBe("websocket")
                            const http = yield* HttpClientRequest.toWeb(sent).pipe(Effect.orDie)
                            expect(http.headers.has("x-codex-routing-hint")).toBe(false)
                            check(JSON.parse(yield* Effect.promise(() => http.text())))
                            return HttpClientResponse.fromWeb(
                              sent,
                              new Response(`data: ${completed}\n\n`, {
                                headers: { "content-type": "text/event-stream" },
                              }),
                            )
                          }),
                      }),
                    ),
                    Layer.fresh,
                  ),
                ),
              )
            }
          }
        }).pipe(
          Effect.provide(SessionModelRequest.layer),
          Effect.provide(ModelResolver.layer),
          Effect.provideService(SessionModelTransport.Service, {
            bind: () => ({ execute: () => Effect.die("unused WebSocket execution") }),
            close: () => Effect.void,
            closeAll: Effect.void,
          }),
        ),
      )
    }
  }

  it.effect("registers browser and headless ChatGPT OAuth methods", () =>
    Effect.gen(function* () {
      yield* addPlugin()
      const integrations = yield* Integration.Service
      expect((yield* integrations.get(Integration.ID.make("openai")))?.methods).toEqual([
        {
          id: Integration.MethodID.make("chatgpt-browser"),
          type: "oauth",
          label: "ChatGPT Pro/Plus (browser)",
        },
        {
          id: Integration.MethodID.make("chatgpt-headless"),
          type: "oauth",
          label: "ChatGPT Pro/Plus (headless)",
        },
      ])
    }),
  )

  it.effect("filters the OpenAI catalog to codex-eligible models under a ChatGPT connection", () =>
    Effect.gen(function* () {
      const catalog = yield* Provider.Service
      const models = yield* Model.Service
      const credentials = yield* Credential.Service
      yield* catalog.transform((catalog) => {
        catalog.update(Provider.ID.openai, (draft) => {
          draft.package = "@opencode/ai/providers/openai"
        })
        catalog.models.update(Provider.ID.openai, Model.ID.make("gpt-5.5"), (model) => {
          model.limit = { context: 1_050_000, input: 922_000, output: 128_000 }
          model.cost = [
            {
              input: Money.USDPerMillionTokens.make(1),
              output: Money.USDPerMillionTokens.make(2),
              cache: {
                read: Money.USDPerMillionTokens.make(0.1),
                write: Money.USDPerMillionTokens.zero,
              },
            },
          ]
        })
        catalog.models.update(Provider.ID.openai, Model.ID.make("gpt-5.5-pro"), () => {})
        catalog.models.update(Provider.ID.openai, Model.ID.make("gpt-5.4"), (model) => {
          model.limit = { context: 1_050_000, input: 922_000, output: 64_000 }
        })
        catalog.models.update(Provider.ID.openai, Model.ID.make("gpt-5.4-pro"), (model) => {
          model.modelID = Model.ID.make("gpt-5.4")
          model.body = { reasoning: { mode: "pro" } }
        })
        catalog.models.update(Provider.ID.openai, Model.ID.make("gpt-5.6"), () => {})
        catalog.models.update(Provider.ID.openai, Model.ID.make("gpt-5.6-sol"), (model) => {
          model.limit = { context: 1_050_000, input: 922_000, output: 128_000 }
        })
        catalog.models.update(Provider.ID.openai, Model.ID.make("gpt-4.1"), () => {})
        catalog.models.update(Provider.ID.openai, Model.ID.make("gpt-6-astra"), (model) => {
          model.limit = { context: 1_050_000, input: 922_000, output: 128_000 }
        })
        catalog.models.update(Provider.ID.openai, Model.ID.make("gpt-5.10"), (model) => {
          model.limit = { context: 1_050_000, input: 922_000, output: 128_000 }
        })
        catalog.models.update(Provider.ID.openai, Model.ID.make("gpt-5"), () => {})
        catalog.models.update(Provider.ID.openai, Model.ID.make("gpt-5.04-astra"), () => {})
        catalog.models.update(Provider.ID.openai, Model.ID.make("gpt-4.99"), () => {})
      })
      yield* credentials.create({
        integrationID: Integration.ID.make("openai"),
        value: Credential.OAuth.make({
          type: "oauth",
          methodID: Integration.MethodID.make("chatgpt-browser"),
          access: "chatgpt-token",
          refresh: "refresh",
          expires: Date.now() + 60_000,
          metadata: { accountID: "acct_123" },
        }),
      })
      yield* addPlugin()

      const direct = yield* request(Provider.ID.openai, "https://api.openai.com/v1")
      const custom = yield* request(Provider.ID.make("custom-openai"), "https://custom.example/v1")
      const proxy = yield* request(Provider.ID.openai, "https://proxy.example/v1?region=us")

      const provider = required(yield* catalog.get(Provider.ID.openai))
      expect(provider.package).toBe("@opencode/ai/providers/openai")
      expect(provider.settings).toMatchObject({ baseURL: "https://chatgpt.com/backend-api/codex" })
      expect(provider.headers).toMatchObject({
        originator: "opencode",
        "chatgpt-account-id": "acct_123",
        "x-codex-beta-features": "remote_compaction_v2",
      })
      expect(direct.baseURL).toBe("https://chatgpt.com/backend-api/codex")
      expect(direct.headers).toMatchObject({ originator: "opencode", "session-id": "ses_test" })
      expect(direct.hasHttpHooks).toBe(false)
      expect(custom.headers).not.toHaveProperty("originator")
      expect(proxy.baseURL).toBe("https://proxy.example/v1?region=us")
      expect(proxy.headers).toMatchObject({ originator: "opencode", "session-id": "ses_test" })
      const eligible = required(yield* models.get(Provider.ID.openai, Model.ID.make("gpt-5.5")))
      expect(eligible.package).toBe("@opencode/ai/providers/openai")
      expect(eligible.headers).toMatchObject({ originator: "opencode", "chatgpt-account-id": "acct_123" })
      expect(eligible.cost).toEqual([
        {
          input: Money.USDPerMillionTokens.make(1),
          output: Money.USDPerMillionTokens.make(2),
          cache: { read: Money.USDPerMillionTokens.make(0.1), write: Money.USDPerMillionTokens.zero },
        },
      ])
      expect(eligible.limit).toEqual({ context: 400_000, input: 272_000, output: 128_000 })
      expect(eligible.enabled).toBe(true)
      expect(required(yield* models.get(Provider.ID.openai, Model.ID.make("gpt-5.5-pro"))).enabled).toBe(false)
      expect(required(yield* models.get(Provider.ID.openai, Model.ID.make("gpt-5.4-pro"))).enabled).toBe(false)
      expect(required(yield* models.get(Provider.ID.openai, Model.ID.make("gpt-5.4"))).enabled).toBe(false)
      expect(required(yield* models.get(Provider.ID.openai, Model.ID.make("gpt-5.6"))).enabled).toBe(false)
      const gpt56 = required(yield* models.get(Provider.ID.openai, Model.ID.make("gpt-5.6-sol")))
      expect(gpt56.enabled).toBe(true)
      expect(gpt56.limit).toEqual({ context: 400_000, input: 272_000, output: 128_000 })
      expect(required(yield* models.get(Provider.ID.openai, Model.ID.make("gpt-4.1"))).enabled).toBe(false)
      expect(required(yield* models.get(Provider.ID.openai, Model.ID.make("gpt-6-astra"))).enabled).toBe(true)
      expect(required(yield* models.get(Provider.ID.openai, Model.ID.make("gpt-5.10"))).enabled).toBe(true)
      expect(required(yield* models.get(Provider.ID.openai, Model.ID.make("gpt-5"))).enabled).toBe(false)
      expect(required(yield* models.get(Provider.ID.openai, Model.ID.make("gpt-5.04-astra"))).enabled).toBe(false)
      expect(required(yield* models.get(Provider.ID.openai, Model.ID.make("gpt-4.99"))).enabled).toBe(false)
    }),
  )

  it.effect("keeps the full OpenAI catalog under an API key connection", () =>
    Effect.gen(function* () {
      const catalog = yield* Provider.Service
      const models = yield* Model.Service
      const credentials = yield* Credential.Service
      yield* catalog.transform((catalog) => {
        catalog.update(Provider.ID.openai, (draft) => {
          draft.package = "@opencode/ai/providers/openai"
        })
        catalog.models.update(Provider.ID.openai, Model.ID.make("gpt-5.5"), (model) => {
          model.limit = { context: 1_050_000, input: 922_000, output: 128_000 }
        })
        catalog.models.update(Provider.ID.openai, Model.ID.make("gpt-4.1"), () => {})
      })
      yield* credentials.create({
        integrationID: Integration.ID.make("openai"),
        value: Credential.Key.make({ type: "key", key: "sk-test" }),
      })
      yield* addPlugin()

      const direct = yield* request(Provider.ID.openai, "https://api.openai.com/v1")

      const provider = required(yield* catalog.get(Provider.ID.openai))
      const model = required(yield* models.get(Provider.ID.openai, Model.ID.make("gpt-5.5")))
      expect(model.package).toBe("@opencode/ai/providers/openai")
      expect(model.enabled).toBe(true)
      expect(model.limit).toEqual({ context: 1_050_000, input: 922_000, output: 128_000 })
      expect(provider.settings?.transport).toBe("websocket")
      expect(model.settings?.transport).toBeUndefined()
      expect(direct.headers).not.toHaveProperty("originator")
      expect(direct.baseURL).toBe("https://api.openai.com/v1")
      expect(provider.headers).not.toHaveProperty("x-codex-beta-features")
      expect(direct.hasHttpHooks).toBe(false)
      expect(provider.headers).not.toHaveProperty("originator")
      expect(required(yield* models.get(Provider.ID.openai, Model.ID.make("gpt-4.1"))).enabled).toBe(true)
    }),
  )

  it.effect("selects WebSocket only from explicit policy", () =>
    Effect.gen(function* () {
      const credentials = yield* Credential.Service
      yield* credentials.create({
        integrationID: Integration.ID.make("openai"),
        value: Credential.Key.make({ type: "key", key: "sk-test" }),
      })
      yield* addPlugin()
      yield* addGithubCopilotPlugin()
      const executor = { execute: () => Effect.die("unused WebSocket execution") }
      const transport = SessionModelTransport.Service.of({
        bind: () => executor,
        close: () => Effect.void,
        closeAll: Effect.void,
      })
      const sessionID = Session.ID.make("ses_websocket_hooks")
      const agentID = Agent.ID.make("build")
      const route = OpenAIResponses.route.with({
        id: "deployment-responses",
        provider: Provider.ID.azure,
      })
      const prepare = (preference?: Provider.Transport) =>
        Effect.gen(function* () {
          const model = SessionRunnerModel.resolved(route.model({ id: "gpt-5.5" }), {
            capabilities: { tools: true, input: ["text"], output: ["text"] },
            cost: [],
            limit: { context: 200_000, output: 32_000 },
            transport: preference,
          })
          const requests = yield* SessionModelRequest.Service
          return yield* requests.primary({
            session: Session.Info.make({
              id: sessionID,
              projectID: Project.ID.global,
              cost: Money.USD.zero,
              tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
              time: { created: DateTime.makeUnsafe(0), updated: DateTime.makeUnsafe(0) },
              location: Location.Ref.make({ directory: AbsolutePath.make("/project") }),
            }),
            agent: agentID,
            model,
            tools: { definitions: [], execute: () => Effect.die("unused tool execution") },
            system: [],
            messages: [],
            webSocket: "session",
          })
        }).pipe(
          Effect.provide(SessionModelRequest.layer),
          Effect.provideService(SessionModelTransport.Service, transport),
        )

      const prepared = yield* prepare("websocket")
      const defaulted = yield* prepare()
      const disabled = yield* prepare("http")

      expect(prepared.options.webSocket).toBe(executor)
      expect(prepared.options.http).toBeUndefined()
      expect(defaulted.options.webSocket).toBeUndefined()
      expect(disabled.options.webSocket).toBeUndefined()
    }),
  )
})
