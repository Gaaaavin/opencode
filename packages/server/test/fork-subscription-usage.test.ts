import { expect } from "bun:test"
import { Context, Effect, Layer } from "effect"
import { HttpClient, HttpClientResponse, HttpEffect, HttpRouter, HttpServer } from "effect/unstable/http"
import { Credential } from "@opencode/core/credential"
import { Integration } from "@opencode/core/integration"
import { LayerNodePlatform } from "@opencode/util/effect/app-node-platform"
import { tmpdirScoped } from "../../core/test/fixture/tmpdir"
import { it } from "../../core/test/lib/effect"
import { createRoutes } from "../src/routes"

const fixture = Effect.gen(function* () {
  const tmp = yield* tmpdirScoped("subscription-usage-v2-http-")
  const state = { reads: 0, usage: 0, fail: false }
  const saved = new Credential.Info({
    id: Credential.ID.make("cred_mock"),
    integrationID: Integration.ID.make("openai"),
    label: "Mock",
    value: Credential.OAuth.make({
      type: "oauth",
      methodID: Integration.MethodID.make("chatgpt-browser"),
      access: "mock-access",
      refresh: "mock-refresh",
      expires: 9000000000000,
      metadata: { accountID: "mock-account" },
    }),
  })
  const credentials = Layer.mock(Credential.Service)({
    all: () => Effect.succeed([saved]),
    list: (id) =>
      Effect.suspend(() => {
        state.reads++
        return state.fail
          ? Effect.die("mock private storage details")
          : Effect.succeed(id === saved.integrationID ? [saved] : [])
      }),
    get: (id) => Effect.succeed(id === saved.id ? saved : undefined),
  })
  const http = HttpClient.make((request) =>
    Effect.sync(() => {
      if (request.url.includes("/wham/usage")) {
        state.usage++
        return HttpClientResponse.fromWeb(request, new Response("mock secret upstream details", { status: 401 }))
      }
      // Actual V2 OpenAI plugin discovers Codex models during location boot; keep it mocked too.
      return HttpClientResponse.fromWeb(
        request,
        new Response(JSON.stringify({ models: [] }), { headers: { "content-type": "application/json" } }),
      )
    }),
  )
  const context = yield* Layer.build(
    createRoutes(
      {
        password: "test-password",
        database: { path: ":memory:" },
        models: { fetch: false },
        fs: { filewatcher: false },
        config: { directory: tmp.path, project: false, content: "{}" },
      },
      undefined,
      [
        Credential.node.replace(credentials),
        LayerNodePlatform.httpClient.replace(Layer.succeed(HttpClient.HttpClient, http)),
      ],
    ).pipe(Layer.provide(HttpServer.layerServices)),
  )
  const handler = Context.get(context, HttpRouter.HttpRouter).asHttpEffect().pipe(HttpEffect.toWebHandlerWith(context))
  const request = (authorized = true) =>
    Effect.promise((signal) => {
      const url = new URL("/api/fork/subscription-usage", "http://opencode.local")
      url.searchParams.set("location[directory]", tmp.path)
      return handler(
        new Request(url, {
          signal,
          headers: authorized ? { authorization: `Basic ${btoa("opencode:test-password")}` } : {},
        }),
      )
    })
  return { state, request }
})

it.live(
  "V2 quota route rejects unauthenticated calls before credential reads",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture
      const response = yield* f.request(false)
      expect(response.status).toBe(401)
      expect(response.headers.get("www-authenticate")).toBe('Basic realm="Secure Area"')
      expect(f.state.reads).toBe(0)
      expect(f.state.usage).toBe(0)
    }),
  15000,
)
it.live(
  "actual V2 route registration returns raw safe provider contract and persistent cache",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture
      const response = yield* f.request()
      expect(response.status).toBe(200)
      const body = yield* Effect.promise(() => response.json())
      expect(body).toMatchObject({
        providers: [{ providerID: "openai", status: "auth_required", windows: [], stale: true }],
      })
      expect(body.providers).toHaveLength(1)
      expect(body).not.toHaveProperty("location")
      expect(JSON.stringify(body)).not.toContain("mock")
      expect((yield* f.request()).status).toBe(200)
      expect(f.state.usage).toBe(1)
    }),
  15000,
)
it.live(
  "V2 quota route maps storage defects to safe standard HTTP 500",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture
      yield* f.request()
      f.state.fail = true
      const response = yield* f.request()
      expect(response.status).toBe(500)
      expect(yield* Effect.promise(() => response.text())).not.toContain("mock")
    }),
  15000,
)
