import asyncio
import base64

from aiohttp import web
import pydantic
import pytest

from tests.mcp.test_http import endpoint
from vectorops.runner.authentication import mcp_settings, models as am
from vectorops.runner.authentication.service import AuthenticationService
from vectorops.runner.mcp import models
from vectorops.runner.mcp.pool import SharedPool
from vectorops.runner.mcp.runtime import MCPRuntime
from vectorops.runner.mcp.binding import MCPTool
from vectorops.runner.mcp.discovery import Discovery, Ranking, SearchResult
from vectorops.runner.runner.activity_context import ActivityExecutionContext


@pytest.mark.parametrize("method", ["client_secret_basic", "client_secret_post"])
async def test_client_credentials_shared_cache_and_header_configuration(
    method: str,
) -> None:
    grants: list[dict[str, str]] = []
    seen_headers: list[str] = []
    base = ""

    async def handler(request: web.Request) -> web.Response:
        if request.path == "/token":
            form = {key: str(value) for key, value in (await request.post()).items()}
            grants.append(form)
            if method == "client_secret_basic":
                assert (
                    base64.b64decode(
                        request.headers["Authorization"].split()[1]
                    ).decode()
                    == "client:private-secret"
                )
                assert "client_secret" not in form
            else:
                assert form["client_secret"] == "private-secret"
            assert form["grant_type"] == "client_credentials"
            assert form["resource"] == base + "/mcp"
            return web.json_response(
                {
                    "access_token": f"token-{len(grants)}",
                    "token_type": "Bearer",
                    "expires_in": 3600,
                }
            )
        seen_headers.append(request.headers["Authorization"])
        assert request.headers["X-Tenant"] == "team"
        assert request.headers["X-Private"] == "private-secret"
        if request.method != "POST":
            return web.Response(status=405)
        data = await request.json()
        if "id" not in data:
            return web.Response(status=202)
        result = (
            {
                "protocolVersion": data["params"]["protocolVersion"],
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "fixture", "version": "1"},
            }
            if data["method"] == "initialize"
            else {"tools": []}
        )
        return web.json_response({"jsonrpc": "2.0", "id": data["id"], "result": result})

    app = web.Application()
    app.router.add_route("*", "/{path:.*}", handler)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "127.0.0.1", 0).start()
    base = f"http://127.0.0.1:{runner.addresses[0][1]}"
    auth = AuthenticationService()
    await auth.provision(
        am.Credential(
            provider="mcp", profile="secret", kind="api_key", secret="private-secret"
        )
    )
    settings = mcp_settings.ClientCredentials.model_validate(
        {
            "resource": base + "/mcp",
            "token_endpoint": base + "/token",
            "client_id": "client",
            "client_secret": "secret",
            "token_auth_method": method,
        }
    )
    config = models.HTTPServer(
        url=base + "/mcp",
        auth=settings,
        headers={"X-Tenant": "team"},
        header_credentials={"X-Private": "secret"},
    )
    pool = SharedPool(auth)
    first, second = MCPRuntime(auth, "first", pool=pool), MCPRuntime(
        auth, "second", pool=pool
    )
    try:
        results = await asyncio.gather(
            first.discover("test", config),
            second.discover("test", config),
            return_exceptions=True,
        )
        assert all(isinstance(item, models.Catalog) for item in results), results
        assert len(grants) == 1 and len(pool.runtime._connections) == 1
        assert not auth.pending()
        token = await auth.mcp_client_token(settings)
        refreshed = await asyncio.gather(
            *(auth.mcp_client_token(settings, rejected=token.secret) for _ in range(4))
        )
        assert len(grants) == 2 and all(item.secret == "token-2" for item in refreshed)
        assert "private-secret" not in config.model_dump_json()
        await first.close()
        await second.close()
        await pool.logout(config)
        assert not pool.status(config).connected
        assert await auth.mcp_secret("secret") == "private-secret"
    finally:
        await asyncio.gather(first.close(), second.close())
        await pool.close()
        await runner.cleanup()


async def test_custom_api_key_header() -> None:
    async def handler(request: web.Request) -> web.Response:
        assert request.headers["X-Api-Key"] == "private"
        assert "Authorization" not in request.headers
        if request.method != "POST":
            return web.Response(status=405)
        data = await request.json()
        if "id" not in data:
            return web.Response(status=202)
        result = {
            "protocolVersion": data["params"]["protocolVersion"],
            "capabilities": {},
            "serverInfo": {"name": "fixture", "version": "1"},
        }
        return web.json_response({"jsonrpc": "2.0", "id": data["id"], "result": result})

    auth = AuthenticationService()
    await auth.provision(
        am.Credential(provider="mcp", profile="key", kind="api_key", secret="private")
    )
    async with endpoint(handler) as url:
        runtime = MCPRuntime(auth)
        try:
            config = models.HTTPServer(
                url=url,
                auth=mcp_settings.HeaderAuth(
                    credential="key", header="X-Api-Key", prefix=""
                ),
            )
            assert not (await runtime.discover("test", config)).tools
        finally:
            await runtime.close()


async def test_weighted_discovery_titles_schemas_and_sources() -> None:
    config = models.StdioServer(command="unused")
    bindings = []
    for source, name, title, properties in [
        ("a", "first", "Invoices", {}),
        ("b", "second", "Other", {"invoice": {"type": "string"}}),
    ]:
        descriptor = models.Descriptor(
            remote_name=name,
            name=models.tool_name(source, config, name),
            title=title,
            description="Generic",
            input_schema=pydantic.TypeAdapter(
                dict[str, pydantic.JsonValue]
            ).validate_python({"type": "object", "properties": properties}),
        )
        catalog = models.Catalog(
            server_name=source,
            server=config,
            protocol_version="test",
            tools=[descriptor],
        )
        bindings.append(MCPTool(catalog, descriptor))
    search = Discovery(bindings, ranking=Ranking()).bindings[0]
    ctx = ActivityExecutionContext(None, "workflow", "step")
    result = await search.execute(ctx, {"query": "invoice"})
    assert result.content[0].type == "text"
    matches = SearchResult.model_validate_json(result.content[0].text)
    assert [item.name for item in matches.tools] == [
        binding.definition.name for binding in bindings
    ]
    filtered = await search.execute(ctx, {"query": "invoice", "source": "b"})
    assert filtered.content[0].type == "text"
    assert len(SearchResult.model_validate_json(filtered.content[0].text).tools) == 1
