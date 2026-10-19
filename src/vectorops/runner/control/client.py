"""Async host client with response correlation and independent live events."""

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
import typing
from uuid import uuid4

import pydantic

from vectorops.runner import configuration as config_api, diagnostics
from vectorops.runner import global_config
from vectorops.runner import models, usage
from vectorops.runner.api.definitions import WorkflowDescriptor
from vectorops.runner.authentication import models as am
from vectorops.runner.mcp import management as mm, models as mcp_models
from vectorops.runner.mcp import content as mcp_content
from vectorops.runner.mcp.pool import SessionStatus
from vectorops.runner.control import auth, protocol as p, transport
from vectorops.runner.control.server import RunnerServer
from vectorops.runner.control.service import RunnerService
from vectorops.runner.runner import execution_state, history_query, mailbox
from vectorops.runner.runner import inspection
from vectorops.runner.runner import inbox
from vectorops.runner.worktrees import models as worktrees


class RunnerClient:
    def __init__(self, connection: transport.Connection, instance_id: str) -> None:
        self.connection, self.instance_id = connection, instance_id
        self.pending: dict[str, asyncio.Future[pydantic.JsonValue]] = {}
        self.events: asyncio.Queue[p.Event] = asyncio.Queue(maxsize=256)
        self.closed = asyncio.Event()
        self.failure: Exception = ConnectionError("runner connection closed")
        self._reader = asyncio.create_task(self._read())

    @classmethod
    async def connect(cls, stream: transport.ByteStream, token: str) -> typing.Self:
        connection = transport.Connection(stream)
        try:
            instance_id = await auth.connect(connection, token)
        except BaseException:
            await connection.aclose()
            raise
        return cls(connection, instance_id)

    @classmethod
    async def tcp(
        cls,
        port: int,
        token: typing.Optional[str] = None,
        *,
        host: str = "127.0.0.1",
        config_dir: typing.Optional[Path] = None,
    ) -> typing.Self:
        if host not in ("127.0.0.1", "::1"):
            raise ValueError("unencrypted runner TCP requires a loopback endpoint")
        if token is None:
            token = (
                await global_config.GlobalConfiguration(config_dir).load()
            ).control_token
        reader, writer = await asyncio.open_connection(host, port)
        return await cls.connect(transport.TCPStream(reader, writer), token)

    async def _read(self) -> None:
        try:
            while True:
                packet = await self.connection.receive()
                if isinstance(packet, p.Event):
                    try:
                        self.events.put_nowait(packet)
                    except asyncio.QueueFull as exc:
                        raise p.RemoteError(
                            "resync_required",
                            "client event queue overflowed; reconnect and reload history",
                        ) from exc
                elif isinstance(packet, (p.Response, p.Error, p.Pong)):
                    future = (
                        self.pending.get(packet.id) if packet.id is not None else None
                    )
                    if future is not None and not future.done():
                        if isinstance(packet, p.Error):
                            future.set_exception(
                                p.RemoteError(
                                    packet.code, packet.message, packet.details
                                )
                            )
                        else:
                            future.set_result(
                                packet.result
                                if isinstance(packet, p.Response)
                                else None
                            )
                else:
                    raise ValueError("unexpected runner packet")
        except Exception as exc:
            self.failure = exc
        finally:
            self.closed.set()
            for future in self.pending.values():
                if not future.done():
                    future.set_exception(self.failure)
            await self.connection.aclose()

    async def request(
        self,
        method: str,
        params: typing.Optional[dict[str, pydantic.JsonValue]] = None,
        *,
        timeout: float = 30,
    ) -> pydantic.JsonValue:
        if self.closed.is_set():
            raise self.failure
        identifier = uuid4().hex
        future: asyncio.Future[pydantic.JsonValue] = (
            asyncio.get_running_loop().create_future()
        )
        self.pending[identifier] = future
        try:
            async with asyncio.timeout(timeout):
                await self.connection.send(
                    p.Request(
                        id=identifier,
                        method=method,
                        params={} if params is None else params,
                    )
                )
                return await future
        finally:
            self.pending.pop(identifier, None)
            if not future.done():
                future.cancel()
            elif not future.cancelled():
                future.exception()

    async def next_event(self) -> p.Event:
        if not self.events.empty():
            return self.events.get_nowait()
        if self.closed.is_set():
            raise self.failure
        event = asyncio.create_task(self.events.get())
        closed = asyncio.create_task(self.closed.wait())
        try:
            await asyncio.wait((event, closed), return_when=asyncio.FIRST_COMPLETED)
            if event.done():
                return event.result()
            raise self.failure
        finally:
            event.cancel()
            closed.cancel()
            await asyncio.gather(event, closed, return_exceptions=True)

    async def start(self, params: p.Start) -> p.Accepted:
        return p.Accepted.model_validate(
            await self.request("executions.start", params.model_dump(mode="json"))
        )

    async def create_worktree(
        self, params: worktrees.CreateRequest
    ) -> worktrees.Record:
        return worktrees.Record.model_validate(
            await self.request(
                "worktrees.create", params.model_dump(mode="json"), timeout=180
            )
        )

    async def list_worktrees(
        self, params: worktrees.Query = worktrees.Query()
    ) -> worktrees.Page:
        return worktrees.Page.model_validate(
            await self.request(
                "worktrees.list", params.model_dump(mode="json"), timeout=180
            )
        )

    async def get_worktree(self, params: worktrees.Get) -> worktrees.Record:
        return worktrees.Record.model_validate(
            await self.request(
                "worktrees.get", params.model_dump(mode="json"), timeout=180
            )
        )

    async def update_worktree(self, params: worktrees.Update) -> worktrees.Record:
        return worktrees.Record.model_validate(
            await self.request("worktrees.update", params.model_dump(mode="json"))
        )

    async def delete_worktree(self, params: worktrees.Delete) -> worktrees.Record:
        return worktrees.Record.model_validate(
            await self.request(
                "worktrees.delete", params.model_dump(mode="json"), timeout=180
            )
        )

    async def iter_events(self) -> AsyncIterator[p.Event]:
        while True:
            yield await self.next_event()

    async def info(self) -> pydantic.JsonValue:
        return await self.request("runner.info")

    async def runner_usage(self) -> usage.RunnerUsage:
        return usage.RunnerUsage.model_validate(await self.request("runner.usage"))

    async def reset_runner_usage(self) -> usage.RunnerUsage:
        return usage.RunnerUsage.model_validate(
            await self.request("runner.usage.reset")
        )

    async def workflow_state(
        self, execution_id: str, workflow_id: str
    ) -> models.Workflow:
        query = p.WorkflowState(execution_id=execution_id, workflow_id=workflow_id)
        return models.Workflow.model_validate(
            await self.request("workflows.state", query.model_dump(mode="json"))
        )

    async def get_configuration(self, name: str = "") -> config_api.Snapshot:
        return config_api.Snapshot.model_validate(
            await self.request(
                "configuration.get",
                config_api.Query(name=name).model_dump(mode="json"),
            )
        )

    async def set_configuration(
        self, name: str, value: pydantic.JsonValue, *, permanent: bool = False
    ) -> None:
        update = config_api.Update(name=name, value=value, permanent=permanent)
        await self.request("configuration.set", update.model_dump(mode="json"))

    async def reset_configuration(self, name: str) -> None:
        await self.request(
            "configuration.reset",
            config_api.Query(name=name).model_dump(mode="json"),
        )

    async def suggest_configuration(
        self, name: str, prefix: str = ""
    ) -> config_api.Suggestions:
        query = config_api.SuggestQuery(name=name, prefix=prefix)
        return config_api.Suggestions.model_validate(
            await self.request("configuration.suggest", query.model_dump(mode="json"))
        )

    async def logs(self) -> diagnostics.LogListing:
        return diagnostics.LogListing.model_validate(await self.request("logs.list"))

    async def download_logs(
        self, query: diagnostics.LogDownload
    ) -> diagnostics.LogChunk:
        return diagnostics.LogChunk.model_validate(
            await self.request("logs.download", query.model_dump(mode="json"))
        )

    async def executions(
        self, params: typing.Optional[p.ExecutionList] = None
    ) -> p.ExecutionPage:
        return p.ExecutionPage.model_validate(
            await self.request(
                "executions.list", (params or p.ExecutionList()).model_dump(mode="json")
            )
        )

    async def workflows(
        self, params: typing.Optional[p.ExecutionList] = None
    ) -> p.WorkflowPage:
        return p.WorkflowPage.model_validate(
            await self.request(
                "workflows.list", (params or p.ExecutionList()).model_dump(mode="json")
            )
        )

    async def search_workflows(self, params: p.WorkflowSearch) -> p.WorkflowPage:
        return p.WorkflowPage.model_validate(
            await self.request("workflows.search", params.model_dump(mode="json"))
        )

    async def describe_workflow(self, reference: str) -> WorkflowDescriptor:
        return WorkflowDescriptor.model_validate(
            await self.request("workflows.describe", {"reference": reference})
        )

    async def execution_summaries(
        self, params: typing.Optional[p.SummaryQuery] = None
    ) -> p.SummaryPage:
        return p.SummaryPage.model_validate(
            await self.request(
                "executions.summaries",
                (params or p.SummaryQuery()).model_dump(mode="json"),
            )
        )

    async def execution_summary(self, execution_id: str) -> inspection.ExecutionSummary:
        return inspection.ExecutionSummary.model_validate(
            await self.request("executions.summary", {"execution_id": execution_id})
        )

    async def active(
        self, params: typing.Optional[p.ExecutionList] = None
    ) -> p.ActivePage:
        return p.ActivePage.model_validate(
            await self.request(
                "executions.active",
                (params or p.ExecutionList()).model_dump(mode="json"),
            )
        )

    async def shutdown(
        self, mode: typing.Literal["cancel", "drain"] = "cancel"
    ) -> None:
        await self.request("runner.shutdown", {"mode": mode})

    async def resume(self, params: p.Resume) -> p.Accepted:
        return p.Accepted.model_validate(
            await self.request("executions.resume", params.model_dump(mode="json"))
        )

    async def branch(self, params: p.Branch) -> p.BranchAccepted:
        return p.BranchAccepted.model_validate(
            await self.request("executions.branch", params.model_dump(mode="json"))
        )

    async def current(
        self, execution_id: typing.Optional[str] = None
    ) -> typing.Optional[p.Active]:
        result = await self.request(
            "executions.current",
            p.Current(execution_id=execution_id).model_dump(mode="json"),
        )
        return None if result is None else p.Active.model_validate(result)

    async def execution(self, execution_id: str) -> execution_state.ExecutionRecord:
        return execution_state.ExecutionRecord.model_validate(
            await self.request("executions.get", {"execution_id": execution_id})
        )

    async def stop(self, execution_id: str) -> p.Active:
        return p.Active.model_validate(
            await self.request("executions.stop", {"execution_id": execution_id})
        )

    async def history(
        self, query: history_query.HistoryQuery
    ) -> history_query.HistoryPage:
        return history_query.HistoryPage.model_validate(
            await self.request("history.query", query.model_dump(mode="json"))
        )

    async def subscribe(self, params: typing.Optional[p.Subscribe] = None) -> str:
        result = await self.request(
            "events.subscribe", (params or p.Subscribe()).model_dump(mode="json")
        )
        return p.Unsubscribe.model_validate(result).subscription_id

    async def pending_inputs(self, query: mailbox.RequestQuery) -> mailbox.RequestPage:
        return mailbox.RequestPage.model_validate(
            await self.request("mailbox.list", query.model_dump(mode="json"))
        )

    async def input_request(
        self, reference: mailbox.RequestRef
    ) -> mailbox.InputRequest:
        return mailbox.InputRequest.model_validate(
            await self.request("mailbox.get", reference.model_dump(mode="json"))
        )

    async def answer_input(self, answer: mailbox.Answer) -> mailbox.InputRequest:
        return mailbox.InputRequest.model_validate(
            await self.request("mailbox.respond", answer.model_dump(mode="json"))
        )

    async def unsubscribe(self, subscription_id: str) -> None:
        await self.request("events.unsubscribe", {"subscription_id": subscription_id})

    async def list_mailboxes(self, execution_id: str) -> inbox.Listing:
        return inbox.Listing.model_validate(
            await self.request("mailbox.inboxes.list", {"execution_id": execution_id})
        )

    async def inspect_mailbox(self, address: inbox.Address) -> inbox.Status:
        return inbox.Status.model_validate(
            await self.request(
                "mailbox.inboxes.inspect", address.model_dump(mode="json")
            )
        )

    async def send_message(self, request: inbox.Send) -> inbox.Message:
        return inbox.Message.model_validate(
            await self.request("mailbox.inboxes.send", request.model_dump(mode="json"))
        )

    async def list_messages(self, query: inbox.MessageQuery) -> inbox.MessagePage:
        return inbox.MessagePage.model_validate(
            await self.request(
                "mailbox.inboxes.messages", query.model_dump(mode="json")
            )
        )

    async def remove_message(
        self, reference: inbox.MessageRef
    ) -> typing.Optional[inbox.Message]:
        result = await self.request(
            "mailbox.inboxes.remove", reference.model_dump(mode="json")
        )
        return None if result is None else inbox.Message.model_validate(result)

    async def pop_message(
        self, address: inbox.Address
    ) -> typing.Optional[inbox.Message]:
        result = await self.request(
            "mailbox.inboxes.pop", address.model_dump(mode="json")
        )
        return None if result is None else inbox.Message.model_validate(result)

    async def pending_authentication(self) -> list[am.Requirement]:
        result = await self.request("auth.list")
        if not isinstance(result, dict):
            raise ValueError("invalid authentication listing")
        return pydantic.TypeAdapter(list[am.Requirement]).validate_python(
            result["requests"]
        )

    async def login(
        self, provider: str, profile: typing.Optional[str] = None
    ) -> am.Requirement:
        return am.Requirement.model_validate(
            await self.request(
                "auth.login",
                am.Login(provider=provider, profile=profile).model_dump(mode="json"),
            )
        )

    async def authentication_providers(self) -> am.AuthenticationProviders:
        return am.AuthenticationProviders.model_validate(
            await self.request("auth.providers")
        )

    async def authentication_profiles(self) -> am.ProfileListing:
        return am.ProfileListing.model_validate(await self.request("auth.profile.list"))

    async def add_authentication_profile(self, name: str) -> am.ProfileResult:
        return am.ProfileResult.model_validate(
            await self.request(
                "auth.profile.add", am.ProfileQuery(name=name).model_dump(mode="json")
            )
        )

    async def switch_authentication_profile(self, name: str) -> am.ProfileResult:
        return am.ProfileResult.model_validate(
            await self.request(
                "auth.profile.switch",
                am.ProfileQuery(name=name).model_dump(mode="json"),
            )
        )

    async def delete_authentication_profile(self, name: str) -> am.ProfileResult:
        return am.ProfileResult.model_validate(
            await self.request(
                "auth.profile.delete",
                am.ProfileQuery(name=name).model_dump(mode="json"),
            )
        )

    async def mcp_servers(self, component: str = "mcp") -> mm.ServerPage:
        return mm.ServerPage.model_validate(
            await self.request("mcp.list", {"component": component})
        )

    async def mcp_status(self, server: str, *, component: str = "mcp") -> SessionStatus:
        return SessionStatus.model_validate(
            await self.request("mcp.status", {"server": server, "component": component})
        )

    async def mcp_connect(
        self, server: str, *, component: str = "mcp", timeout: float = 600
    ) -> SessionStatus:
        return SessionStatus.model_validate(
            await self.request(
                "mcp.connect",
                {"server": server, "component": component},
                timeout=timeout,
            )
        )

    async def mcp_login(
        self, server: str, *, component: str = "mcp", timeout: float = 600
    ) -> am.Requirement:
        return am.Requirement.model_validate(
            await self.request(
                "mcp.login", {"server": server, "component": component}, timeout=timeout
            )
        )

    async def mcp_refresh(
        self, server: str, *, component: str = "mcp", timeout: float = 600
    ) -> mcp_models.Catalog:
        return mcp_models.Catalog.model_validate(
            await self.request(
                "mcp.refresh",
                {"server": server, "component": component},
                timeout=timeout,
            )
        )

    async def mcp_disconnect(self, server: str, *, component: str = "mcp") -> None:
        await self.request("mcp.disconnect", {"server": server, "component": component})

    async def mcp_logout(self, server: str, *, component: str = "mcp") -> None:
        await self.request("mcp.logout", {"server": server, "component": component})

    async def mcp_discover(
        self, server: str, *, component: str = "mcp", timeout: float = 600
    ) -> mcp_models.Catalog:
        return mcp_models.Catalog.model_validate(
            await self.request(
                "mcp.discover",
                {"server": server, "component": component},
                timeout=timeout,
            )
        )

    async def mcp_content(
        self, server: str, *, component: str = "mcp", timeout: float = 600
    ) -> mcp_content.ContentCatalog:
        return mcp_content.ContentCatalog.model_validate(
            await self.request(
                "mcp.content",
                {"server": server, "component": component},
                timeout=timeout,
            )
        )

    async def mcp_roots(
        self,
        server: str,
        roots: typing.Optional[list[mcp_models.Root]],
        *,
        component: str = "mcp",
    ) -> SessionStatus:
        query = mm.RootUpdate(server=server, component=component, roots=roots)
        return SessionStatus.model_validate(
            await self.request("mcp.roots.set", query.model_dump(mode="json"))
        )

    async def claim_authentication(self, identifier: str) -> am.Claim:
        return am.Claim.model_validate(
            await self.request("auth.claim", {"id": identifier})
        )

    async def authentication_ready(self, attempt: am.Attempt) -> str:
        result = await self.request("auth.ready", attempt.model_dump(mode="json"))
        if not isinstance(result, dict) or not isinstance(result.get("url"), str):
            raise ValueError("invalid authorization URL")
        return typing.cast(str, result["url"])

    async def complete_authentication(self, callback: am.Callback) -> None:
        await self.request(
            "auth.complete", callback.model_dump(mode="json"), timeout=120
        )

    async def cancel_authentication(self, identifier: str) -> None:
        await self.request("auth.cancel", {"id": identifier})

    async def provision_credentials(self, credential: am.Credential) -> None:
        await self.request(
            "auth.provision", credential.model_dump(mode="json", exclude_unset=True)
        )

    async def credentials(
        self,
        profile: typing.Optional[str] = None,
        *,
        after: typing.Optional[str] = None,
        limit: int = 100,
    ) -> am.CredentialPage:
        query = am.CredentialListQuery(profile=profile, after=after, limit=limit)
        return am.CredentialPage.model_validate(
            await self.request("auth.credentials.list", query.model_dump(mode="json"))
        )

    async def check_credential(
        self,
        provider: str,
        profile: typing.Optional[str] = None,
    ) -> am.CredentialStatus:
        query = am.CredentialQuery(provider=provider, profile=profile)
        return am.CredentialStatus.model_validate(
            await self.request("auth.credentials.check", query.model_dump(mode="json"))
        )

    async def delete_credential(
        self,
        provider: str,
        profile: typing.Optional[str] = None,
    ) -> am.CredentialDeleted:
        query = am.CredentialQuery(provider=provider, profile=profile)
        return am.CredentialDeleted.model_validate(
            await self.request("auth.credentials.delete", query.model_dump(mode="json"))
        )

    async def aclose(self) -> None:
        self._reader.cancel()
        await asyncio.gather(self._reader, return_exceptions=True)

    async def __aenter__(self) -> typing.Self:
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.aclose()


@asynccontextmanager
async def embedded(
    path: Path,
    *,
    config_dir: typing.Optional[Path] = None,
    workspace_directory: typing.Optional[str] = None,
) -> AsyncIterator[RunnerClient]:
    token = (await global_config.GlobalConfiguration(config_dir).load()).control_token
    async with RunnerService(
        path, config_dir=config_dir, workspace_directory=workspace_directory
    ) as service:
        server = RunnerServer(service, token)
        host, worker = transport.memory_pair()
        server.attach(worker)
        try:
            async with await RunnerClient.connect(host, token) as client:
                yield client
        finally:
            await server.aclose()
