from __future__ import annotations

import asyncio
import json
import logging
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode

import websockets

from app.agents.orchestrator import find_mentioned_agents, handle_message
from app.agents.runtime import is_agent
from app.core.config import settings
from app.core.context import (
    current_agent_id,
    current_org_id,
    current_room_id,
    current_sender,
)
from app.core.database import SessionLocal
from app.integrations.switch.agent_registry import (
    RegisteredAgent,
    registry,
    set_connection_status,
)
from app.models.switch import SwitchConnectionStatus, SwitchEvent
from app.services.audit_service import audit_service
from app.services.common import iso, utcnow

log = logging.getLogger("regmind.switch.supervisor")

_SAFE_NAME = re.compile(r"[^A-Za-z0-9._-]+")


def _utc_iso() -> str:
    return utcnow().astimezone(timezone.utc).isoformat()


def _safe_filename(name: str, fallback: str = "attachment") -> str:
    cleaned = _SAFE_NAME.sub("_", Path(name or fallback).name)
    return cleaned[:180] or fallback


_LOCAL_PATH_RE = re.compile(r"(?im)\b[A-Za-z]:[\\][^\s'\"]+")


def _redact_for_log(value: object) -> object:
    """Return a shallow snapshot of a frame for diagnostics.
    Keeps MXC/media URIs visible for debugging attachment flow.
    """
    if isinstance(value, dict):
        return {k: _redact_for_log(v) for k, v in value.items() if k not in ("api_key", "token", "key")}
    if isinstance(value, list):
        return [_redact_for_log(v) for v in value[:5]]
    if isinstance(value, str):
        return value[:200]
    return value



def _first_present(mapping: dict, *keys: str):
    for key in keys:
        value = mapping.get(key)
        if value not in (None, ""):
            return value
    return ""


def _normalize_attachment_entry(entry: dict) -> dict | None:
    if not isinstance(entry, dict):
        return None

    info = entry.get("info") if isinstance(entry.get("info"), dict) else {}
    mxc = str(
        _first_present(entry, "mxc", "uri", "url", "media_uri", "remote_url", "media")
        or _first_present(info, "url", "uri", "mxc")
        or ""
    )
    filename = str(
        _first_present(entry, "filename", "name", "display_name")
        or _first_present(info, "filename", "name")
        or _first_present(entry, "body")
        or ""
    )
    size = entry.get("size") or entry.get("fileSize") or info.get("size") or 0
    mimetype = str(
        _first_present(entry, "mimetype", "mime_type", "contentType", "content_type")
        or _first_present(info, "mimetype", "mime_type")
        or ""
    )
    msgtype = str(_first_present(entry, "msgtype", "type") or "")

    if mxc or filename:
        return {
            "mxc": mxc,
            "filename": filename,
            "size": size,
            "mimetype": mimetype,
            "msgtype": msgtype,
        }
    return None


def _normalize_attachments(raw) -> list[dict]:
    """Normalize Switch attachment payloads from the several shapes Switch can emit."""

    if isinstance(raw, dict):
        raw = [raw]
    if not isinstance(raw, list):
        return []

    normalized: list[dict] = []
    for entry in raw:
        item = _normalize_attachment_entry(entry)
        if item:
            normalized.append(item)
    return normalized


def _content_dict(mapping: dict) -> dict:
    content = mapping.get("content") if isinstance(mapping, dict) else None
    return content if isinstance(content, dict) else {}


def _message_body(payload: dict, data: dict) -> str:
    for mapping in (payload, data):
        for key in ("body", "text", "caption", "formatted_body"):
            value = mapping.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()

        content = _content_dict(mapping)
        for key in ("body", "text", "caption", "formatted_body"):
            value = content.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()

    return ""


def _single_media_attachment(payload: dict, data: dict) -> list[dict]:
    """Extract a lone m.file/m.image attachment from direct/content fields."""

    for source in (payload, data):
        if not isinstance(source, dict):
            continue

        content = _content_dict(source)

        direct = _normalize_attachment_entry(source)
        if direct and _first_present(source, "uri", "mxc", "url", "media_uri", "remote_url"):
            return [direct]
        if direct and str(_first_present(source, "msgtype", "type") or "") in {"m.file", "m.image"}:
            return [direct]

        content_item = _normalize_attachment_entry(content)
        if content_item and (
            content_item.get("mxc")
            or str(_first_present(content, "msgtype", "type") or "") in {"m.file", "m.image"}
        ):
            return [content_item]

        # Some bridges put a single attachment under info.file / info.attachment.
        for key in ("info", "file", "attachment"):
            nested = None
            if isinstance(source.get(key), dict):
                nested = source.get(key)
            elif isinstance(content.get(key), dict):
                nested = content.get(key)

            if nested:
                item = _normalize_attachment_entry(nested)
                if item and item.get("mxc"):
                    return [item]

        # Some payloads expose the media URI directly on the top-level event.
        for key in ("url", "uri", "mxc", "media_uri", "remote_url"):
            direct_uri = str(source.get(key) or "")
            if direct_uri:
                return [
                    _normalize_attachment_entry(
                        {
                            "mxc": direct_uri,
                            "filename": str(
                                source.get("filename")
                                or content.get("filename")
                                or source.get("body")
                                or content.get("body")
                                or "attachment"
                            ),
                            "size": source.get("size") or source.get("fileSize") or content.get("size") or 0,
                            "mimetype": str(
                                source.get("mimetype")
                                or source.get("mime_type")
                                or content.get("mimetype")
                                or content.get("mime_type")
                                or ""
                            ),
                            "msgtype": str(
                                source.get("msgtype")
                                or source.get("type")
                                or content.get("msgtype")
                                or content.get("type")
                                or ""
                            ),
                        }
                    )
                ]

    return []


def _is_media_message(payload: dict, data: dict, attachments: list[dict]) -> bool:
    if attachments:
        return True

    for mapping in (payload, data):
        if str(_first_present(mapping, "msgtype", "type") or "") in {"m.file", "m.image"}:
            return True

        content = _content_dict(mapping)
        if str(_first_present(content, "msgtype", "type") or "") in {"m.file", "m.image"}:
            return True

    return False


def _event_fields(data: dict) -> tuple[str, str, str, str, str, list[dict], dict]:
    """Extract common fields from the current and legacy event shapes."""

    payload = data.get("payload") if isinstance(data, dict) else None
    if not isinstance(payload, dict):
        payload = {}

    if not payload and isinstance(data, dict):
        fallback_keys = (
            "body",
            "content",
            "text",
            "caption",
            "attachments",
            "media",
            "sender",
            "sender_name",
        )
        if any(key in data for key in fallback_keys):
            payload = data

    room_id = str(
        _first_present(data, "room_id", "roomId", "channel_id", "channelId", "workspace_room_id")
        or _first_present(payload, "room_id", "roomId", "channel_id", "channelId")
        or ""
    )
    message_id = str(
        _first_present(payload, "message_id", "messageId", "id", "event_id", "eventId")
        or _first_present(data, "message_id", "messageId", "id", "event_id", "eventId")
        or ""
    )
    sender = str(
        _first_present(payload, "sender", "sender_id", "user_id", "senderName")
        or _first_present(data, "sender", "sender_id", "user_id", "senderName")
        or ""
    )
    sender_name = str(
        _first_present(
            payload,
            "sender_name",
            "senderName",
            "display_name",
            "username",
        )
        or _first_present(data, "sender_name", "senderName", "display_name", "username")
        or sender
    )
    sender_kind = str(
        _first_present(payload, "sender_kind", "senderKind", "kind")
        or _first_present(data, "sender_kind", "senderKind", "kind")
        or ""
    )
    attachments: list[dict] = []
    for source in (payload, data):
        if not isinstance(source, dict):
            continue
        for key in ("attachments", "media", "files"):
            if source.get(key) is not None:
                attachments.extend(_normalize_attachments(source.get(key) or []))
        content = _content_dict(source)
        if content and content.get("attachments") is not None:
            attachments.extend(_normalize_attachments(content.get("attachments") or []))
        if content and content.get("media") is not None:
            attachments.extend(_normalize_attachments(content.get("media") or []))
        if attachments:
            break

    if not attachments:
        attachments = _single_media_attachment(payload, data)
    return room_id, message_id, sender, sender_name, sender_kind, attachments, payload


def _mentioned_from_addressed(payload: dict, agent: RegisteredAgent) -> list[str]:
    target = _first_present(
        payload,
        "addressed_agent",
        "target_agent",
        "mentioned_agent",
        "addressed_to",
    )
    if target:
        target_text = str(target)
        if is_agent(target_text):
            return [target_text]
        for known in registry.agents.values():
            if target_text in (known.switch_agent_id, known.chat_name):
                return [known.chat_name]
        return []

    if payload.get("addressed"):
        return [agent.chat_name]
    return []


def _strip_client_paths(text: str) -> str:
    """Remove user-local Windows paths from the message body.

    The backend cannot read files under a sender's laptop path. The supervisor
    appends the server-side materialized path separately, so the agent should
    not mistake the client path for the file it is allowed to open.
    """

    return _LOCAL_PATH_RE.sub("", text).strip()


def _is_recent_processed(message_id: str) -> bool:
    if not message_id:
        return False
    db = SessionLocal()
    try:
        from sqlalchemy import select

        row = db.execute(
            select(SwitchEvent).where(
                SwitchEvent.message_id == message_id,
                SwitchEvent.processed.is_(True),
            )
        ).first()
        return row is not None
    finally:
        db.close()


def record_event(
    *,
    org_id: uuid.UUID | None,
    switch_agent_id: str,
    room_id: str,
    message_id: str,
    event_type: str,
    sender: str,
    payload: dict,
    processed: bool,
) -> None:
    db = SessionLocal()
    try:
        from sqlalchemy import select

        row = db.execute(
            select(SwitchEvent).where(
                SwitchEvent.message_id == message_id,
                SwitchEvent.switch_agent_id == switch_agent_id,
            )
        ).scalar_one_or_none()
        if row is None:
            row = SwitchEvent(
                org_id=org_id,
                switch_agent_id=switch_agent_id,
                room_id=room_id,
                message_id=message_id,
                event_type=event_type,
                sender=sender,
                payload=payload,
                processed=processed,
            )
            db.add(row)
        else:
            row.processed = processed
            row.payload = payload
        db.commit()
    finally:
        db.close()


def _record_debug_event(
    *,
    org_id: uuid.UUID | None,
    switch_agent_id: str,
    room_id: str,
    message_id: str,
    event_type: str,
    sender: str,
    payload: dict,
    reason: str,
    detail: dict | None = None,
) -> None:
    """Record a dropped or failed event for local diagnostics."""

    try:
        debug_payload = dict(payload or {})
        debug_payload["_regmind_debug"] = {
            "reason": reason,
            "detail": detail or {},
        }
        record_event(
            org_id=org_id,
            switch_agent_id=switch_agent_id,
            room_id=room_id,
            message_id=message_id,
            event_type=event_type,
            sender=sender,
            payload=debug_payload,
            processed=False,
        )
    except Exception:
        log.exception("Could not record dropped Switch message %s", message_id)


def _core_room_switch_id() -> str:
    """Return the Switch room id for the configured core compliance office."""
    db = SessionLocal()
    try:
        from sqlalchemy import select

        from app.models.switch import SwitchRoom

        row = db.execute(
            select(SwitchRoom.switch_room_id).where(SwitchRoom.name == "Compliance Office")
        ).scalar_one_or_none()
        return str(row or "")
    except Exception:
        log.exception("Could not resolve core room id")
        return ""
    finally:
        db.close()


class SwitchSupervisor:
    def __init__(self) -> None:
        self._task: asyncio.Task[None] | None = None
        self._stop = asyncio.Event()
        self._cursor = 0
        self._connection_id = ""
        self._started_at = ""
        self._last_error = ""

    @property
    def running(self) -> bool:
        return self._task is not None and not self._task.done()

    def status(self) -> dict[str, object]:
        agent = registry.supervisor_agent()
        return {
            "running": self.running,
            "configured": bool(settings.switch_url),
            "supervisor_agent": agent.chat_name if agent else "",
            "connection_id": self._connection_id,
            "started_at": self._started_at,
            "cursor": self._cursor,
            "last_error": self._last_error,
        }

    async def start(self) -> dict[str, object]:
        if self.running:
            return self.status()
        self._stop.clear()
        self._task = asyncio.create_task(self._run(), name="regmind-switch-supervisor")
        return self.status()

    async def stop(self) -> None:
        self._stop.set()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def restart(self) -> dict[str, object]:
        await self.stop()
        return await self.start()

    async def _run(self) -> None:
        attempt = 0
        while not self._stop.is_set():
            try:
                await registry.refresh()
                agent = registry.supervisor_agent()
                if agent is None:
                    self._last_error = "No Switch agents are provisioned yet"
                    await asyncio.sleep(15)
                    continue
                if not settings.switch_url:
                    self._last_error = "SWITCH_URL is not configured"
                    await asyncio.sleep(30)
                    continue
                self._last_error = ""
                await self._connect(agent)
                attempt = 0
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                attempt += 1
                self._last_error = str(exc)
                log.warning("Switch supervisor error: %s", exc)
                delay = min(30.0, 1.0 * (2 ** min(attempt, 5)))
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=delay)
                except asyncio.TimeoutError:
                    pass

    async def _connect(self, agent: RegisteredAgent) -> None:
        self._connection_id = str(uuid.uuid4())
        params = {
            "connection_id": self._connection_id,
            "scope": "all",
            "filter": "all",
            "start_from": str(self._cursor) if self._cursor else "head",
            "client": "regmind",
            "client_version": "1.0",
        }
        uri = (
            f"{settings.switch_ws_url}/agents/{agent.switch_agent_id}/connection/ws?"
            f"{urlencode(params)}"
        )
        log.info("Connecting Switch supervisor for %s", agent.chat_name)

        set_connection_status(
            agent.switch_agent_id, SwitchConnectionStatus.CONNECTED, _utc_iso()
        )
        self._started_at = _utc_iso()
        queue: asyncio.Queue[dict] = asyncio.Queue(maxsize=500)
        try:
            async with websockets.connect(
                uri,
                additional_headers={"Authorization": f"Bearer {agent.api_key}"},
                ping_interval=None,
                close_timeout=5,
                max_size=16 * 1024 * 1024,
            ) as socket:
                worker = asyncio.create_task(
                    self._worker(queue, agent), name="regmind-switch-worker"
                )
                try:
                    async for raw in socket:
                        try:
                            frame = json.loads(raw)
                        except (ValueError, TypeError):
                            continue
                        event = frame.get("event")
                        if frame.get("id") is not None:
                            self._cursor = int(frame["id"])
                        if event == "ping":
                            await socket.send(
                                json.dumps({"type": "pong", "cursor": self._cursor})
                            )
                        elif event == "connection_state":
                            log.info(
                                "Switch connection ready: %s",
                                (frame.get("data") or {}).get("connection_id"),
                            )
                        elif event == "refused":
                            detail = (frame.get("data") or {}).get("detail")
                            raise RuntimeError(f"Switch refused the connection: {detail}")
                        elif event == "evicted":
                            detail = (frame.get("data") or {}).get("code")
                            log.warning("Switch connection evicted: %s", detail)
                            return
                        elif event == "message":
                            data = frame.get("data") or {}
                            try:
                                room_id, message_id, sender, sender_name, sender_kind, attachments, payload = _event_fields(
                                    data
                                )
                                body = _message_body(payload, data).strip()
                                content = _content_dict(payload) or _content_dict(data)
                                msgtype = str(
                                    _first_present(payload, "msgtype", "type")
                                    or _first_present(content, "msgtype", "type")
                                    or ""
                                )
                                addressed = _first_present(
                                    payload,
                                    "addressed",
                                    "addressed_agent",
                                    "target_agent",
                                    "mentioned_agent",
                                    "addressed_to",
                                )
                                log.info(
                                    "Switch message received room=%s message_id=%s sender=%s sender_kind=%s body=%r attachments=%s msgtype=%s content_keys=%s addressed=%s data_keys=%s",
                                    room_id,
                                    message_id,
                                    sender_name or sender,
                                    sender_kind,
                                    body[:160],
                                    len(attachments),
                                    msgtype,
                                    sorted((content or {}).keys()),
                                    str(addressed),
                                    sorted(data.keys()),
                                )
                                log.info(
                                    "Switch raw message sample room=%s message_id=%s sender=%s content_sample=%s data_sample=%s",
                                    room_id,
                                    message_id,
                                    sender_name or sender,
                                    _redact_for_log(payload),
                                    _redact_for_log(data),
                                )
                            except Exception:
                                log.exception("Could not summarize incoming Switch message")
                            try:
                                queue.put_nowait(data)
                            except asyncio.QueueFull:
                                log.error(
                                    "Switch message queue full; dropping event %s",
                                    (data.get("payload") or {}).get("message_id"),
                                )
                finally:
                    worker.cancel()
                    try:
                        await worker
                    except (asyncio.CancelledError, Exception):
                        pass
        finally:
            set_connection_status(agent.switch_agent_id, SwitchConnectionStatus.DISCONNECTED)

    async def _worker(self, queue: asyncio.Queue[dict], agent: RegisteredAgent) -> None:
        while True:
            data = await queue.get()
            try:
                await self._handle_message(data, agent)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("Failed to handle Switch message")
            finally:
                queue.task_done()

    async def _handle_message(self, data: dict, agent: RegisteredAgent) -> None:
        (
            room_id,
            message_id,
            sender,
            sender_name,
            sender_kind,
            attachments,
            payload,
        ) = _event_fields(data)
        sender_name = sender_name or sender or "unknown"

        if sender_kind == "agent":
            log.debug(
                "Switch message ignored sender_agent room=%s message_id=%s sender=%s",
                room_id,
                message_id,
                sender_name,
            )
            return
        if sender_kind == "" and is_agent(sender_name):
            log.debug(
                "Switch message ignored sender_is_agent room=%s message_id=%s sender=%s",
                room_id,
                message_id,
                sender_name,
            )
            return

        if not room_id or not message_id:
            log.warning(
                "Switch message ignored missing room/message id room=%r message_id=%r data_keys=%s",
                room_id,
                message_id,
                sorted(data.keys()),
            )
            return

        if message_id in self._seen_messages():
            log.debug("Switch message ignored duplicate_message room=%s message_id=%s", room_id, message_id)
            return
        if _is_recent_processed(message_id):
            self._remember_message(message_id)
            log.debug("Switch message ignored already_processed room=%s message_id=%s", room_id, message_id)
            return

        body = _message_body(payload, data).strip()
        body = _strip_client_paths(body)
        file_refs: list[dict] = []
        file_failures: list[dict] = []
        for attachment in attachments:
            result = await self._download_attachment(room_id, attachment, agent)
            if result.get("path"):
                file_refs.append(result)
            else:
                file_failures.append(result)

        text = body
        if file_refs:
            if not text:
                text = "Please review the attached SEBI circular."
            file_lines = [
                f"- {ref.get('filename') or 'attachment'}: {ref['path']}"
                for ref in file_refs
            ]
            text += "\n\nAttached file(s):\n" + "\n".join(file_lines)
        if file_failures:
            if not text:
                text = "Please review the attached SEBI circular."
            failure_lines = [
                f"- {failure.get('filename') or 'attachment'}: {failure.get('error') or 'download failed'}"
                for failure in file_failures
            ]
            text += "\n\nAttachment download failure(s):\n" + "\n".join(failure_lines)

        addressed_value = _first_present(
            payload,
            "addressed",
            "addressed_agent",
            "target_agent",
            "mentioned_agent",
            "addressed_to",
        )

        media_like = _is_media_message(payload, data, attachments)
        if media_like and not attachments:
            if not text:
                text = "Please review the attached SEBI circular."
            text += "\n\nAttachment metadata was not provided by Switch; please re-upload the file."

        mentions = find_mentioned_agents(text, sender_name)
        if not mentions:
            mentions = _mentioned_from_addressed(payload, agent)
        if not mentions and (attachments or media_like):
            text = "@regmind-intake " + text
            mentions = ["regmind-intake"]

        # Core-room fallback: route human messages into the compliance office
        # even when Discord does not resolve a literal @regmind-intake mention.
        if not mentions:
            core_room_id = _core_room_switch_id()
            if core_room_id and room_id == core_room_id:
                if text or attachments:
                    text = "@regmind-intake " + text
                    mentions = ["regmind-intake"]

        if not mentions:
            # Route any unmentioned human message to regmind-intake.
            # Skip agent echoes and pure system/admin notices.
            if sender_kind == "agent":
                return
            if sender_name.lower() == "admin":
                return
            if body.strip().startswith("Added **") or body.strip().startswith("**Agent status in this room:**"):
                return

            # Route the message to intake even if body/attachments are empty.
            # Intake will ask for the file if needed.
            text = "@regmind-intake " + text
            mentions = ["regmind-intake"]

            log.info(
                "Switch message routed to intake (no mention) room=%s message_id=%s sender=%s body=%r attachments=%s media_like=%s",
                room_id,
                message_id,
                sender_name,
                body[:160],
                len(attachments),
                media_like,
            )

        self._remember_message(message_id)
        record_event(
            org_id=agent.org_id,
            switch_agent_id=agent.switch_agent_id,
            room_id=room_id,
            message_id=message_id,
            event_type="message",
            sender=sender,
            payload=payload,
            processed=False,
        )

        log.info(
            "Switch message routing room=%s message_id=%s sender=%s mentions=%s attachments=%s media_like=%s downloaded=%s failed=%s",
            room_id,
            message_id,
            sender_name,
            mentions,
            len(attachments),
            media_like,
            len(file_refs),
            len(file_failures),
        )

        org_token = current_org_id.set(agent.org_id)
        room_token = current_room_id.set(room_id)
        sender_token = current_sender.set(sender_name)
        agent_token = current_agent_id.set(agent.chat_name)
        try:
            await handle_message(room_id, sender_name, text, self._post)
            record_event(
                org_id=agent.org_id,
                switch_agent_id=agent.switch_agent_id,
                room_id=room_id,
                message_id=message_id,
                event_type="message",
                sender=sender,
                payload=payload,
                processed=True,
            )
        finally:
            current_agent_id.reset(agent_token)
            current_sender.reset(sender_token)
            current_room_id.reset(room_token)
            current_org_id.reset(org_token)

    async def _post(self, agent_name: str, room_id: str, text: str) -> None:
        if not text.strip():
            return
        try:
            await registry.post_as(agent_name, room_id, text)
        except Exception:
            log.exception("Could not post as %s in room %s", agent_name, room_id)
            await audit_service.log(
                actor="regmind-switch",
                action="agent_post_failed",
                ref_id=room_id,
                detail={"agent": agent_name, "room_id": room_id},
            )

    async def _download_attachment(
        self, room_id: str, attachment: dict, agent: RegisteredAgent
    ) -> dict:
        mxc = str(attachment.get("mxc") or "")

        filename = _safe_filename(str(attachment.get("filename") or "attachment"))
        try:
            size = int(attachment.get("size") or 0)
        except (TypeError, ValueError):
            size = 0
        result = {
            "path": "",
            "filename": filename,
            "mxc": mxc,
            "size": size,
            "error": "",
        }

        if not mxc:
            result["error"] = "missing_switch_uri"
            log.warning(
                "Could not download attachment %s: missing Switch media URI; keys=%s",
                filename,
                sorted(attachment.keys()),
            )
            return result

        log.info(
            "Switch Attachment download room=%s filename=%s mxc=%s size=%s",
            room_id,
            filename,
            mxc,
            size,
        )
        
        if size and size > settings.max_upload_mb * 1024 * 1024:
            result["error"] = f"oversized: {size} bytes"
            log.warning("Skipping oversized attachment %s (%s bytes)", filename, size)
            return result

        try:
            data = await agent.client.download_media(room_id, mxc)
        except Exception as exc:
            result["error"] = str(exc)
            log.warning("Could not download attachment %s: %s", filename, exc)
            return result

        try:
            directory = settings.storage_dir / "switch" / "attachments" / _safe_filename(room_id)
            directory.mkdir(parents=True, exist_ok=True)
            path = directory / f"{uuid.uuid4().hex[:12]}-{filename}"
            await asyncio.to_thread(path.write_bytes, data)
            result["path"] = str(path.resolve())
        except Exception as exc:
            result["error"] = str(exc)
            log.warning("Could not save attachment %s: %s", filename, exc)
            return result

        log.info(
            "Switch attachment materialized room=%s filename=%s bytes=%s path=%s",
            room_id,
            filename,
            len(data),
            result["path"],
        )
        return result

    _memory_seen: set[str] = set()

    def _seen_messages(self) -> set[str]:
        return self._memory_seen

    def _remember_message(self, message_id: str) -> None:
        self._memory_seen.add(message_id)
        if len(self._memory_seen) > 5000:
            self._memory_seen = set(list(self._memory_seen)[-2000:])


supervisor = SwitchSupervisor()


__all__ = ["SwitchSupervisor", "supervisor"]
