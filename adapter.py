"""QQ via OneBot v11 (NapCat forward WebSocket) - Hermes platform adapter.

Ported from an OpenClaw ``extensions/qq`` channel plugin (TypeScript). Behaviour kept:

* private chats always trigger; group chats need an @ / a reply to the bot / a keyword / a poke
  (``require_mention``), admins may send bare ``/commands`` in groups;
* ``admins`` list, optional ``admin_only_chat``, ``allowed_groups`` / ``blocked_users``;
* Markdown stripping for QQ (``format_markdown``) incl. the LaTeX operator fix, plus ``anti_risk_mode``;
* long replies split by ``max_message_length`` with ``rate_limit_ms`` between chunks, first group
  chunk @-mentions the asker;
* inbound images / voice / video / files are downloaded into Hermes' media cache; reply and
  merged-forward context is expanded into ``<context_layers>`` like OpenClaw did;
* outbound images / voice / video / files: NapCat runs on this machine, so local files are passed
  as ``file://`` paths (no base64, no size patch needed); files use upload_*_file, a timed-out
  upload is reported as pending instead of being resent (avoids duplicates). Text follows the same
  rule: once a chunk may be on screen, ``send()`` never reports failure (the gateway would resend
  the whole reply);
* group triggers also carry the last ``history_limit`` group messages as ``<history>`` (OpenClaw's
  historyLimit); files / voice / video in a quoted message are downloaded like the message's own;
* slow plugin commands listed in ``background_commands`` run in their own task instead of holding
  the (group-shared) session while they work; ``command_aliases`` can rename bare ``/commands``.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import datetime
import json
import logging
import mimetypes
import os
import re
import shutil
import time
import uuid
from collections import OrderedDict, deque
from pathlib import Path
from typing import Any, Callable, Optional
from urllib.parse import unquote, urlparse

from gateway.config import Platform
from gateway.platforms._shared import extra_or_secret, send_error
from gateway.platforms.base import (
    BasePlatformAdapter,
    SendResult,
    cache_audio_from_bytes,
    cache_image_from_bytes,
    get_document_cache_dir,
    get_video_cache_dir,
)
from gateway.platforms.event import MessageEvent, MessageType

logger = logging.getLogger(__name__)

PLATFORM_NAME = "onebot"
_TRUTHY = {"1", "true", "yes", "on", "y"}
_MIB = 1024 * 1024
_MAX_INBOUND_IMAGES = 5
_IMAGE_MAX_BYTES = 64 * _MIB
_AUDIO_MAX_BYTES = 64 * _MIB
_LOOKUP_BUDGET = 30
_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) HermesOneBot/0.2.1"
_RESUME_WAIT_S = 20.0  # how long a half-sent reply waits for the reconnect before giving up the rest
_MAX_BACKGROUND_COMMANDS = 3
_BARE_CMD_RE = re.compile(r"^/([A-Za-z][A-Za-z0-9_-]*)(?:\s|$)")
_MEDIA_LINE_RE = re.compile(r"^\s*MEDIA:\s*(\S.*?)\s*$")
_IMAGE_EXTS = {".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp"}
_VIDEO_EXTS = {".mp4", ".mov", ".mkv", ".webm", ".avi", ".m4v"}
_AUDIO_EXTS = {".mp3", ".wav", ".ogg", ".m4a", ".flac", ".amr", ".silk", ".opus"}


# --------------------------------------------------------------------------- small helpers

def _as_bool(value: Any, default: bool) -> bool:
    if value is None or (isinstance(value, str) and not value.strip()):
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in _TRUTHY


def _as_int(value: Any, default: int) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _id_list(value: Any) -> list[str]:
    if value is None:
        return []
    items = value if isinstance(value, (list, tuple, set)) else re.split(r"[,\s，;；]+", str(value))
    return [str(x).strip() for x in items if str(x).strip()]


def _str_list(value: Any) -> list[str]:
    if value is None:
        return []
    items = value if isinstance(value, (list, tuple, set)) else re.split(r"[,，]", str(value))
    return [str(x).strip() for x in items if str(x).strip()]


def parse_chat_id(chat_id: Any) -> tuple[str, int]:
    """``g123`` / ``group:123`` -> ("group", 123); ``123`` / ``private:123`` -> ("private", 123)."""
    s = re.sub(r"^qq:", "", str(chat_id or "").strip(), flags=re.I)
    m = re.fullmatch(r"(?i)(?:g|group:?)(\d{4,12})", s)
    if m:
        return "group", int(m.group(1))
    m = re.fullmatch(r"(?i)(?:private:|user:|u)?(\d{4,12})", s)
    if m:
        return "private", int(m.group(1))
    raise ValueError(f"Invalid QQ chat id: {chat_id!r} (use <QQ number> or g<group id>)")


def _chat_id_for(kind: str, target: Any) -> str:
    return f"g{target}" if kind == "group" else str(target)


def _local_path_from_ref(ref: str) -> Optional[str]:
    """Local filesystem path for ``file://...`` / plain absolute path refs, else None."""
    if not ref:
        return None
    s = ref.strip()
    if s.lower().startswith("file:"):
        parsed = urlparse(s)
        p = unquote((parsed.netloc or "") + (parsed.path or "")) if not re.match(r"^[A-Za-z]:", parsed.netloc or "") \
            else unquote(parsed.netloc + parsed.path)
        if not p:
            p = unquote(s[5:].lstrip("/"))
        p = p.replace("\\", "/")
        if re.match(r"^/[A-Za-z]:/", p):
            p = p[1:]
        if re.match(r"^[A-Za-z]%3A", s[7:] if s.lower().startswith("file://") else "", re.I):
            p = unquote(s[7:])
        return os.path.normpath(p)
    if re.match(r"^[A-Za-z]:[\\/]", s) or s.startswith("/") or s.startswith("\\\\"):
        return os.path.normpath(s)
    return None


def _http_ref(d: dict) -> Optional[str]:
    """Download URL of a media segment: ``url``, or ``file`` when that is an http(s) URL."""
    file = str(d.get("file") or "")
    return d.get("url") or (file if re.match(r"^https?://", file, re.I) else None)


def _guess_image_ext(data: bytes, name_hint: str = "") -> str:
    head = data[:16]
    if head.startswith(b"\x89PNG"):
        return ".png"
    if head.startswith(b"\xff\xd8"):
        return ".jpg"
    if head.startswith(b"GIF8"):
        return ".gif"
    if head[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return ".webp"
    if head.startswith(b"BM"):
        return ".bmp"
    ext = os.path.splitext(name_hint or "")[1].lower()
    return ext if ext in {".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp"} else ".jpg"


def _safe_file_name(name: str, fallback: str = "file.bin") -> str:
    base = os.path.basename(str(name or "").replace("\\", "/")).strip()
    cleaned = re.sub(r'[\x00-\x1f<>:"|?*/\\]', "_", base).strip(" .")
    return cleaned or fallback


def _split_text(text: str, limit: int) -> list[str]:
    """Split on paragraph/line boundaries when possible (OpenClaw cut at the hard limit)."""
    limit = max(200, int(limit or 4000))
    out: list[str] = []
    rest = text
    while len(rest) > limit:
        window = rest[:limit]
        cut = max(window.rfind("\n\n"), window.rfind("\n"))
        if cut < limit // 2:
            cut = limit
        out.append(rest[:cut].rstrip())
        rest = rest[cut:].lstrip("\n")
    if rest.strip():
        out.append(rest)
    return [c for c in out if c.strip()]


# --------------------------------------------------------------------------- Markdown filter

_LATEX_OPS = (
    (re.compile(r"\\geq?(?![A-Za-z])"), "≥"),
    (re.compile(r"\\leq?(?![A-Za-z])"), "≤"),
    (re.compile(r"\\times(?![A-Za-z])"), "×"),
    (re.compile(r"\\div(?![A-Za-z])"), "÷"),
)


# The model sometimes parrots the gateway's own scaffolding back as a reply: the inbound envelope
# ("Message from QQ ...:\n<user_message>...</user_message>", "Gateway message origin (JSON data ...")
# or the tail of the system prompt (ending in "<!-- Context layers: ... -->"). Strip those before sending.
_LEAK_ENVELOPE_RE = re.compile(r"\A\s*Message from [^\n]{0,200}:\s*<user_message>.*?</user_message>\s*", re.S)
_LEAK_ORIGIN_RE = re.compile(r"\A\s*Gateway message origin \(JSON data[^\n]*\n\{[^\n]*\}\n[^\n]*\n*", re.S)
_LEAK_LAYERS_MARK = "<!-- Context layers:"


def _strip_prompt_leak(text: str) -> str:
    original = text
    i = text.find(_LEAK_LAYERS_MARK)
    if i >= 0:
        j = text.find("-->", i)
        text = text[j + 3:] if j >= 0 else ""
    for rx in (_LEAK_ENVELOPE_RE, _LEAK_ORIGIN_RE):
        m = rx.match(text)
        if m:
            text = text[m.end():]
    if text != original:
        logger.warning("[OneBot] stripped leaked prompt scaffolding from reply (%d -> %d chars)",
                       len(original), len(text.strip()))
        text = text.strip()
    return text


def strip_markdown(text: str, *, code_blocks: str = "placeholder") -> str:
    """Port of OpenClaw ``stripMarkdown``; QQ shows Markdown literally.

    Same step order as the original. Small fixes: links keep their URL, ``snake_case`` / ``2*3*4``
    are no longer eaten as italics, table separator rows are dropped, headings only at line start.
    ``code_blocks``: ``placeholder`` (OpenClaw behaviour, ``[代码块]``) or ``keep`` (drop fences only).
    """
    r = text
    for pattern, repl in _LATEX_OPS:
        r = pattern.sub(repl, r)
    # 1. fenced code first so the inline-code rule cannot tear the fences apart
    if code_blocks == "keep":
        r = re.sub(r"```[^\n`]*\n?([\s\S]*?)```", lambda m: m.group(1).rstrip("\n"), r)
    else:
        r = re.sub(r"```[\s\S]*?```", "[代码块]", r)
    # 2. horizontal rules
    r = re.sub(r"(?m)^[ \t]*(?:-{3,}|\*{3,}|_{3,})[ \t]*$", "", r)
    # 10a. table separator rows (|---|:--:|)
    r = re.sub(r"(?m)^[ \t]*\|?[ \t]*:?-+:?[ \t]*(?:\|[ \t]*:?-+:?[ \t]*)+\|?[ \t]*$\n?", "", r)
    # 3. bold before italic. Word guards are ASCII-only: Python's \w also matches 汉字, which
    #    left the markers in "这是__重点__内容" / "这是*重点*内容".
    r = re.sub(r"\*\*(.+?)\*\*", r"\1", r)
    r = re.sub(r"(?<![A-Za-z0-9_])__(.+?)__(?![A-Za-z0-9_])", r"\1", r)
    # 4. italic ("_x_" keeps the \w guard so file names like 数据_2024_最终.xlsx survive)
    r = re.sub(r"(?<![A-Za-z0-9_*])\*(?![\s*])([^*\n]+?)(?<!\s)\*(?![A-Za-z0-9_*])", r"\1", r)
    r = re.sub(r"(?<![\w_])_(?![\s_])([^_\n]+?)(?<!\s)_(?![\w_])", r"\1", r)
    # 5. strikethrough
    r = re.sub(r"~~(.+?)~~", r"\1", r)
    # 6. inline code
    r = re.sub(r"`([^`\n]*)`", r"\1", r)
    # 7. headers
    r = re.sub(r"(?m)^[ \t]*#{1,6}[ \t]+(.*)$", r"\1", r)
    # 8. images / links (keep the URL so it stays clickable in QQ)
    r = re.sub(r"!\[([^\]]*)\]\(([^)\s]+)[^)]*\)", lambda m: (m.group(1) + " " if m.group(1) else "") + m.group(2), r)

    def _link(m: re.Match) -> str:
        label, url = m.group(1).strip(), m.group(2).strip()
        return url if not label or label == url else f"{label} {url}"
    r = re.sub(r"\[([^\]]*)\]\(([^)\s]+)[^)]*\)", _link, r)
    # 9. blockquotes
    r = re.sub(r"(?m)^[ \t]*>[ \t]?(.*)$", r"▎\1", r)
    # 10. table rows
    r = re.sub(r"(?m)^[ \t]*\|(.*)\|[ \t]*$",
               lambda m: "  ".join(c.strip() for c in m.group(1).split("|")).strip(), r)
    # 11. ordered lists "1. " -> "1、"
    r = re.sub(r"(?m)^([ \t]*)(\d+)\.[ \t]+", r"\1\2、", r)
    # 12. unordered lists
    r = re.sub(r"(?m)^([ \t]*)[-*+][ \t]+", r"\1• ", r)
    r = re.sub(r"\n{3,}", "\n\n", r)
    return r.strip()


def anti_risk(text: str) -> str:
    return re.sub(r"(?i)(https?://)", r"\1 ", text)


# --------------------------------------------------------------------------- CQ code fallback

_CQ_RE = re.compile(r"\[CQ:([A-Za-z0-9_-]+)((?:,[^\]]*)?)\]")


def _cq_unescape(value: str) -> str:
    return value.replace("&#44;", ",").replace("&#91;", "[").replace("&#93;", "]").replace("&amp;", "&")


def parse_cq_string(raw: str) -> list[dict]:
    segs: list[dict] = []
    pos = 0
    for m in _CQ_RE.finditer(raw or ""):
        if m.start() > pos:
            segs.append({"type": "text", "data": {"text": _cq_unescape(raw[pos:m.start()])}})
        data: dict[str, str] = {}
        for part in (m.group(2) or "").lstrip(",").split(","):
            if "=" in part:
                k, v = part.split("=", 1)
                data[k.strip()] = _cq_unescape(v)
        segs.append({"type": m.group(1).lower(), "data": data})
        pos = m.end()
    if pos < len(raw or ""):
        segs.append({"type": "text", "data": {"text": _cq_unescape(raw[pos:])}})
    return segs


def _as_segments(message: Any) -> list[dict]:
    if isinstance(message, list):
        return [s for s in message if isinstance(s, dict)]
    if isinstance(message, str):
        return parse_cq_string(message)
    return []


def _msg_segments(msg: dict) -> list[dict]:
    """Segments of a OneBot message object (``message``, falling back to ``raw_message``)."""
    return _as_segments(msg.get("message")) or _as_segments(msg.get("raw_message") or "")


def _node_segments(node: dict) -> list[dict]:
    """Segments of a merged-forward node (``message`` or ``content``, then ``raw_message``)."""
    content = node.get("message") if node.get("message") is not None else node.get("content")
    return _as_segments(content if content is not None else node.get("raw_message") or "")


def _message_id(data: Any) -> Optional[str]:
    return str(data["message_id"]) if isinstance(data, dict) and data.get("message_id") is not None else None


# --------------------------------------------------------------------------- OneBot connection

class OneBotError(Exception):
    pass


class OneBotTimeout(OneBotError):
    pass


class OneBotNotSent(ConnectionError):
    """The request never left this process (socket closed / not connected): safe to send again.
    A plain ``ConnectionError`` from ``call()`` means the socket died while waiting for the ack,
    so the request may already have been carried out."""


class OneBotConnection:
    """Forward-WebSocket OneBot v11 client: ``call()`` matches responses by ``echo``; events go to
    ``on_event`` (called synchronously from the reader, must not block)."""

    def __init__(self, url: str, token: str = "", on_event: Optional[Callable[[dict], None]] = None,
                 action_timeout: float = 30.0):
        self.url = url
        self.token = token
        self.on_event = on_event
        self.action_timeout = action_timeout
        self.self_id: Optional[str] = None
        self.closed = asyncio.Event()
        self._ws = None
        self._reader_task: Optional[asyncio.Task] = None
        self._pending: dict[str, asyncio.Future] = {}
        self._send_lock = asyncio.Lock()

    @property
    def is_open(self) -> bool:
        return self._ws is not None and not self.closed.is_set()

    async def open(self, timeout: float = 15.0) -> None:
        import websockets
        headers = {"Authorization": f"Bearer {self.token}"} if self.token else None
        self._ws = await asyncio.wait_for(
            websockets.connect(self.url, additional_headers=headers, max_size=None,
                               ping_interval=30, ping_timeout=60, open_timeout=timeout),
            timeout=timeout + 5)
        self.closed.clear()
        self._reader_task = asyncio.create_task(self._reader(), name="onebot-reader")

    async def _reader(self) -> None:
        try:
            async for raw in self._ws:
                try:
                    data = json.loads(raw)
                except (TypeError, ValueError):
                    continue
                if not isinstance(data, dict):
                    continue
                if "post_type" not in data and data.get("echo") is not None:
                    fut = self._pending.pop(str(data.get("echo")), None)
                    if fut is not None and not fut.done():
                        fut.set_result(data)
                    continue
                if data.get("post_type") == "meta_event" and data.get("self_id"):
                    self.self_id = str(data["self_id"])
                if data.get("post_type") and self.on_event is not None:
                    try:
                        self.on_event(data)
                    except Exception:
                        logger.exception("[OneBot] event handler raised")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("[OneBot] WebSocket reader stopped: %s", exc)
        finally:
            for fut in self._pending.values():
                if not fut.done():
                    fut.set_exception(ConnectionError("OneBot WebSocket closed"))
            self._pending.clear()
            self.closed.set()

    async def call(self, action: str, params: Optional[dict] = None, timeout: Optional[float] = None) -> Any:
        if not self.is_open:
            raise OneBotNotSent("OneBot WebSocket not open")
        echo = uuid.uuid4().hex
        fut = asyncio.get_running_loop().create_future()
        self._pending[echo] = fut
        try:
            try:
                async with self._send_lock:
                    await self._ws.send(json.dumps({"action": action, "params": params or {}, "echo": echo},
                                                   ensure_ascii=False))
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                raise OneBotNotSent(f"OneBot {action} not sent: {exc}") from None
            resp = await asyncio.wait_for(fut, timeout or self.action_timeout)
        except asyncio.TimeoutError:
            raise OneBotTimeout(f"OneBot {action} request timeout") from None
        except ConnectionError:
            raise
        except Exception as exc:
            if self.closed.is_set() or type(exc).__name__.startswith("ConnectionClosed"):
                raise ConnectionError(f"OneBot WebSocket closed: {exc}") from None
            raise
        finally:
            self._pending.pop(echo, None)
            if fut.done() and not fut.cancelled():
                fut.exception()  # mark retrieved
            else:
                fut.cancel()
        status = str(resp.get("status") or "").lower()
        retcode = resp.get("retcode")
        if status == "failed" or retcode not in (0, 1, None):
            detail = resp.get("wording") or resp.get("message") or resp.get("msg") or ""
            raise OneBotError(f"OneBot {action} failed (retcode={retcode}) {detail}".strip())
        return resp.get("data")

    async def close(self) -> None:
        ws, self._ws = self._ws, None
        if ws is not None:
            with contextlib.suppress(Exception):
                await asyncio.wait_for(ws.close(), timeout=5)
        if self._reader_task is not None:
            self._reader_task.cancel()
            with contextlib.suppress(BaseException):
                await self._reader_task
            self._reader_task = None
        self.closed.set()


# --------------------------------------------------------------------------- adapter

class _Media:
    """Inbound attachments collected for one MessageEvent."""

    def __init__(self) -> None:
        self.urls: list[str] = []
        self.types: list[str] = []
        self.inlined: list[Optional[bool]] = []
        self.notes: list[str] = []
        self.voice = False

    @property
    def image_count(self) -> int:
        return sum(t.startswith("image/") for t in self.types)

    def add(self, got: Optional[tuple[str, str]], note: Optional[str] = None, *,
            inlined: Optional[bool] = None) -> None:
        if got:
            self.urls.append(got[0])
            self.types.append(got[1])
            self.inlined.append(inlined)
        if note:
            self.notes.append(note)


class OneBotAdapter(BasePlatformAdapter):
    """QQ through NapCat's OneBot v11 forward WebSocket."""

    # send() chunks long text itself; without this the gateway cuts cron output at 4000 chars.
    splits_long_messages = True

    def __init__(self, config, **kwargs):
        super().__init__(config=config, platform=Platform(PLATFORM_NAME))
        extra = getattr(config, "extra", {}) or {}
        self.extra = extra
        self.ws_url = str(extra_or_secret(extra, "ws_url", "ONEBOT_WS_URL", "ws://127.0.0.1:3001")).strip()
        self.access_token = str(extra_or_secret(extra, "access_token", "ONEBOT_ACCESS_TOKEN", "") or "").strip()
        self.admins = set(_id_list(extra.get("admins")))
        self.admin_only_chat = _as_bool(extra.get("admin_only_chat"), False)
        self.require_mention = _as_bool(extra.get("require_mention"), True)
        self.mention_all_triggers = _as_bool(extra.get("mention_all_triggers"), True)
        self.allow_bare_group_commands = _as_bool(extra.get("allow_bare_group_commands"), True)
        self.keyword_triggers = _str_list(extra.get("keyword_triggers"))
        self.keyword_only_trigger = _as_bool(extra.get("keyword_only_trigger"), False)
        self.allowed_groups = set(_id_list(extra.get("allowed_groups")))
        self.blocked_users = set(_id_list(extra.get("blocked_users")))
        self.format_markdown = _as_bool(extra.get("format_markdown"), True)
        self.markdown_code_blocks = str(extra.get("markdown_code_blocks") or "placeholder").strip().lower()
        self.anti_risk_mode = _as_bool(extra.get("anti_risk_mode"), False)
        self.max_message_length = max(200, _as_int(extra.get("max_message_length"), 4000))
        self.rate_limit_s = max(0, _as_int(extra.get("rate_limit_ms"), 1000)) / 1000.0
        self.group_at_sender = _as_bool(extra.get("group_at_sender"), True)
        self.inbound_file_max_bytes = max(1, _as_int(extra.get("inbound_file_max_mb"), 2048)) * _MIB
        self.outbound_media_max_bytes = max(1, _as_int(extra.get("outbound_media_max_mb"), 1024)) * _MIB
        self.enrich_context = _as_bool(extra.get("enrich_reply_forward_context"), True)
        self.max_reply_layers = max(0, _as_int(extra.get("max_reply_layers"), 1))
        self.max_forward_layers = max(0, _as_int(extra.get("max_forward_layers"), 5))
        self.max_forward_messages = max(1, _as_int(extra.get("max_forward_messages_per_layer"), 8))
        self.max_chars_per_layer = max(100, _as_int(extra.get("max_chars_per_layer"), 900))
        self.max_total_context_chars = max(300, _as_int(extra.get("max_total_context_chars"), 3000))
        self.history_limit = max(0, _as_int(extra.get("history_limit"), 5))
        aliases = extra.get("command_aliases")
        self.command_aliases: dict[str, str] = (
            {str(k).lstrip("/").lower(): str(v).lstrip("/") for k, v in aliases.items()}
            if isinstance(aliases, dict) else {})
        # slow plugin commands run beside the session: {"name": "please-wait note" or ""}
        bg = extra.get("background_commands")
        self.command_progress: dict[str, str] = (
            {str(k).lstrip("/").lower(): str(v or "") for k, v in bg.items()} if isinstance(bg, dict) else {})
        # commands whose files go to the group file area instead of inline (prefix match)
        self.group_file_commands = [c.lstrip("/").lower() for c in _str_list(extra.get("group_file_commands"))]
        guest_deny = extra.get("guest_deny_toolsets")
        self.guest_deny_toolsets = set(_str_list(guest_deny) if guest_deny is not None
                                       else ["memory", "session_search", "connections"])
        self.restrict_guests = _as_bool(extra.get("restrict_guest_tools"), True)
        # NapCat may run inside WSL (Linux QQ): local paths must then be translated both ways.
        self.wsl_distro = str(extra.get("napcat_wsl_distro") or "").strip()

        self._conn: Optional[OneBotConnection] = None
        self._supervisor: Optional[asyncio.Task] = None
        self._stopping = False
        self._self_id: Optional[str] = None
        self._self_name: str = ""
        self._seen: OrderedDict[str, float] = OrderedDict()
        self._msg_sender: OrderedDict[str, str] = OrderedDict()
        self._group_names: dict[str, str] = {}
        self._tasks: set[asyncio.Task] = set()
        self._command_slots = asyncio.Semaphore(_MAX_BACKGROUND_COMMANDS)

    # ------------------------------------------------------------------ lifecycle

    @property
    def name(self) -> str:
        return "QQ"

    def _fail(self, code: str, message: str, *, retryable: bool) -> bool:
        self._set_fatal_error(code, message, retryable=retryable)
        return False

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        if not self.ws_url:
            return self._fail("config_missing", "OneBot ws_url is not configured", retryable=False)
        if not self._acquire_platform_lock("onebot", self.ws_url, f"OneBot endpoint {self.ws_url}"):
            return False
        self._stopping = False
        try:
            await self._open()
        except Exception as exc:
            logger.error("[OneBot] connect to %s failed: %s", self.ws_url, exc)
            with contextlib.suppress(Exception):
                self._release_platform_lock()
            return self._fail("connect_failed", f"OneBot connect failed: {exc}", retryable=True)
        self._supervisor = asyncio.create_task(self._supervise(), name="onebot-supervisor")
        self._mark_connected()
        logger.info("[OneBot] connected to %s as %s (%s)", self.ws_url, self._self_id, self._self_name)
        self._wire_plugin_handlers(None)
        return True

    async def _open(self) -> None:
        conn = OneBotConnection(self.ws_url, self.access_token, on_event=self._on_event)
        await conn.open()
        self._conn = conn
        try:
            info = await conn.call("get_login_info", timeout=15) or {}
            self._self_id = str(info.get("user_id") or conn.self_id or "") or None
            self._self_name = str(info.get("nickname") or "")
        except ConnectionError as exc:
            await conn.close()
            self._conn = None
            raise ConnectionError(f"{exc} (wrong ONEBOT_ACCESS_TOKEN?)") from None
        except Exception as exc:
            logger.warning("[OneBot] get_login_info failed: %s", exc)
            self._self_id = conn.self_id

    async def _supervise(self) -> None:
        attempt = 0
        while not self._stopping:
            conn = self._conn
            if conn is not None:
                await conn.closed.wait()
            if self._stopping:
                return
            self._mark_disconnected()
            delay = min(60, 2 ** min(attempt, 6))
            attempt += 1
            logger.warning("[OneBot] connection lost; reconnecting in %ss", delay)
            await asyncio.sleep(delay)
            if self._stopping:
                return
            try:
                await self._open()
                attempt = 0
                self._mark_connected()
                logger.info("[OneBot] reconnected to %s", self.ws_url)
            except Exception as exc:
                logger.warning("[OneBot] reconnect failed: %s", exc)
                self._conn = None
                await asyncio.sleep(1)

    async def disconnect(self) -> None:
        self._stopping = True
        if self._supervisor is not None:
            self._supervisor.cancel()
            with contextlib.suppress(BaseException):
                await self._supervisor
            self._supervisor = None
        for task in list(self._tasks):
            task.cancel()
        if self._conn is not None:
            await self._conn.close()
            self._conn = None
        with contextlib.suppress(Exception):
            self._release_platform_lock()
        self._mark_disconnected()

    async def _call(self, action: str, params: Optional[dict] = None, timeout: Optional[float] = None) -> Any:
        conn = self._conn
        if conn is None or not conn.is_open:
            raise OneBotNotSent("OneBot not connected")
        return await conn.call(action, params, timeout)

    async def _wait_connected(self, timeout: float) -> bool:
        """Wait (bounded) for the supervisor to bring the socket back."""
        deadline = time.monotonic() + timeout
        while not self._stopping:
            conn = self._conn
            if conn is not None and conn.is_open:
                return True
            if time.monotonic() >= deadline:
                return False
            await asyncio.sleep(0.5)
        return False

    async def _safe_call(self, action: str, params: Optional[dict] = None, timeout: Optional[float] = None) -> Any:
        try:
            return await self._call(action, params, timeout)
        except Exception as exc:
            logger.debug("[OneBot] %s failed: %s", action, exc)
            return None

    # ------------------------------------------------------------------ access / tools

    def is_admin(self, user_id: Any) -> bool:
        return str(user_id or "") in self.admins

    def _is_known_command(self, text: str) -> bool:
        """``text`` starts with a slash command Hermes would run (built-in, alias, plugin, our aliases)."""
        m = _BARE_CMD_RE.match(text)
        if not m:
            return False
        name = m.group(1).lower()
        if name in self.command_aliases:
            return True
        try:
            from hermes_cli.commands import is_gateway_known_command, resolve_command
            name = name.replace("_", "-")
            return resolve_command(name) is not None or is_gateway_known_command(name)
        except Exception:
            return False

    def toolsets_for_source(self, source) -> Optional[list[str]]:
        """Non-admins get the normal QQ toolsets minus the ones that reach the owner's private data
        (memory, other sessions, connected accounts) - same policy as OpenClaw's nonAdminToolPolicy.
        Admins keep the platform toolsets untouched."""
        if not self.restrict_guests or not self.guest_deny_toolsets:
            return None
        if self.is_admin(getattr(source, "user_id", None)):
            return None
        try:
            from hermes_cli.config import load_config_readonly
            from hermes_cli.tools_config import _get_platform_tools
            enabled = _get_platform_tools(load_config_readonly(), PLATFORM_NAME)
        except Exception:
            logger.exception("[OneBot] could not resolve guest toolsets; denying tools")
            return ["no_mcp"]
        kept = sorted(ts for ts in enabled if ts not in self.guest_deny_toolsets)
        return kept or ["no_mcp"]

    # ------------------------------------------------------------------ inbound

    def _on_event(self, event: dict) -> None:
        if event.get("post_type") == "meta_event":
            if event.get("self_id") and not self._self_id:
                self._self_id = str(event["self_id"])
            return
        task = asyncio.create_task(self._handle_event(event))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    def _remember(self, store: OrderedDict, key: str, value: Any, cap: int = 4000) -> None:
        store[key] = value
        store.move_to_end(key)
        while len(store) > cap:
            store.popitem(last=False)

    async def _handle_event(self, ev: dict) -> None:
        try:
            await self._handle_event_inner(ev)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("[OneBot] failed to handle inbound event")

    async def _handle_event_inner(self, ev: dict) -> None:
        self_id = self._self_id or (str(ev.get("self_id")) if ev.get("self_id") else "")
        post_type = ev.get("post_type")
        is_poke = False
        if post_type == "notice":
            if not (ev.get("notice_type") == "notify" and ev.get("sub_type") == "poke"):
                return
            if not self_id or str(ev.get("target_id")) != self_id or str(ev.get("user_id")) == self_id:
                return
            is_poke = True
            ev = dict(ev)
            ev["message_type"] = "group" if ev.get("group_id") else "private"
            ev["message"] = [{"type": "text", "data": {"text": "[动作] 用户戳了你一下"}}]
            ev["message_id"] = f"poke-{ev.get('user_id')}-{ev.get('time') or time.time()}"
        elif post_type != "message":
            return

        message_type = ev.get("message_type")
        if message_type not in ("private", "group"):
            return
        user_id = str(ev.get("user_id") or "")
        if not user_id or (self_id and user_id == self_id):
            return
        msg_id = str(ev.get("message_id") or "")
        if msg_id:
            key = f"{message_type}:{ev.get('group_id') or ''}:{msg_id}"
            if key in self._seen:
                return
            self._remember(self._seen, key, time.time())
        is_group = message_type == "group"
        group_id = str(ev.get("group_id") or "")
        if is_group and self.allowed_groups and group_id not in self.allowed_groups:
            return
        if user_id in self.blocked_users:
            return
        is_admin = self.is_admin(user_id)

        segments = _msg_segments(ev)
        parsed = self._parse_segments(segments, self_id)
        text = parsed["text"].strip()
        text = re.sub(r"^[／/]", "/", text)

        # ---- trigger decision (same order as OpenClaw)
        triggered = (not is_group) or is_poke
        if is_group and not triggered:
            keyword_hit = any(kw in text for kw in self.keyword_triggers)
            if self.keyword_only_trigger:
                triggered = keyword_hit
            else:
                # Bare "/cmd" without an @: OpenClaw let every member's through. Admins may send
                # anything; members only commands Hermes knows, so stray "/..." text stays quiet.
                # Who may run which command is still the gateway's (group_)user_allowed_commands.
                triggered = (keyword_hit or not self.require_mention or parsed["mentioned"]
                             or (text.startswith("/") and self.allow_bare_group_commands
                                 and (is_admin or self._is_known_command(text))))
        replied = None
        if parsed["reply_id"] and (triggered or (is_group and not self.keyword_only_trigger)):
            replied = await self._safe_call("get_msg", {"message_id": _maybe_int(parsed["reply_id"])})
        if is_group and not triggered and isinstance(replied, dict):
            r_sender = str((replied.get("sender") or {}).get("user_id") or replied.get("user_id") or "")
            triggered = bool(self_id) and r_sender == self_id
        if not triggered:
            return
        if self.admin_only_chat and not is_admin:
            logger.info("[OneBot] ignored non-admin %s (admin_only_chat)", user_id)
            return

        # ---- command_aliases: bare /alias -> /real-command
        m = re.match(r"^/([A-Za-z0-9_-]+)(.*)$", text, re.S)
        if m and m.group(1).lower() in self.command_aliases:
            text = f"/{self.command_aliases[m.group(1).lower()]}{m.group(2)}"

        sender = ev.get("sender") or {}
        user_name = _labelled_sender(user_id, sender.get("card") or sender.get("nickname"))
        chat_id = f"g{group_id}" if is_group else user_id
        # Group label carries the verified group number so the model can tell groups apart / target them.
        chat_name = (f"QQ群 {group_id}｜{await self._group_name(group_id)}") if is_group else user_name
        if msg_id:
            self._remember(self._msg_sender, msg_id, user_id)
        source = self.build_source(
            chat_id=chat_id, chat_name=chat_name, chat_type="group" if is_group else "dm",
            user_id=user_id, user_name=user_name, message_id=msg_id or None)

        # ---- slow direct commands (``background_commands``) run beside the session, not in it
        m = re.match(r"^/([A-Za-z0-9_-]+)(.*)$", text, re.S)
        if m and self._start_background_command(m.group(1).lower().replace("_", "-"), m.group(2).strip(), source):
            return

        # ---- media: the message's own, then the quoted message's, then images inside forwards
        budget = {"lookup": _LOOKUP_BUDGET}
        media = _Media()
        ctx_ids = {"group_id": _maybe_int(group_id) if is_group else None, "user_id": _maybe_int(user_id)}
        await self._collect_media(parsed, ctx_ids, media)

        # ---- reply + forward context
        reply_kwargs: dict[str, Any] = {}
        if isinstance(replied, dict):
            r_sender = replied.get("sender") or {}
            r_uid = str(r_sender.get("user_id") or replied.get("user_id") or "")
            r_segments = _msg_segments(replied)
            r_sum = self._summarize(r_segments, 4000)
            r_own = bool(self_id) and r_uid == self_id
            r_name = _labelled_sender(r_uid, r_sender.get("card") or r_sender.get("nickname")) if r_uid else None
            r_text = r_sum["text"] or "(空文本)"
            reply_kwargs = {
                "reply_to_message_id": str(parsed["reply_id"]),
                # Hermes renders only the quoted text (not reply_to_author_name), so the author rides in it.
                "reply_to_text": f"{r_name}: {r_text}" if r_name and not r_own else r_text,
                "reply_to_author_id": r_uid or None,
                "reply_to_author_name": r_name,
                "reply_to_is_own_message": r_own,
            }
            # A QQ file message cannot carry an @, so "reply to the file + @bot" is how a group hands
            # the bot a file (or a video / voice message): fetch those like the message's own.
            r_ctx = {"group_id": ctx_ids["group_id"], "user_id": _maybe_int(r_uid) if r_uid else ctx_ids["user_id"]}
            await self._collect_media(r_sum, r_ctx, media, quoted=True)
        history_block = await self._group_history(group_id, msg_id, ev.get("time")) if is_group else ""
        layers_block = ""
        if self.enrich_context and (parsed["forwards"] or replied):
            layers_block, layer_images = await self._build_layers(parsed["forwards"], replied, budget)
            await self._collect_media({"images": layer_images}, ctx_ids, media)
        channel_context = "\n\n".join(b for b in (history_block, layers_block) if b) or None

        if media.notes:
            text = (text + "\n" + "\n".join(media.notes)).strip()
        if not text and not media.urls:
            if parsed["reply_id"]:
                text = "[回复消息]"
            elif parsed["forwards"]:
                text = "[转发消息]"
            elif parsed["other_types"]:
                text = f"[QQ消息:{','.join(sorted(set(parsed['other_types'])))}]"
            elif parsed["mentioned"]:
                text = "[动作] 用户@了你一下，没有说别的"
            else:
                return

        if media.voice:
            message_type_enum = MessageType.VOICE
        elif media.image_count:
            message_type_enum = MessageType.PHOTO
        elif any(t.startswith("video/") for t in media.types):
            message_type_enum = MessageType.VIDEO
        elif media.urls:
            message_type_enum = MessageType.DOCUMENT
        else:
            message_type_enum = MessageType.TEXT

        ts = ev.get("time")
        event = MessageEvent(
            text=text, message_type=message_type_enum, source=source, raw_message=ev,
            message_id=msg_id or None, media_urls=media.urls, media_types=media.types,
            media_text_inlined=media.inlined, channel_context=channel_context,
            timestamp=datetime.datetime.fromtimestamp(ts) if isinstance(ts, (int, float)) else datetime.datetime.now(),
            **reply_kwargs)
        logger.info("[OneBot] inbound %s user=%s admin=%s media=%d text=%r", chat_id, user_id, is_admin,
                    len(media.urls), text[:80])
        await self.handle_message(event)

    async def _collect_media(self, items: dict, ctx: dict, media: _Media, *, quoted: bool = False) -> None:
        """Download the images / voice / videos / files listed in ``items`` (``_parse_segments`` or
        ``_summarize`` output) into Hermes' caches and add them to ``media``."""
        for d in items.get("images") or []:
            if media.image_count >= _MAX_INBOUND_IMAGES:
                break
            media.add(await self._resolve_image(d))
        for d in items.get("records") or []:
            got = await self._resolve_record(d)
            media.add(got, None if got else ("[引用的语音：下载失败]" if quoted else "[语音：下载失败]"))
            media.voice = media.voice or bool(got)
        for d in items.get("videos") or []:
            media.add(*await self._resolve_video(d))
        for d in items.get("files") or []:
            got, note = await self._resolve_file(d, ctx)
            media.add(got, note, inlined=False)

    def _parse_segments(self, segments: list[dict], self_id: str) -> dict:
        texts: list[str] = []
        out = {"mentioned": False, "reply_id": None, "forwards": [], "images": [], "records": [],
               "videos": [], "files": [], "other_types": []}
        for seg in segments:
            t = str(seg.get("type") or "").lower()
            d = seg.get("data") or {}
            if t == "text":
                texts.append(str(d.get("text") or ""))
            elif t == "at":
                qq = str(d.get("qq") or "")
                if (self_id and qq == self_id) or (qq == "all" and self.mention_all_triggers):
                    out["mentioned"] = True
                    if qq == "all":
                        texts.append("@全体成员 ")
                else:
                    texts.append(f"@{d.get('name') or qq} ")
            elif t == "face":
                texts.append("[表情]")
            elif t in ("mface", "marketface"):
                texts.append(f"[{d.get('summary') or '表情'}]".replace("[[", "[").replace("]]", "]"))
            elif t == "image":
                out["images"].append(d)
            elif t == "record":
                out["records"].append(d)
            elif t == "video":
                out["videos"].append(d)
            elif t in ("file", "onlinefile"):
                out["files"].append(d)
            elif t == "reply":
                rid = d.get("id") or d.get("message_id")
                if rid is not None:
                    out["reply_id"] = str(rid)
            elif t in ("forward", "forward_msg", "nodes"):
                fid = d.get("id") or d.get("message_id") or d.get("forward_id")
                if fid:
                    out["forwards"].append(str(fid))
                texts.append("[合并转发消息]")
            elif t == "json":
                texts.append(_json_card_text(d.get("data")))
            elif t == "markdown":
                texts.append(str(d.get("content") or ""))
            elif t in ("dice", "rps"):
                texts.append(f"[{'骰子' if t == 'dice' else '猜拳'}:{d.get('result', '?')}]")
            elif t == "location":
                texts.append(f"[位置:{d.get('title') or ''} {d.get('lat', '')},{d.get('lon', '')}]")
            elif t == "contact":
                texts.append(f"[名片:{d.get('type', '')} {d.get('id', '')}]")
            elif t in ("poke", "shake"):
                texts.append("[戳一戳]")
            elif t:
                out["other_types"].append(t)
        out["text"] = "".join(texts)
        return out

    def _summarize(self, segments: list[dict], max_chars: int) -> dict:
        """Text outline of a referenced message (OpenClaw summarizeOneBotSegments)."""
        parts: list[str] = []
        images: list[dict] = []
        records: list[dict] = []
        videos: list[dict] = []
        files: list[dict] = []
        forwards: list[str] = []
        reply_id = None
        for seg in segments:
            t = str(seg.get("type") or "").lower()
            d = seg.get("data") or {}
            if t == "text":
                parts.append(str(d.get("text") or ""))
            elif t == "at":
                parts.append(f" @{d.get('name') or d.get('qq') or 'unknown'} ")
            elif t == "image":
                parts.append(" [图片]")
                if len(images) < _MAX_INBOUND_IMAGES:
                    images.append(d)
            elif t == "record":
                parts.append(f" [语音{':' + str(d['text']) if d.get('text') else ''}]")
                records.append(d)
            elif t == "video":
                parts.append(" [视频]")
                videos.append(d)
            elif t == "face":
                parts.append("[表情]")
            elif t in ("mface", "marketface"):
                parts.append(f"[{d.get('summary') or '表情'}]")
            elif t == "json":
                parts.append(" " + _json_card_text(d.get("data")))
            elif t in ("file", "onlinefile"):
                parts.append(f" [文件:{d.get('name') or d.get('file') or '未命名'}]")
                files.append(d)
            elif t in ("forward", "forward_msg", "nodes"):
                fid = d.get("id") or d.get("message_id")
                if fid:
                    forwards.append(str(fid))
                parts.append(" [合并转发]")
                content = d.get("content")
                if isinstance(content, list) and not fid:
                    for node in content[: self.max_forward_messages]:
                        parts.append(" " + self._node_outline(node, 200))
            elif t == "reply":
                rid = d.get("id")
                if rid:
                    reply_id = str(rid)
        text = re.sub(r"[ \t]+", " ", "".join(parts)).strip()
        if len(text) > max_chars:
            text = text[: max_chars - 1] + "…"
        return {"text": text, "images": images, "records": records, "videos": videos, "files": files,
                "forwards": forwards, "reply_id": reply_id}

    def _node_outline(self, node: Any, max_chars: int) -> str:
        if not isinstance(node, dict):
            return ""
        sender = node.get("sender") or {}
        name = sender.get("card") or sender.get("nickname") or node.get("user_id") or "unknown"
        return f"{name}: {self._summarize(_node_segments(node), max_chars)['text']}"

    async def _build_layers(self, root_forwards: list[str], replied: Optional[dict], budget: dict) -> tuple[str, list[dict]]:
        lines: list[str] = []
        images: list[dict] = []
        used = 0

        def push(line: str) -> None:
            nonlocal used
            if not line or used >= self.max_total_context_chars:
                return
            remain = self.max_total_context_chars - used
            if len(line) > remain:
                line = line[: max(0, remain - 1)] + "…"
            lines.append(line)
            used += len(line) + 1

        def take(n: int = 1) -> bool:
            if budget["lookup"] < n:
                return False
            budget["lookup"] -= n
            return True

        queue: deque = deque()
        seen: set[str] = set()

        def enqueue(ids: list[str], depth: int, tag: str) -> None:
            for fid in ids:
                if fid and fid not in seen and depth <= self.max_forward_layers:
                    seen.add(fid)
                    queue.append((fid, depth, tag))

        enqueue(root_forwards, 1, "forward")
        # reply chain beyond the direct reply (the direct reply is passed as reply_to_text)
        cursor = replied
        for layer in range(1, self.max_reply_layers + 1):
            if not isinstance(cursor, dict):
                break
            segs = _msg_segments(cursor)
            summary = self._summarize(segs, self.max_chars_per_layer)
            enqueue(summary["forwards"], 1, "forward-in-reply")
            if layer > 1:
                sender = cursor.get("sender") or {}
                name = _labelled_sender(sender.get("user_id"), sender.get("card") or sender.get("nickname"))
                push(f"[Layer {layer}][reply][from:{name}] {summary['text'] or '(空文本)'}")
                images.extend(summary["images"])
            if layer >= self.max_reply_layers or not summary["reply_id"] or not take():
                break
            cursor = await self._safe_call("get_msg", {"message_id": _maybe_int(summary["reply_id"])})

        while queue and budget["lookup"] > 0:
            fid, depth, tag = queue.popleft()
            if not take():
                break
            data = await self._safe_call("get_forward_msg", {"id": fid, "message_id": fid})
            if not isinstance(data, dict):
                continue
            nodes = data.get("messages") or data.get("message") or data.get("nodes") or []
            if not isinstance(nodes, list):
                continue
            for idx, node in enumerate(nodes[: self.max_forward_messages], 1):
                if not isinstance(node, dict):
                    continue
                sender = node.get("sender") or {}
                name = _labelled_sender(sender.get("user_id") or node.get("user_id"),
                                        sender.get("card") or sender.get("nickname"))
                summary = self._summarize(_node_segments(node), self.max_chars_per_layer)
                push(f"[Layer F{depth}.{idx}][{tag}][from:{name}] {summary['text'] or '(空文本)'}")
                images.extend(summary["images"])
                if depth < self.max_forward_layers:
                    enqueue(summary["forwards"], depth + 1, "forward-nested")
        if not lines:
            return "", images[:_MAX_INBOUND_IMAGES]
        block = "<context_layers>\n" + "\n".join(lines) + "\n</context_layers>"
        return block, images[:_MAX_INBOUND_IMAGES]

    async def _group_name(self, group_id: str) -> str:
        if group_id in self._group_names:
            return self._group_names[group_id]
        info = await self._safe_call("get_group_info", {"group_id": _maybe_int(group_id)}, timeout=10)
        raw = str((info or {}).get("group_name") or "")
        name = re.sub(r"\s+", " ", re.sub(r"[\x00-\x1f\[\]｜|]", " ", raw)).strip()[:60] or "未命名群"
        if info:  # don't cache a failed lookup
            self._group_names[group_id] = name
        return name

    async def _group_history(self, group_id: str, current_msg_id: str, current_time: Any) -> str:
        """The ``history_limit`` group messages before the trigger as ``<history>`` (OpenClaw's
        historyLimit, default 5): without it "@bot 你怎么看上面" has nothing to look at."""
        if not self.history_limit:
            return ""
        data = await self._safe_call("get_group_msg_history",
                                     {"group_id": _maybe_int(group_id), "count": self.history_limit + 5}, timeout=10)
        rows = data.get("messages") if isinstance(data, dict) else data
        if not isinstance(rows, list):
            return ""
        rows = [r for r in rows if isinstance(r, dict)]
        # NapCat's real_seq counts up; message_seq is id-like (not ordered). Stable sort keeps ties.
        rows.sort(key=lambda r: (_as_int(r.get("time"), 0), _as_int(r.get("real_seq"), 0)))
        cut = next((i for i, r in enumerate(rows) if current_msg_id and str(r.get("message_id")) == current_msg_id), None)
        if cut is not None:
            rows = rows[:cut]
        elif isinstance(current_time, (int, float)):
            rows = [r for r in rows if _as_int(r.get("time"), 0) <= current_time]
        lines: list[str] = []
        for r in rows[-self.history_limit:]:
            sender = r.get("sender") or {}
            uid = str(sender.get("user_id") or r.get("user_id") or "")
            name = ("（你自己）" if uid and uid == self._self_id
                    else _labelled_sender(uid, sender.get("card") or sender.get("nickname")))
            segs = _msg_segments(r)
            lines.append(f"{name}: {self._summarize(segs, self.max_chars_per_layer)['text'] or '(空文本)'}")
        return "<history>\n" + "\n".join(lines) + "\n</history>" if lines else ""

    # ------------------------------------------------------------------ slow direct commands

    def _start_background_command(self, cmd: str, args: str, source) -> bool:
        """Run a slow direct command (a ``background_commands`` key) in its own
        task. Through the gateway it runs inside the session, and a group shares one session, so a
        30-minute download parked everyone's messages behind it. False leaves the message to the
        gateway (no such plugin command, or this sender may not run it: the gateway then answers
        with its usual notice, and no "please wait" note goes out first)."""
        note = self.command_progress.get(cmd)
        if note is None:
            return False
        try:
            from gateway.slash_access import policy_from_extra
            from hermes_cli.plugins import get_plugin_command_handler
            handler = get_plugin_command_handler(cmd)
            scope = "dm" if source.chat_type == "dm" else "group"
            allowed = policy_from_extra(self.extra, scope).can_run(source.user_id, cmd)
        except Exception:
            logger.exception("[OneBot] could not resolve /%s; leaving it to the gateway", cmd)
            return False
        if handler is None or not allowed:
            return False
        is_help = not args or bool(re.fullmatch(r"(?:help|--help|-h|帮助)", args, re.I))
        task = asyncio.create_task(self._run_background_command(handler, cmd, args, source, None if is_help else note),
                                   name=f"onebot-cmd-{cmd}")
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return True

    async def _run_background_command(self, handler: Callable, cmd: str, args: str, source,
                                      note: Optional[str]) -> None:
        chat_id, reply_to = source.chat_id, source.message_id
        logger.info("[OneBot] /%s from %s in %s runs beside the session", cmd, source.user_id, chat_id)
        if note:
            await self._safe_send_note(chat_id, note)
        async with self._command_slots:
            tokens = None
            try:
                from gateway.session_context import set_session_vars
                # Same session key the gateway would bind, so paging commands keep their state.
                tokens = set_session_vars(
                    platform=PLATFORM_NAME, chat_id=chat_id, chat_type=source.chat_type,
                    chat_name=source.chat_name or "", user_id=source.user_id or "",
                    user_name=source.user_name or "", session_key=self._source_session_key(source),
                    message_id=reply_to or "")
                if asyncio.iscoroutinefunction(handler):
                    result = await handler(args)
                else:
                    result = await asyncio.to_thread(handler, args)
                    if asyncio.iscoroutine(result):
                        result = await result
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("[OneBot] /%s failed", cmd)
                result = f"/{cmd} 执行出错，请稍后重试。"
            finally:
                if tokens is not None:
                    from gateway.session_context import clear_session_vars
                    clear_session_vars(tokens)
        await self._deliver_command_result(chat_id, str(result or ""), reply_to, cmd=cmd)

    async def _deliver_command_result(self, chat_id: str, result: str, reply_to: Optional[str],
                                      cmd: str = "") -> None:
        """Send a command reply the way the gateway would: the text, then each ``MEDIA:<path>`` line
        as a native image / voice / video / file."""
        text_lines: list[str] = []
        files: list[str] = []
        for line in result.splitlines():
            m = _MEDIA_LINE_RE.match(line)
            if m:
                files.append(m.group(1))
            else:
                text_lines.append(line)
        text = "\n".join(text_lines).strip()
        if text:
            res = await self.send(chat_id, text, reply_to=reply_to)
            if not res.success and res.retryable and await self._wait_connected(_RESUME_WAIT_S):
                await self.send(chat_id, text, reply_to=reply_to)
        try:
            # ``group_file_commands`` files go to the group file area; the rest are sent inline
            in_group = parse_chat_id(chat_id)[0] == "group" and any(cmd.startswith(c) for c in self.group_file_commands)
        except ValueError:
            in_group = False
        for path in files:
            if in_group:
                res = await self._upload_group_command_file(chat_id, path)
            else:
                res = await self._send_local_file(chat_id, path)
            if not res.success:
                logger.warning("[OneBot] command file %s not delivered: %s", os.path.basename(path), res.error)
                await self.send(chat_id, f"文件发送失败：{os.path.basename(path)}", reply_to=reply_to)

    async def _upload_group_command_file(self, chat_id: str, path: str) -> SendResult:
        """Command files in groups always go to the group file area (images too). QQ often rejects
        a group upload because of words in the file name (retcode 1200 "rich media transfer
        failed"), so one retry uses a neutral name like ``abc123456.pdf``."""
        res = await self.send_document(chat_id, path)
        if res.success or res.retryable or not os.path.isfile(path):
            return res
        base = os.path.basename(path)
        ext = os.path.splitext(base)[1].lower()
        m = re.match(r"^([A-Za-z]+_?\d+)", base)
        neutral = (m.group(1) if m else f"file_{int(time.time())}") + ext
        if neutral == base:
            return res
        logger.warning("[OneBot] group upload of %s failed (%s); retrying as %s", base, res.error, neutral)
        res2 = await self.send_document(chat_id, path, file_name=neutral)
        return res2 if res2.success else SendResult(success=False, error=f"{res.error}; neutral name: {res2.error}")

    # ------------------------------------------------------------------ inbound media helpers

    async def _url_is_safe(self, url: str) -> bool:
        try:
            from tools.url_safety import is_safe_url
        except Exception:
            return True
        return await asyncio.to_thread(is_safe_url, url)

    async def _http_get(self, url: str, max_bytes: int, write: Callable[[bytes], Any], timeout: float) -> bool:
        """Stream ``url`` into ``write``; False for non-http / unsafe URLs, raises on errors or > max_bytes."""
        import httpx
        if not re.match(r"^https?://", url or "", re.I) or not await self._url_is_safe(url):
            return False
        async with httpx.AsyncClient(timeout=httpx.Timeout(timeout, connect=15.0), follow_redirects=True) as client:
            async with client.stream("GET", url, headers={"User-Agent": _USER_AGENT}) as resp:
                resp.raise_for_status()
                total = 0
                async for chunk in resp.aiter_bytes(256 * 1024):
                    total += len(chunk)
                    if total > max_bytes:
                        raise ValueError(f"payload over {max_bytes} bytes")
                    write(chunk)
        return True

    async def _fetch_bytes(self, url: str, max_bytes: int) -> Optional[bytes]:
        buf = bytearray()
        try:
            return bytes(buf) if await self._http_get(url, max_bytes, buf.extend, 60.0) else None
        except Exception as exc:
            logger.debug("[OneBot] download failed %s: %s", url[:80], exc)
            return None

    async def _download_to(self, url: str, dest: Path, max_bytes: int) -> bool:
        """Stream a (possibly huge) attachment straight to disk."""
        tmp = dest.with_name(dest.name + ".part")
        try:
            with open(tmp, "wb") as fh:
                ok = await self._http_get(url, max_bytes, fh.write, 120.0)
            if ok:
                os.replace(tmp, dest)
                return True
        except Exception as exc:
            logger.warning("[OneBot] attachment download failed: %s", exc)
        with contextlib.suppress(OSError):
            tmp.unlink()
        return False

    def _to_napcat_path(self, local: str) -> str:
        """Host path -> path NapCat can open (WSL: C:\\x -> /mnt/c/x, \\\\wsl.localhost\\D\\x -> /x)."""
        p = str(Path(local).resolve())
        if not self.wsl_distro:
            return p if os.name == "nt" else Path(p).as_uri()
        m = re.match(r"^\\\\wsl(?:\.localhost|\$)\\[^\\]+(\\.*)$", p, re.I)
        if m:
            return m.group(1).replace("\\", "/")
        m = re.match(r"^([A-Za-z]):[\\/](.*)$", p)
        if m:
            return f"/mnt/{m.group(1).lower()}/" + m.group(2).replace("\\", "/")
        return p.replace("\\", "/")

    def _from_napcat_path(self, path: str) -> str:
        """Path reported by NapCat -> host path (WSL: /mnt/c/x -> C:\\x, /root/x -> \\\\wsl.localhost\\D\\root\\x)."""
        if not self.wsl_distro or os.name != "nt" or not path.startswith("/"):
            return path
        m = re.match(r"^/mnt/([a-zA-Z])/(.*)$", path)
        if m:
            return f"{m.group(1).upper()}:\\" + m.group(2).replace("/", "\\")
        return f"\\\\wsl.localhost\\{self.wsl_distro}" + path.replace("/", "\\")

    def _existing_local(self, path: Any) -> Optional[str]:
        if not isinstance(path, str) or not path.strip():
            return None
        s = path.strip()
        local = _local_path_from_ref(s) if s.lower().startswith("file:") else s
        if local and local.replace("\\", "/").startswith("/"):
            local = self._from_napcat_path(local.replace("\\", "/"))
        try:
            return local if local and os.path.isfile(local) else None
        except (OSError, ValueError):
            return None

    async def _pull_from_wsl(self, napcat_path: Any, dest: Path, max_bytes: int) -> bool:
        """NapCat (in WSL) reports a file under its own filesystem (/root/..., not readable from
        Windows: /root is 0700 and \\\\wsl.localhost is invisible to this packaged app). Copy it out
        through ``wsl.exe cat`` so large inbound files still arrive without base64 round-trips."""
        if not self.wsl_distro or os.name != "nt" or not isinstance(napcat_path, str):
            return False
        p = napcat_path.strip()
        if p.lower().startswith("file://"):
            p = unquote(p[7:])
        if not p.startswith("/") or p.startswith("/mnt/"):
            return False
        tmp = dest.with_name(dest.name + ".part")
        try:
            proc = await asyncio.create_subprocess_exec(
                "wsl.exe", "-d", self.wsl_distro, "-u", "root", "--", "cat", "--", p,
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
            total = 0
            with open(tmp, "wb") as fh:
                while True:
                    chunk = await proc.stdout.read(1024 * 1024)
                    if not chunk:
                        break
                    total += len(chunk)
                    if total > max_bytes:
                        proc.kill()
                        raise ValueError(f"payload over {max_bytes} bytes")
                    fh.write(chunk)
            rc = await proc.wait()
            if rc != 0 or total == 0:
                raise OSError(f"wsl cat exit {rc}, {total} bytes")
            os.replace(tmp, dest)
            return True
        except Exception as exc:
            logger.warning("[OneBot] copy from WSL failed for %s: %s", p, exc)
            with contextlib.suppress(OSError):
                tmp.unlink()
            return False

    async def _local_or_wsl_bytes(self, path: Any, max_bytes: int) -> Optional[bytes]:
        local = self._existing_local(path)
        if local:
            return await asyncio.to_thread(Path(local).read_bytes)
        if not self.wsl_distro or not isinstance(path, str):
            return None
        tmp = get_document_cache_dir() / f"wsl_{uuid.uuid4().hex[:12]}.bin"
        if await self._pull_from_wsl(path, tmp, max_bytes):
            try:
                return await asyncio.to_thread(tmp.read_bytes)
            finally:
                with contextlib.suppress(OSError):
                    tmp.unlink()
        return None

    async def _resolve_image(self, d: dict) -> Optional[tuple[str, str]]:
        data: Optional[bytes] = None
        name = str(d.get("file") or d.get("name") or "")
        url = _http_ref(d)
        if url:
            data = await self._fetch_bytes(str(url), _IMAGE_MAX_BYTES)
        if data is None:
            local = self._existing_local(d.get("path")) or self._existing_local(d.get("file"))
            if local:
                data = await asyncio.to_thread(Path(local).read_bytes)
        if data is None and d.get("file"):
            info = await self._safe_call("get_image", {"file": d.get("file")}, timeout=30) or {}
            data = await self._local_or_wsl_bytes(info.get("file"), _IMAGE_MAX_BYTES)
            if data is None and info.get("base64"):
                with contextlib.suppress(Exception):
                    data = base64.b64decode(info["base64"])
            if data is None and info.get("url"):
                data = await self._fetch_bytes(str(info["url"]), _IMAGE_MAX_BYTES)
        if not data:
            return None
        ext = _guess_image_ext(data, name)
        try:
            path = await asyncio.to_thread(cache_image_from_bytes, data, ext)
        except ValueError as exc:
            logger.debug("[OneBot] not caching image: %s", exc)
            return None
        return path, mimetypes.guess_type(path)[0] or "image/jpeg"

    async def _resolve_record(self, d: dict) -> Optional[tuple[str, str]]:
        data: Optional[bytes] = None
        ext = ".mp3"
        if d.get("file") or d.get("file_id"):
            params = {"out_format": "mp3"}
            if d.get("file"):
                params["file"] = d["file"]
            if d.get("file_id"):
                params["file_id"] = d["file_id"]
            info = await self._safe_call("get_record", params, timeout=60) or {}
            data = await self._local_or_wsl_bytes(info.get("file"), _AUDIO_MAX_BYTES)
            if data is not None:
                ext = os.path.splitext(str(info.get("file")))[1].lower() or ".mp3"
            elif info.get("base64"):
                with contextlib.suppress(Exception):
                    data = base64.b64decode(info["base64"])
        if data is None:
            url = _http_ref(d)
            if url:
                data = await self._fetch_bytes(str(url), _AUDIO_MAX_BYTES)
                ext = os.path.splitext(urlparse(str(url)).path)[1].lower() or ".amr"
        if data is None:
            local = self._existing_local(d.get("path"))
            if local:
                data = await asyncio.to_thread(Path(local).read_bytes)
                ext = os.path.splitext(local)[1].lower() or ".amr"
        if not data:
            return None
        try:
            path = await asyncio.to_thread(cache_audio_from_bytes, data, ext)
        except ValueError as exc:
            logger.warning("[OneBot] not caching voice: %s", exc)
            return None
        mime = {".mp3": "audio/mpeg", ".amr": "audio/amr", ".silk": "audio/silk", ".wav": "audio/wav",
                ".ogg": "audio/ogg", ".m4a": "audio/mp4"}.get(ext, "audio/mpeg")
        return path, mime

    async def _resolve_video(self, d: dict) -> tuple[Optional[tuple[str, str]], Optional[str]]:
        name = _safe_file_name(str(d.get("name") or d.get("file") or "video.mp4"), "video.mp4")
        if not os.path.splitext(name)[1]:
            name += ".mp4"
        size = _maybe_int(d.get("file_size"))
        if isinstance(size, int) and size > self.inbound_file_max_bytes:
            return None, f"[视频 {name}：{size // _MIB}MB，超过入站上限，未下载]"
        dest = get_video_cache_dir() / f"video_{uuid.uuid4().hex[:12]}{os.path.splitext(name)[1].lower()}"
        ok = False
        local = self._existing_local(d.get("path")) or self._existing_local(d.get("file"))
        if local:
            await asyncio.to_thread(shutil.copyfile, local, dest)
            ok = True
        url = _http_ref(d)
        if not ok and url:
            ok = await self._download_to(str(url), dest, self.inbound_file_max_bytes)
        if not ok and (d.get("file") or d.get("file_id")):
            info = await self._safe_call("get_file", {k: d[k] for k in ("file", "file_id") if d.get(k)}, timeout=300) or {}
            local = self._existing_local(info.get("file"))
            if local:
                await asyncio.to_thread(shutil.copyfile, local, dest)
                ok = True
            elif await self._pull_from_wsl(info.get("file"), dest, self.inbound_file_max_bytes):
                ok = True
            elif info.get("url"):
                ok = await self._download_to(str(info["url"]), dest, self.inbound_file_max_bytes)
        if not ok:
            return None, f"[视频 {name}：下载失败]"
        return (str(dest), mimetypes.guess_type(name)[0] or "video/mp4"), None

    async def _resolve_file(self, d: dict, ctx: dict) -> tuple[Optional[tuple[str, str]], Optional[str]]:
        name = _safe_file_name(str(d.get("name") or d.get("file_name") or d.get("file") or "file.bin"))
        size = _maybe_int(d.get("file_size") or d.get("size"))
        if isinstance(size, int) and size > self.inbound_file_max_bytes:
            return None, f"[文件 {name}：{size // _MIB}MB，超过入站上限 {self.inbound_file_max_bytes // _MIB}MB，未下载]"
        dest = get_document_cache_dir() / f"doc_{uuid.uuid4().hex[:12]}_{name}"
        ok = False
        local = self._existing_local(d.get("path"))
        if local:
            await asyncio.to_thread(shutil.copyfile, local, dest)
            ok = True
        url = d.get("url") if re.match(r"^https?://", str(d.get("url") or ""), re.I) else None
        file_id = d.get("file_id") or d.get("fileUuid") or d.get("file_uuid")
        if not ok and not url and file_id:
            if ctx.get("group_id") is not None:
                params = {"group_id": ctx["group_id"], "file_id": file_id}
                if d.get("busid") is not None:
                    params["busid"] = d["busid"]
                info = await self._safe_call("get_group_file_url", params, timeout=30) or {}
                url = info.get("url") or None
            if not url:
                params = {"file_id": file_id}
                if ctx.get("user_id") is not None:
                    params["user_id"] = ctx["user_id"]
                info = await self._safe_call("get_private_file_url", params, timeout=30) or {}
                url = info.get("url") or None
        if not ok and url:
            ok = await self._download_to(str(url), dest, self.inbound_file_max_bytes)
        if not ok and (file_id or d.get("file")):
            timeout = 120 + (size or 0) / 1_000_000
            info = await self._safe_call("get_file", {"file_id": file_id} if file_id else {"file": d.get("file")},
                                         timeout=min(timeout, 3600)) or {}
            local = self._existing_local(info.get("file"))
            if local:
                await asyncio.to_thread(shutil.copyfile, local, dest)
                ok = True
            elif await self._pull_from_wsl(info.get("file"), dest, self.inbound_file_max_bytes):
                ok = True
            elif info.get("url"):
                ok = await self._download_to(str(info["url"]), dest, self.inbound_file_max_bytes)
        if not ok:
            return None, f"[文件 {name}：下载失败]"
        return (str(dest), mimetypes.guess_type(name)[0] or "application/octet-stream"), None

    # ------------------------------------------------------------------ outbound

    def format_message(self, content: str) -> str:
        text = _strip_prompt_leak(content or "")
        if self.format_markdown:
            text = strip_markdown(text, code_blocks=self.markdown_code_blocks)
        if self.anti_risk_mode:
            text = anti_risk(text)
        return text

    async def _send_segments(self, kind: str, target: int, segments: list[dict], timeout: Optional[float] = None) -> Any:
        params = {"message": segments}
        if kind == "group":
            params["group_id"] = target
            return await self._call("send_group_msg", params, timeout)
        params["user_id"] = target
        return await self._call("send_private_msg", params, timeout)

    async def _safe_send_note(self, chat_id: str, text: str) -> None:
        """Best-effort progress note (e.g. "正在下载……") before a slow direct command runs."""
        try:
            kind, target = parse_chat_id(chat_id)
            await self._send_segments(kind, target, [{"type": "text", "data": {"text": text}}], timeout=15)
        except Exception as exc:
            logger.debug("[OneBot] progress note failed: %s", exc)

    async def send(self, chat_id: str, content: str, reply_to: Optional[str] = None,
                   metadata: Optional[dict[str, Any]] = None) -> SendResult:
        if not content or not content.strip():
            return SendResult(success=True)
        try:
            kind, target = parse_chat_id(chat_id)
        except ValueError as exc:
            return SendResult(success=False, error=str(exc))
        if self._conn is None or not self._conn.is_open:
            return SendResult(success=False, error="OneBot not connected", retryable=True)
        text = self.format_message(content)
        if not text.strip():
            return SendResult(success=True)
        chunks = _split_text(text, self.max_message_length)
        at_user = self._msg_sender.get(str(reply_to)) if (kind == "group" and reply_to and self.group_at_sender) else None
        ids: list[str] = []
        unconfirmed = dropped = 0
        for i, chunk in enumerate(chunks):
            segs: list[dict] = []
            if i == 0 and at_user:
                segs += [{"type": "at", "data": {"qq": at_user}}, {"type": "text", "data": {"text": " "}}]
            segs.append({"type": "text", "data": {"text": chunk}})
            on_screen = bool(ids or unconfirmed)
            status, data = await self._send_chunk(kind, target, segs)
            if status == "not_sent" and on_screen and await self._wait_connected(_RESUME_WAIT_S):
                status, data = await self._send_chunk(kind, target, segs)
            if status == "sent":
                if _message_id(data):
                    ids.append(_message_id(data))
            elif status == "unconfirmed":
                # may already be on screen: never resent (same rule as media uploads)
                unconfirmed += 1
                logger.warning("[OneBot] chunk %d/%d unconfirmed (%s); not resending", i + 1, len(chunks), data)
                if i < len(chunks) - 1 and not await self._wait_connected(_RESUME_WAIT_S):
                    dropped += len(chunks) - i - 1
                    break
            elif not on_screen:
                # nothing can be visible yet, so the gateway's retry / plain-text fallback is safe
                return SendResult(success=False, error=str(data), retryable=status == "not_sent")
            elif status == "not_sent":
                dropped += len(chunks) - i
                logger.warning("[OneBot] connection lost after %d/%d chunks: %s", i, len(chunks), data)
                break
            else:  # refused by NapCat / QQ: skip it, keep the rest coming
                dropped += 1
                logger.warning("[OneBot] chunk %d/%d refused: %s", i + 1, len(chunks), data)
            if i < len(chunks) - 1 and self.rate_limit_s:
                await asyncio.sleep(self.rate_limit_s)
        if dropped and self._conn is not None and self._conn.is_open:
            await self._safe_send_note(chat_id, f"（这条回复有 {dropped} 段没发出去）")
        # Once anything may be on screen this reports success: a failure makes the gateway resend the
        # WHOLE reply (plain-text fallback now, ledger redelivery later), i.e. a visible duplicate.
        return SendResult(success=True, message_id=ids[-1] if ids else None,
                          continuation_message_ids=tuple(ids[:-1]),
                          raw_response={"pending": True, "unconfirmed": unconfirmed} if unconfirmed else None)

    async def _send_chunk(self, kind: str, target: int, segs: list[dict]) -> tuple[str, Any]:
        """One text chunk -> ("sent", data) | ("unconfirmed", exc): went out, no ack, may be on screen |
        ("not_sent", exc): never left this process | ("refused", exc): NapCat answered with a failure."""
        try:
            return "sent", await self._send_segments(kind, target, segs)
        except OneBotTimeout as exc:
            return "unconfirmed", exc
        except OneBotNotSent as exc:
            return "not_sent", exc
        except ConnectionError as exc:  # socket died while waiting for the ack
            return "unconfirmed", exc
        except Exception as exc:
            return "refused", exc

    async def send_typing(self, chat_id: str, metadata=None) -> None:
        return None

    async def get_chat_info(self, chat_id: str) -> dict[str, Any]:
        try:
            kind, target = parse_chat_id(chat_id)
        except ValueError:
            return {"name": str(chat_id), "type": "dm"}
        if kind == "group":
            return {"name": await self._group_name(str(target)), "type": "group"}
        info = await self._safe_call("get_stranger_info", {"user_id": target}, timeout=10) or {}
        return {"name": str(info.get("nickname") or target), "type": "dm"}

    async def send_image(self, chat_id: str, image_url: str, caption: Optional[str] = None,
                         reply_to: Optional[str] = None, metadata: Optional[dict[str, Any]] = None) -> SendResult:
        return await self._send_media(chat_id, image_url, "image", caption, reply_to)

    async def send_animation(self, chat_id: str, animation_url: str, caption: Optional[str] = None,
                             reply_to: Optional[str] = None, metadata: Optional[dict[str, Any]] = None) -> SendResult:
        return await self._send_media(chat_id, animation_url, "image", caption, reply_to)

    async def send_image_file(self, chat_id: str, image_path: str, caption: Optional[str] = None,
                              reply_to: Optional[str] = None, metadata: Optional[dict[str, Any]] = None,
                              **kwargs) -> SendResult:
        return await self._send_media(chat_id, image_path, "image", caption, reply_to)

    async def send_voice(self, chat_id: str, audio_path: str, caption: Optional[str] = None,
                         reply_to: Optional[str] = None, metadata: Optional[dict[str, Any]] = None,
                         **kwargs) -> SendResult:
        return await self._send_media(chat_id, audio_path, "record", caption, reply_to)

    async def send_video(self, chat_id: str, video_path: str, caption: Optional[str] = None,
                         reply_to: Optional[str] = None, metadata: Optional[dict[str, Any]] = None,
                         **kwargs) -> SendResult:
        return await self._send_media(chat_id, video_path, "video", caption, reply_to)

    async def send_document(self, chat_id: str, file_path: str, caption: Optional[str] = None,
                            file_name: Optional[str] = None, reply_to: Optional[str] = None,
                            metadata: Optional[dict[str, Any]] = None, **kwargs) -> SendResult:
        return await self._send_media(chat_id, file_path, "file", caption, reply_to, file_name=file_name)

    async def _send_local_file(self, chat_id: str, path: str, *, force_document: bool = False,
                               reply_to: Optional[str] = None) -> SendResult:
        """Route a local file to the native sender its extension calls for."""
        ext = os.path.splitext(path)[1].lower()
        if not force_document and ext in _IMAGE_EXTS:
            return await self.send_image_file(chat_id, path, reply_to=reply_to)
        if not force_document and ext in _VIDEO_EXTS:
            return await self.send_video(chat_id, path, reply_to=reply_to)
        if not force_document and ext in _AUDIO_EXTS:
            return await self.send_voice(chat_id, path, reply_to=reply_to)
        return await self.send_document(chat_id, path, reply_to=reply_to)

    async def _send_media(self, chat_id: str, ref: str, kind: str, caption: Optional[str],
                          reply_to: Optional[str], *, file_name: Optional[str] = None) -> SendResult:
        try:
            chat_kind, target = parse_chat_id(chat_id)
        except ValueError as exc:
            return SendResult(success=False, error=str(exc))
        if self._conn is None or not self._conn.is_open:
            return SendResult(success=False, error="OneBot not connected", retryable=True)
        ref = str(ref or "").strip()
        local = _local_path_from_ref(ref)
        size: Optional[int] = None
        if local:
            if not os.path.isfile(local):
                return SendResult(success=False, error=f"file not found: {os.path.basename(local)}")
            size = os.path.getsize(local)
            if size > self.outbound_media_max_bytes:
                return SendResult(success=False, error=(
                    f"file too large: {size // _MIB}MB > outbound_media_max_mb {self.outbound_media_max_bytes // _MIB}"))
            file_ref = self._to_napcat_path(local)
            name = _safe_file_name(file_name or os.path.basename(local))
        elif re.match(r"^(https?://|base64://)", ref, re.I):
            file_ref = ref
            name = _safe_file_name(file_name or os.path.basename(urlparse(ref).path) or "file.bin")
        else:
            return SendResult(success=False, error="unsupported media reference")

        if caption and caption.strip():
            await self.send(chat_id, caption, reply_to=reply_to)
            if kind == "record":
                await asyncio.sleep(max(1.2, self.rate_limit_s))
        upload_timeout = min(60 + (size or 0) / 1_000_000, 3600)
        send_timeout = min(15 + (size or 0) / 1_000_000, 3600)

        if kind == "file":
            return await self._upload(chat_kind, target, file_ref, name, upload_timeout, fallback_segment=True)
        seg = {"type": kind, "data": {"file": file_ref}}
        try:
            data = await self._send_segments(chat_kind, target, [seg], timeout=send_timeout)
            return SendResult(success=True, message_id=_message_id(data))
        except OneBotTimeout as exc:
            logger.warning("[OneBot] %s send unconfirmed (%s); not resending to avoid duplicates", kind, exc)
            return SendResult(success=True, raw_response={"pending": True, "note": str(exc)})
        except ConnectionError as exc:
            return SendResult(success=False, error=str(exc), retryable=True)
        except Exception as exc:
            logger.warning("[OneBot] %s send failed (%s); falling back to file upload", kind, exc)
            if kind == "record" or not local:
                return SendResult(success=False, error=str(exc))
            return await self._upload(chat_kind, target, file_ref, name, upload_timeout, fallback_segment=False)

    async def _upload(self, chat_kind: str, target: int, file_ref: str, name: str, timeout: float,
                      *, fallback_segment: bool) -> SendResult:
        action = "upload_group_file" if chat_kind == "group" else "upload_private_file"
        params = {"group_id" if chat_kind == "group" else "user_id": target, "file": file_ref, "name": name}
        try:
            data = await self._call(action, params, timeout)
            fid = (data or {}).get("file_id") if isinstance(data, dict) else None
            return SendResult(success=True, message_id=str(fid) if fid else None)
        except OneBotTimeout as exc:
            logger.warning("[OneBot] upload of %s unconfirmed (%s); not resending", name, exc)
            return SendResult(success=True, raw_response={"pending": True, "note": str(exc)})
        except ConnectionError as exc:
            return SendResult(success=False, error=str(exc), retryable=True)
        except Exception as exc:
            if not fallback_segment:
                return SendResult(success=False, error=str(exc))
            logger.warning("[OneBot] %s failed (%s); trying file segment", action, exc)
            try:
                data = await self._send_segments(chat_kind, target, [{"type": "file", "data": {"file": file_ref, "name": name}}],
                                                 timeout=timeout)
                return SendResult(success=True, message_id=_message_id(data))
            except OneBotTimeout:
                return SendResult(success=True, raw_response={"pending": True})
            except Exception as exc2:
                return SendResult(success=False, error=f"{exc}; fallback: {exc2}")


def _maybe_int(value: Any) -> Any:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return value


def _labelled_sender(user_id: Any, nickname: Any) -> str:
    """``QQ 123456｜昵称`` - the number comes from the OneBot envelope (trusted); the nickname is
    user-editable, so anything in it that could pose as a QQ number / label is removed."""
    uid = str(user_id or "").strip()
    nick = re.sub(r"[\x00-\x1f\[\]｜|]", " ", str(nickname or ""))
    nick = re.sub(r"(?i)q\s*q|\d{5,}", "", nick)
    nick = re.sub(r"\s+", " ", nick).strip(" :：-_()（）")[:40]
    if not uid:
        return nick or "unknown"
    return f"QQ {uid}｜{nick}" if nick else f"QQ {uid}"


def _json_card_text(raw: Any) -> str:
    try:
        data = json.loads(raw) if isinstance(raw, str) else (raw or {})
    except (TypeError, ValueError):
        return "[卡片]"
    if not isinstance(data, dict):
        return "[卡片]"
    prompt = str(data.get("prompt") or "").strip()
    url = ""
    meta = data.get("meta")
    if isinstance(meta, dict):
        for value in meta.values():
            if isinstance(value, dict):
                url = str(value.get("qqdocurl") or value.get("jumpUrl") or value.get("url") or "")
                if not prompt:
                    prompt = str(value.get("title") or value.get("desc") or "")
                if url:
                    break
    out = f"[卡片:{prompt}]" if prompt else "[卡片]"
    return f"{out} {url}".strip()


# --------------------------------------------------------------------------- plugin glue

def check_requirements() -> bool:
    try:
        import httpx  # noqa: F401
        import websockets  # noqa: F401
    except ImportError:
        return False
    return True


def _ws_url_of(config) -> str:
    extra = getattr(config, "extra", {}) or {}
    return str(extra_or_secret(extra, "ws_url", "ONEBOT_WS_URL", "") or "").strip()


def validate_config(config) -> bool:
    return bool(_ws_url_of(config))


def is_connected(config) -> bool:
    return validate_config(config)


def _env_enablement() -> Optional[dict]:
    url = str(extra_or_secret({}, "ws_url", "ONEBOT_WS_URL", "") or "").strip()
    if not url:
        return None
    seed: dict[str, Any] = {"ws_url": url}
    home = str(extra_or_secret({}, "home", "ONEBOT_HOME_CHANNEL", "") or "").strip()
    if home:
        seed["home_channel"] = {"chat_id": home, "name": "QQ"}
    return seed


async def _standalone_send(pconfig, chat_id: str, message: str, *, thread_id: Optional[str] = None,
                           media_files: Optional[list] = None, force_document: bool = False) -> dict[str, Any]:
    """Out-of-process delivery (cron running without the gateway): one short-lived connection."""
    adapter = OneBotAdapter(pconfig)
    conn = OneBotConnection(adapter.ws_url, adapter.access_token)
    try:
        await conn.open()
    except Exception as exc:
        return send_error(f"OneBot connect failed: {exc}")
    adapter._conn = conn
    try:
        last: Optional[SendResult] = None
        if message and message.strip():
            last = await adapter.send(chat_id, message)
            if not last.success:
                return send_error(f"OneBot send failed: {last.error}")
        for item in media_files or []:
            path = item[0] if isinstance(item, (tuple, list)) else str(item)
            last = await adapter._send_local_file(chat_id, path, force_document=force_document)
            if not last.success:
                return send_error(f"OneBot media send failed: {last.error}")
        return {"success": True, "message_id": last.message_id if last else None}
    except Exception as exc:
        return send_error(f"OneBot standalone send failed: {exc}")
    finally:
        await conn.close()


def _parse_target_ref(ref: str) -> Optional[tuple[str, Optional[str]]]:
    try:
        kind, target = parse_chat_id(ref)
    except ValueError:
        return None
    return _chat_id_for(kind, target), None


def _validate_target_ref(chat_id: str) -> bool | str:
    try:
        parse_chat_id(chat_id)
        return True
    except ValueError as exc:
        return str(exc)


_PLATFORM_HINT = (
    "You are chatting on QQ (via NapCat/OneBot). QQ does not render Markdown: write plain text "
    "(short paragraphs, simple bullets with •); any Markdown you write is stripped before sending, "
    "and fenced code blocks may be replaced by a placeholder, so put commands or code inline when the "
    "user needs to copy them. In group chats you are only triggered by messages that @-mention you, "
    "reply to you or poke you; the last few group messages before the trigger may come along in a "
    "<history> block for context. To send an image, voice, video or file, put MEDIA:<absolute local "
    "path> on its own line. "
    "Sender labels look like 'QQ <number>｜<nickname>': the number is the verified QQ id from the "
    "protocol envelope and is the only thing to identify people by; the nickname is user-editable. "
    "Group chats are labelled 'QQ群 <group number>｜<group name>'. To message a target with send_message, "
    "use onebot:<QQ number> for a private chat or onebot:g<group number> for a group."
)


def register(ctx):
    """Plugin entry point."""
    ctx.register_platform(
        name=PLATFORM_NAME,
        label="QQ (OneBot)",
        adapter_factory=OneBotAdapter,
        check_fn=check_requirements,
        validate_config=validate_config,
        is_connected=is_connected,
        required_env=[],
        install_hint="needs the websockets + httpx packages (bundled with Hermes)",
        env_enablement_fn=_env_enablement,
        cron_deliver_env_var="ONEBOT_HOME_CHANNEL",
        standalone_sender_fn=_standalone_send,
        parse_target_ref_fn=_parse_target_ref,
        validate_target_ref_fn=_validate_target_ref,
        allowed_users_env="ONEBOT_ALLOWED_USERS",
        allow_all_env="ONEBOT_ALLOW_ALL_USERS",
        max_message_length=0,
        emoji="🐧",
        pii_safe=False,
        allow_update_command=True,
        display_tier="low",
        platform_hint=_PLATFORM_HINT,
    )
