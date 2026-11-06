#!/usr/bin/env python3
"""Unified Telegram project analytics.

Reads outreach data from Dialog Hub, accepts durable events from TG-zayavki,
and publishes daily/weekly/monthly reports to a configured Telegram chat.
"""
import asyncio
import datetime as dt
import html
import json
import logging
import os
import re
import secrets
import sqlite3
from collections import defaultdict
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import aiohttp
from aiohttp import web
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("project-analytics")

USERNAME_RE = re.compile(r"(?<![\w@])@([A-Za-z][A-Za-z0-9_]{4,31})\b")
USERNAME_URL_RE = re.compile(r"(?:https?://)?t\.me/([A-Za-z][A-Za-z0-9_]{4,31})\b", re.IGNORECASE)


def normalize_username(value: Any) -> str:
    """Return a comparable Telegram username, without the leading @."""
    if not isinstance(value, str):
        return ""
    username = value.strip().lstrip("@").casefold()
    return username if re.fullmatch(r"[a-z][a-z0-9_]{4,31}", username) else ""


def username_from_post(post: dict[str, Any]) -> str:
    """Read the username published with Masha's photo lead."""
    text = post.get("caption") or post.get("text") or ""
    if not isinstance(text, str):
        return ""
    match = USERNAME_RE.search(text) or USERNAME_URL_RE.search(text)
    return normalize_username(match.group(1)) if match else ""


@dataclass(frozen=True)
class Settings:
    token: str
    db_path: Path
    dialoghub_db: Path
    dialoghub_sessions_dir: Path
    followup_db: Path
    timezone: ZoneInfo
    api_host: str
    api_port: int
    ingest_token: str

    @classmethod
    def from_env(cls) -> "Settings":
        token = os.getenv("BOT_TOKEN", "")
        if not token:
            raise RuntimeError("BOT_TOKEN is required")
        return cls(
            token=token,
            db_path=Path(os.getenv("DATABASE_PATH", "data/analytics.sqlite3")),
            dialoghub_db=Path(os.getenv("DIALOGHUB_DATABASE_PATH", "/opt/dialoghub/data/dialoghub.sqlite3")),
            dialoghub_sessions_dir=Path(os.getenv("DIALOGHUB_SESSIONS_DIR", "/root/FULL_CRM/accounts")),
            followup_db=Path(os.getenv("FOLLOWUP_DATABASE_PATH", "/opt/tg-follow-up/data/followup.sqlite3")),
            timezone=ZoneInfo(os.getenv("REPORT_TIMEZONE", "Europe/Moscow")),
            api_host=os.getenv("INGEST_HOST", "127.0.0.1"),
            api_port=int(os.getenv("INGEST_PORT", "8071")),
            ingest_token=os.getenv("INGEST_TOKEN", ""),
        )


class Store:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path)
        self.db.row_factory = sqlite3.Row
        self.db.executescript("""
        PRAGMA journal_mode=WAL;
        CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY,
            source TEXT NOT NULL,
            event_key TEXT NOT NULL UNIQUE,
            project TEXT NOT NULL,
            event_type TEXT NOT NULL,
            occurred_at INTEGER NOT NULL,
            payload TEXT NOT NULL DEFAULT '{}'
        );
        CREATE INDEX IF NOT EXISTS idx_events_period ON events(occurred_at, project, event_type);
        CREATE TABLE IF NOT EXISTS sent_reports (
            kind TEXT NOT NULL,
            period_start TEXT NOT NULL,
            PRIMARY KEY(kind, period_start)
        );
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            started_at INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS report_deliveries (
            kind TEXT NOT NULL,
            period_start TEXT NOT NULL,
            user_id INTEGER NOT NULL,
            PRIMARY KEY(kind, period_start, user_id)
        );
        CREATE TABLE IF NOT EXISTS report_messages (
            kind TEXT NOT NULL,
            period_start TEXT NOT NULL,
            chat_id INTEGER NOT NULL,
            message_id INTEGER NOT NULL,
            PRIMARY KEY(kind, period_start, chat_id)
        );
        """)
        self.db.commit()

    def get(self, key: str, default: str = "") -> str:
        row = self.db.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        return row["value"] if row else default

    def set(self, key: str, value: str) -> None:
        self.db.execute("INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, value))
        self.db.commit()

    def add_event(self, event: dict[str, Any]) -> bool:
        try:
            self.db.execute(
                "INSERT INTO events(source,event_key,project,event_type,occurred_at,payload) VALUES(?,?,?,?,?,?)",
                (event["source"], event["event_key"], event["project"], event["event_type"], event["occurred_at"], json.dumps(event.get("payload", {}), ensure_ascii=False)),
            )
            self.db.commit()
            return True
        except sqlite3.IntegrityError:
            return False

    def event_rows(self, start: int, end: int | None) -> list[sqlite3.Row]:
        sql = "SELECT project,event_type,COUNT(*) count FROM events WHERE occurred_at>=?"
        args: list[Any] = [start]
        if end is not None:
            sql += " AND occurred_at<?"; args.append(end)
        sql += " GROUP BY project,event_type"
        return self.db.execute(sql, args).fetchall()

    def funnel_events(self, start: int = 0, end: int | None = None) -> list[sqlite3.Row]:
        sql = "SELECT project,event_type,event_key,occurred_at,payload FROM events WHERE event_type IN ('second_message_sent','join_request') AND occurred_at>=?"
        args: list[Any] = [start]
        if end is not None:
            sql += " AND occurred_at<?"; args.append(end)
        return self.db.execute(sql, args).fetchall()

    def identity_events(self, start: int, end: int | None) -> list[sqlite3.Row]:
        """Return the two Apple streams used for person-level matching."""
        sql = "SELECT project,event_type,event_key,payload FROM events WHERE event_type IN ('join_request','lead') AND occurred_at>=?"
        args: list[Any] = [start]
        if end is not None:
            sql += " AND occurred_at<?"; args.append(end)
        return self.db.execute(sql, args).fetchall()

    def report_sent(self, kind: str, period_start: str) -> bool:
        return self.db.execute("SELECT 1 FROM sent_reports WHERE kind=? AND period_start=?", (kind, period_start)).fetchone() is not None

    def mark_report_sent(self, kind: str, period_start: str) -> None:
        self.db.execute("INSERT OR IGNORE INTO sent_reports(kind,period_start) VALUES(?,?)", (kind, period_start)); self.db.commit()

    def add_user(self, user_id: int) -> None:
        self.db.execute("INSERT OR IGNORE INTO users(user_id,started_at) VALUES(?,?)", (user_id, int(dt.datetime.now(dt.timezone.utc).timestamp())))
        self.db.commit()

    def has_user(self, user_id: int) -> bool:
        return self.db.execute("SELECT 1 FROM users WHERE user_id=?", (user_id,)).fetchone() is not None

    def user_ids(self) -> list[int]:
        return [row["user_id"] for row in self.db.execute("SELECT user_id FROM users ORDER BY started_at")]

    def report_delivered(self, kind: str, period_start: str, user_id: int) -> bool:
        return self.db.execute(
            "SELECT 1 FROM report_deliveries WHERE kind=? AND period_start=? AND user_id=?",
            (kind, period_start, user_id),
        ).fetchone() is not None

    def mark_report_delivered(self, kind: str, period_start: str, user_id: int) -> None:
        self.db.execute(
            "INSERT OR IGNORE INTO report_deliveries(kind,period_start,user_id) VALUES(?,?,?)",
            (kind, period_start, user_id),
        )
        self.db.commit()

    def save_report_message(self, kind: str, period_start: str, chat_id: int, message_id: int) -> None:
        self.db.execute(
            "INSERT OR REPLACE INTO report_messages(kind,period_start,chat_id,message_id) VALUES(?,?,?,?)",
            (kind, period_start, chat_id, message_id),
        )
        self.db.commit()

    def apple_leads_channel_id(self) -> int | None:
        value = self.get("apple_leads_channel_id")
        return int(value) if value else None

    def set_apple_leads_channel_id(self, chat_id: int) -> None:
        self.set("apple_leads_channel_id", str(chat_id))

    def seafood_leads_channel_id(self) -> int | None:
        value = self.get("seafood_leads_channel_id")
        return int(value) if value else None

    def set_seafood_leads_channel_id(self, chat_id: int) -> None:
        self.set("seafood_leads_channel_id", str(chat_id))
    def tenders_report_chat_ids(self) -> list[int]:
        values = [value.strip() for value in self.get("tenders_report_chat_ids").split(",") if value.strip()]
        legacy = self.get("tenders_report_chat_id").strip()
        if legacy and legacy not in values:
            values.append(legacy)
        return list(dict.fromkeys(int(value) for value in values))

    def add_tenders_report_chat(self, chat_id: int) -> bool:
        chat_ids = self.tenders_report_chat_ids()
        if chat_id in chat_ids:
            return False
        chat_ids.append(chat_id)
        self.set("tenders_report_chat_ids", ",".join(str(value) for value in chat_ids))
        return True

    def blog_report_chat_ids(self) -> list[int]:
        return [int(value) for value in self.get("dmitry_blog_report_chat_ids").split(",") if value.strip()]

    def add_blog_report_chat(self, chat_id: int) -> bool:
        chat_ids = self.blog_report_chat_ids()
        if chat_id in chat_ids:
            return False
        chat_ids.append(chat_id)
        self.set("dmitry_blog_report_chat_ids", ",".join(str(value) for value in chat_ids))
        return True


class Metrics:
    """Combines direct Dialog Hub outreach facts with submitted funnel events."""
    def __init__(self, store: Store, hub_db: Path, hub_sessions_dir: Path = Path("/root/FULL_CRM/accounts"), followup_db: Path = Path("/opt/tg-follow-up/data/followup.sqlite3")):
        self.store, self.hub_db, self.hub_sessions_dir, self.followup_db = store, hub_db, hub_sessions_dir, followup_db

    @staticmethod
    def project_name(name: str) -> str:
        aliases = {"apple": "АЙФОНЫ", "AI РАЗБОР ТЕНДЕРОВ": "ГОСЗАКУПКИ"}
        return aliases.get(name, name)

    def own_peer_ids(self) -> set[int]:
        """Read Telegram IDs of all Dialog Hub accounts from their sessions.

        An internal conversation uses one of these IDs as ``peer_id``.  The
        account registry controls the scope, so stale unrelated session files
        can never affect analytics.
        """
        if not self.hub_db.exists() or not self.hub_sessions_dir.exists():
            return set()
        try:
            with closing(sqlite3.connect(f"file:{self.hub_db}?mode=ro", uri=True)) as db:
                session_names = [row[0] for row in db.execute("SELECT session_name FROM accounts")]
            ids: set[int] = set()
            for session_name in session_names:
                path = self.hub_sessions_dir / f"{Path(session_name).name}.session"
                if not path.exists():
                    continue
                try:
                    with closing(sqlite3.connect(f"file:{path}?mode=ro", uri=True)) as session_db:
                        row = session_db.execute("SELECT user_id FROM sessions LIMIT 1").fetchone()
                    if row and isinstance(row[0], int):
                        ids.add(row[0])
                except sqlite3.Error:
                    log.warning("Could not read Dialog Hub account session: %s", path.name)
            return ids
        except sqlite3.Error:
            log.exception("Could not read Dialog Hub account registry")
            return set()

    def _own_peer_filter(self, column: str = "peer_id") -> str:
        own_ids = self.own_peer_ids()
        if not own_ids:
            return ""
        return f" AND {column} NOT IN ({','.join(str(peer_id) for peer_id in sorted(own_ids))})"

    def _hub_rows(self, start: int, end: int | None) -> list[sqlite3.Row]:
        if not self.hub_db.exists():
            log.warning("Dialog Hub database not found: %s", self.hub_db)
            return []
        # Only the first outgoing message in an account's dialogue with a
        # customer is an initial outreach.  Later messages — including Follow
        # Up Bot campaigns — belong to an existing dialogue and must not
        # inflate acquisition metrics.
        own_peer_filter = self._own_peer_filter("f.peer_id")
        query = f"""
          WITH first_outreach AS (
            SELECT account_id, project_id, peer_id, MIN(sent_at) sent_at
            FROM outreach_messages
            GROUP BY account_id, project_id, peer_id
          ), first_reply AS (
            SELECT o.account_id, o.project_id, o.peer_id, MIN(o.replied_at) replied_at
            FROM outreach_messages o
            JOIN first_outreach f ON f.account_id=o.account_id AND f.project_id=o.project_id AND f.peer_id=o.peer_id
            WHERE o.replied_at IS NOT NULL AND o.replied_at>=f.sent_at
            GROUP BY o.account_id, o.project_id, o.peer_id
          )
          SELECT COALESCE(p.name, 'Без проекта') project,
            COUNT(*) sent,
            SUM(CASE WHEN r.replied_at IS NOT NULL AND (:end IS NULL OR r.replied_at<:end) THEN 1 ELSE 0 END) replied,
            SUM(CASE WHEN r.replied_at>=:start AND (:end IS NULL OR r.replied_at<:end) THEN 1 ELSE 0 END) replies_received
          FROM first_outreach f LEFT JOIN first_reply r ON r.account_id=f.account_id AND r.project_id=f.project_id AND r.peer_id=f.peer_id
          LEFT JOIN projects p ON p.id=f.project_id
          WHERE f.sent_at>=:start AND (:end IS NULL OR f.sent_at<:end) {own_peer_filter}
          GROUP BY COALESCE(p.name, 'Без проекта')
        """
        try:
            with closing(sqlite3.connect(f"file:{self.hub_db}?mode=ro", uri=True)) as db:
                db.row_factory = sqlite3.Row
                return db.execute(query, {"start": start, "end": end}).fetchall()
        except sqlite3.Error:
            log.exception("Could not read Dialog Hub metrics")
            return []

    def _qualifying_second_replies(self, start: int, end: int | None) -> dict[tuple[str, int], int]:
        """Return customers eligible for a follow-up and their first reply time.

        A second message is a funnel step only when a unique recipient first
        received the initial outreach in the period and replied before that
        follow-up.  This prevents both duplicate follow-ups and an orphaned
        event from inflating the conversion rate.
        """
        if not self.hub_db.exists():
            return {}
        own_peer_filter = self._own_peer_filter("f.peer_id")
        query = f"""
          WITH period_outreach AS (
            SELECT project_id, peer_id, MIN(sent_at) sent_at
            FROM outreach_messages
            WHERE sent_at>=:start AND (:end IS NULL OR sent_at<:end)
            GROUP BY project_id, peer_id
          ), period_reply AS (
            SELECT project_id, peer_id, MIN(replied_at) replied_at
            FROM outreach_messages
            WHERE sent_at>=:start AND (:end IS NULL OR sent_at<:end)
              AND replied_at IS NOT NULL AND replied_at>=sent_at
              AND (:end IS NULL OR replied_at<:end)
            GROUP BY project_id, peer_id
          )
          SELECT COALESCE(p.name, 'Без проекта') project, f.peer_id, r.replied_at
          FROM period_outreach f
          JOIN period_reply r ON r.project_id=f.project_id AND r.peer_id=f.peer_id
          LEFT JOIN projects p ON p.id=f.project_id
          WHERE 1=1 {own_peer_filter}
        """
        try:
            with closing(sqlite3.connect(f"file:{self.hub_db}?mode=ro", uri=True)) as db:
                rows = db.execute(query, {"start": start, "end": end}).fetchall()
        except sqlite3.Error:
            log.exception("Could not read Dialog Hub follow-up eligibility")
            return {}
        return {(self.project_name(row[0]), int(row[1])): int(row[2]) for row in rows}

    def summary(self, start: int, end: int | None) -> dict[str, dict[str, int]]:
        data: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        for row in self._hub_rows(start, end):
            project = self.project_name(row["project"])
            data[project]["sent"] += row["sent"] or 0
            data[project]["replied"] += row["replied"] or 0
            data[project]["replies_received"] += row["replies_received"] or 0
        names = {"join_request": "requests", "join_approved": "approved", "second_message_sent": "second_sent", "third_message_sent": "third_sent", "lead": "leads"}
        for row in self.store.event_rows(start, end):
            # Follow-ups carry the peer ID in their payload, so they can be
            # filtered below.  Do not let an internal account-to-account test
            # inflate a customer-facing conversion rate.
            if row["event_type"] == "second_message_sent":
                continue
            project = self.project_name(row["project"])
            data[project][names.get(row["event_type"], row["event_type"])] += row["count"]
        qualifying_replies = self._qualifying_second_replies(start, end)
        second_people: set[tuple[str, int]] = set()
        for row in self.store.funnel_events(start, end):
            if row["event_type"] != "second_message_sent":
                continue
            try:
                peer_id = int(json.loads(row["payload"]).get("user_id"))
            except (TypeError, ValueError, json.JSONDecodeError):
                peer_id = None
            project = self.project_name(row["project"])
            replied_at = qualifying_replies.get((project, peer_id))
            if replied_at is None or int(row["occurred_at"]) < replied_at:
                continue
            second_people.add((project, peer_id))
        for project, _peer_id in second_people:
            data[project]["second_sent"] += 1
        return data

    def followup_summary(self, start: int, end: int | None) -> dict[str, int]:
        """Count cold Follow Up Bot deliveries from its own durable ledger.

        This deliberately does not infer follow-ups from all non-first
        Dialog Hub messages: managers can send ordinary messages in an
        existing dialogue.  The Follow Up service writes an audit event only
        after its own delivery succeeds, making this a separate, auditable
        metric that never affects first-contact acquisition numbers.
        """
        result = {"sent": 0, "dialogs": 0}
        if not self.followup_db.exists():
            log.warning("Follow Up database not found: %s", self.followup_db)
            return result
        start_at = dt.datetime.fromtimestamp(start, dt.timezone.utc).isoformat()
        end_at = dt.datetime.fromtimestamp(end, dt.timezone.utc).isoformat() if end is not None else None
        query = """
          SELECT COUNT(*) sent,
                 COUNT(DISTINCT CAST(account_id AS TEXT) || ':' || CAST(peer_id AS TEXT)) dialogs
          FROM audit_log
          WHERE kind='followup_sent' AND at>=?
        """
        params: list[Any] = [start_at]
        if end_at is not None:
            query += " AND at<?"
            params.append(end_at)
        try:
            with closing(sqlite3.connect(f"file:{self.followup_db}?mode=ro", uri=True)) as db:
                row = db.execute(query, params).fetchone()
            result["sent"] = int(row[0] or 0)
            result["dialogs"] = int(row[1] or 0)
        except sqlite3.Error:
            log.exception("Could not read Follow Up metrics")
        return result

    def apple_lead_match_stats(self, start: int, end: int | None) -> dict[str, int]:
        """Count unique channel applicants also published as Masha photo leads.

        A lead post carries the username from its caption; the approval bot
        carries the applicant's Telegram username.  Sets intentionally make
        reposts and repeated applications count as one person.
        """
        # The selected period applies to channel applications.  Masha's list
        # is deliberately global: a person she posted last week must not turn
        # into an "outside" application merely because they joined this week.
        lead_usernames: set[str] = set()
        for row in self.store.identity_events(0, None):
            if self.project_name(row["project"]) != "АЙФОНЫ":
                continue
            if row["event_type"] != "lead":
                continue
            try:
                username = normalize_username(json.loads(row["payload"]).get("username", ""))
            except (TypeError, json.JSONDecodeError):
                username = ""
            if username:
                lead_usernames.add(username)

        applicant_usernames: set[str] = set()
        applicants_without_username = 0
        for row in self.store.identity_events(start, end):
            if self.project_name(row["project"]) != "АЙФОНЫ" or row["event_type"] != "join_request":
                continue
            try:
                username = normalize_username(json.loads(row["payload"]).get("username", ""))
            except (TypeError, json.JSONDecodeError):
                username = ""
            if username:
                applicant_usernames.add(username)
            else:
                applicants_without_username += 1
        return {
            "matched": len(lead_usernames & applicant_usernames),
            "outside": len(applicant_usernames - lead_usernames),
            "unmatched": applicants_without_username,
        }

    def second_message_variants(self, project: str, start: int = 0, end: int | None = None) -> list[dict[str, Any]]:
        events = self.store.funnel_events(start, end)
        requests = {(row["project"], row["event_key"]) for row in events if row["event_type"] == "join_request"}
        own_peer_ids = self.own_peer_ids()
        variants: dict[str, dict[str, int]] = {}
        for row in events:
            if row["event_type"] != "second_message_sent" or self.project_name(row["project"]) != project:
                continue
            try:
                payload = json.loads(row["payload"])
                if int(payload.get("user_id")) in own_peer_ids:
                    continue
                label = payload.get("message_text", "").strip()
            except (TypeError, json.JSONDecodeError):
                label = ""
            except ValueError:
                label = ""
            if not label:
                continue
            data = variants.setdefault(label, {"sent": 0, "requests": 0})
            data["sent"] += 1
            request_key = "join-request:" + row["event_key"].removeprefix("second-message:")
            if (row["project"], request_key) in requests:
                data["requests"] += 1
        return [{"text": text, **data} for text, data in sorted(variants.items(), key=lambda item: (-item[1]["sent"], item[0]))]

    def apple_base_summary(self, start: int, end: int | None) -> dict[str, dict[str, int]]:
        """Aggregate the user-approved Apple accounts by their response source."""
        bases = {
            "2ГИС": (20, 16, 14, 9, 11, 28, 29, 30, 34, 35),
            "Яндекс Карты": (12, 13, 15, 24, 10, 26, 27, 31, 32, 33),
            "Инст": (19,),
        }
        result = {name: {"sent": 0, "replied": 0} for name in bases}
        if not self.hub_db.exists():
            return result
        account_to_base = {account_id: name for name, account_ids in bases.items() for account_id in account_ids}
        placeholders = ",".join("?" for _ in account_to_base)
        own_peer_filter = self._own_peer_filter("f.peer_id")
        query = f"""
          WITH first_outreach AS (
            SELECT account_id, project_id, peer_id, MIN(sent_at) sent_at
            FROM outreach_messages
            GROUP BY account_id, project_id, peer_id
          ), first_reply AS (
            SELECT o.account_id, o.project_id, o.peer_id, MIN(o.replied_at) replied_at
            FROM outreach_messages o
            JOIN first_outreach f ON f.account_id=o.account_id AND f.project_id=o.project_id AND f.peer_id=o.peer_id
            WHERE o.replied_at IS NOT NULL AND o.replied_at>=f.sent_at
            GROUP BY o.account_id, o.project_id, o.peer_id
          )
          SELECT f.account_id, f.peer_id,
            CASE WHEN r.replied_at IS NOT NULL AND (:end IS NULL OR r.replied_at<:end) THEN 1 ELSE 0 END replied
          FROM first_outreach f JOIN projects p ON p.id=f.project_id
          LEFT JOIN first_reply r ON r.account_id=f.account_id AND r.project_id=f.project_id AND r.peer_id=f.peer_id
          WHERE p.name='АЙФОНЫ' AND f.account_id IN ({placeholders})
            AND f.sent_at>=:start AND (:end IS NULL OR f.sent_at<:end) {own_peer_filter}
        """
        try:
            with closing(sqlite3.connect(f"file:{self.hub_db}?mode=ro", uri=True)) as db:
                db.row_factory = sqlite3.Row
                params: dict[str, Any] = {"start": start, "end": end}
                # sqlite supports named and positional parameters together only
                # awkwardly, so interpolate validated integer IDs from this map.
                safe_ids = ",".join(str(account_id) for account_id in account_to_base)
                rows = db.execute(query.replace(placeholders, safe_ids), params).fetchall()
        except sqlite3.Error:
            log.exception("Could not read Apple base metrics")
            return result
        for row in rows:
            base = account_to_base[row["account_id"]]
            result[base]["sent"] += 1
            if row["replied"]:
                result[base]["replied"] += 1
        return result

    def outreach_texts(self, project: str, start: int, end: int | None) -> list[sqlite3.Row]:
        """Message-level results: which first outreach texts create replies."""
        if not self.hub_db.exists(): return []
        own_peer_filter = self._own_peer_filter("f.peer_id")
        query = f"""
          WITH ranked_outreach AS (
            SELECT project_id, peer_id, sent_at, script_label,
                   ROW_NUMBER() OVER (PARTITION BY project_id, peer_id ORDER BY sent_at, id) AS position
            FROM outreach_messages
          ), first_outreach AS (
            SELECT project_id, peer_id, sent_at, script_label
            FROM ranked_outreach WHERE position=1
          ), first_reply AS (
            SELECT project_id, peer_id, MIN(replied_at) replied_at
            FROM outreach_messages WHERE replied_at IS NOT NULL GROUP BY project_id, peer_id
          )
          SELECT f.script_label, COUNT(*) sent, MAX(f.sent_at) last_sent_at,
            SUM(CASE WHEN r.replied_at>=:start AND (:end IS NULL OR r.replied_at<:end) THEN 1 ELSE 0 END) replied
          FROM first_outreach f
          LEFT JOIN first_reply r ON r.project_id=f.project_id AND r.peer_id=f.peer_id
          JOIN projects p ON p.id=f.project_id
          WHERE p.name=:project AND f.sent_at>=:start AND (:end IS NULL OR f.sent_at<:end) {own_peer_filter}
          GROUP BY f.script_label ORDER BY sent DESC, replied DESC, f.script_label
        """
        try:
            with closing(sqlite3.connect(f"file:{self.hub_db}?mode=ro", uri=True)) as db:
                db.row_factory = sqlite3.Row
                return db.execute(query, {"project": project, "start": start, "end": end}).fetchall()
        except sqlite3.Error:
            log.exception("Could not read outreach text metrics")
            return []


class TelegramAPI:
    def __init__(self, token: str):
        self.base = f"https://api.telegram.org/bot{token}"
        self.http: aiohttp.ClientSession | None = None

    async def start(self): self.http = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60))
    async def close(self):
        if self.http: await self.http.close()

    async def call(self, method: str, **payload):
        assert self.http
        async with self.http.post(f"{self.base}/{method}", json=payload) as response:
            data = await response.json()
        if not data.get("ok"):
            raise RuntimeError(data.get("description", "Telegram API error"))
        return data["result"]

    async def send(self, chat_id: int, text: str, keyboard: dict | None = None):
        payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}
        if keyboard: payload["reply_markup"] = keyboard
        return await self.call("sendMessage", **payload)

    async def delete(self, chat_id: int, message_id: int):
        return await self.call("deleteMessage", chat_id=chat_id, message_id=message_id)

    async def poll(self, offset: int):
        return await self.call(
            "getUpdates",
            offset=offset,
            timeout=45,
            allowed_updates=["message", "callback_query", "channel_post", "chat_join_request"],
        )


class AnalyticsBot:
    def __init__(self, settings: Settings):
        self.s = settings
        self.store = Store(settings.db_path)
        self.metrics = Metrics(self.store, settings.dialoghub_db, settings.dialoghub_sessions_dir, settings.followup_db)
        self.tg = TelegramAPI(settings.token)
        self.offset = int(self.store.get("telegram_offset", "0"))

    @staticmethod
    def keyboard() -> dict:
        return {"inline_keyboard": [
            [
                {"text": "◀️ Вчера", "callback_data": "day"},
                {"text": "\U0001F4C5 \u042D\u0442\u0430 \u043D\u0435\u0434\u0435\u043B\u044F", "callback_data": "week"},
            ],
            [
                {"text": "\U0001F4C6 \u041F\u0440\u0435\u0434\u044B\u0434\u0443\u0449\u0430\u044F \u043D\u0435\u0434\u0435\u043B\u044F", "callback_data": "previous_week"},
                {"text": "♾ За всё время", "callback_data": "all"},
            ],
            [{"text": "\U0001F9EA \u0421\u043A\u0440\u0438\u043F\u0442\u044B", "callback_data": "scripts_menu"}],
            [{"text": "\U0001F4C8 2-е сообщение", "callback_data": "second_message_analytics"}],
            [{"text": "\U0001F4DA \u041F\u043E \u0434\u043D\u044F\u043C \u043D\u0435\u0434\u0435\u043B\u0438", "callback_data": "week_days"}],
            [{"text": "\U0001F4CC \u041E\u0442\u0434\u0435\u043B\u044C\u043D\u044B\u0439 \u043F\u0440\u043E\u0435\u043A\u0442", "callback_data": "project_menu"}],
        ]}



    @staticmethod
    def project_menu_keyboard() -> dict:
        return {"inline_keyboard": [
            [{"text": "АЙФОНЫ", "callback_data": "project:apple"}, {"text": "ГОСЗАКУПКИ", "callback_data": "project:tenders"}],
            [{"text": "МОРЕПРОДУКТЫ", "callback_data": "project:seafood"}, {"text": "ТРЕЙДИНГ", "callback_data": "project:trading"}],
            [{"text": "ДМИТРИЙ БЛОГ", "callback_data": "project:blog"}],
            [{"text": "← К отчётам", "callback_data": "reports_menu"}],
        ]}



    @staticmethod
    def project_keyboard(code: str) -> dict:
        return {"inline_keyboard": [
            [{"text": "◀️ Вчера", "callback_data": f"project_report:{code}:day"}, {"text": "\U0001F4C5 \u042D\u0442\u0430 \u043D\u0435\u0434\u0435\u043B\u044F", "callback_data": f"project_report:{code}:week"}],
            [{"text": "\U0001F4C6 \u041F\u0440\u0435\u0434\u044B\u0434\u0443\u0449\u0430\u044F \u043D\u0435\u0434\u0435\u043B\u044F", "callback_data": f"project_report:{code}:previous_week"}, {"text": "♾ За всё время", "callback_data": f"project_report:{code}:all"}],
            [{"text": "\U0001F4DA \u041F\u043E \u0434\u043D\u044F\u043C \u043D\u0435\u0434\u0435\u043B\u0438", "callback_data": f"project_week_days:{code}"}],
            [{"text": "← К проектам", "callback_data": "project_menu"}],
        ]}



    @staticmethod
    def scripts_keyboard(project: str | None = None) -> dict:
        rows = []
        if project:
            code = {"ГОСЗАКУПКИ": "tenders", "ТРЕЙДИНГ": "trading", "ДМИТРИЙ БЛОГ": "blog"}[project]
            rows.append([{"text": "\U0001F5C2 \u0412\u0441\u0435 \u0441\u043A\u0440\u0438\u043F\u0442\u044B \u0437\u0430 \u0432\u0441\u0451 \u0432\u0440\u0435\u043C\u044F", "callback_data": f"scripts_all_{code}"}])
        rows.append([
            {"text": "Госзакупки", "callback_data": "scripts_tenders"},
            {"text": "Трейдинг", "callback_data": "scripts_trading"},
        ])
        rows.append([{"text": "Дмитрий блог", "callback_data": "scripts_blog"}])
        rows.append([{"text": "← К отчётам", "callback_data": "reports_menu"}])
        return {"inline_keyboard": [
            *rows,
        ]}

    def report_chat_id(self) -> int | None:
        value = self.store.get("report_chat_id")
        return int(value) if value else None

    def is_admin(self, user_id: int) -> bool:
        admins = {int(x) for x in self.store.get("admin_ids").split(",") if x.strip()}
        return user_id in admins

    def has_access(self, user_id: int) -> bool:
        return self.is_admin(user_id) or self.store.has_user(user_id)

    def period(self, kind: str) -> tuple[int, int | None, str]:
        now = dt.datetime.now(self.s.timezone)
        if kind == "day":
            day = now.date() - dt.timedelta(days=1)
            start = dt.datetime.combine(day, dt.time.min, self.s.timezone); end = start + dt.timedelta(days=1)
            return int(start.timestamp()), int(end.timestamp()), day.strftime("%d.%m.%Y")
        if kind == "week":
            start = dt.datetime.combine(now.date() - dt.timedelta(days=now.weekday()), dt.time.min, self.s.timezone)
            return int(start.timestamp()), int(now.timestamp()) + 1, f"{start:%d.%m}–{now:%d.%m.%Y}"
        if kind == "previous_week":
            this_week_start = now.date() - dt.timedelta(days=now.weekday())
            previous_week_start = this_week_start - dt.timedelta(days=7)
            start = dt.datetime.combine(previous_week_start, dt.time.min, self.s.timezone)
            end = dt.datetime.combine(this_week_start, dt.time.min, self.s.timezone)
            return int(start.timestamp()), int(end.timestamp()), f"{previous_week_start:%d.%m}–{(this_week_start - dt.timedelta(days=1)):%d.%m.%Y}"
        if kind == "month":
            start = dt.datetime(now.year, now.month, 1, tzinfo=self.s.timezone)
            return int(start.timestamp()), int(now.timestamp()) + 1, now.strftime("%B %Y")
        return 0, None, "за всё время"

    def format_report(self, kind: str, start: int | None = None, end: int | None = None, label: str | None = None, project_filter: str | None = None) -> str:
        if start is None:
            start, end, label = self.period(kind)
        assert label is not None
        titles = {"day": "Ежедневная статистика", "week": "Статистика за неделю", "previous_week": "Статистика за предыдущую неделю", "month": "Статистика за месяц", "all": "Статистика за всё время"}
        result = [
            f"<b>{titles[kind]}</b>",
            f"Период: {html.escape(label)}",
        ]
        summary = self.metrics.summary(start, end)
        all_time_summary = self.metrics.summary(0, None)
        # A report must have a stable layout.  Zero is operationally useful:
        # it means no activity, not a missing or random metric.
        core_projects = ("АЙФОНЫ", "ДМИТРИЙ БЛОГ", "МОРЕПРОДУКТЫ", "ГОСЗАКУПКИ", "ТРЕЙДИНГ")
        # Historical technical projects are retained in storage, but never leak
        # into the agreed operational report.
        projects = (project_filter,) if project_filter else core_projects
        project_icons = {
            "АЙФОНЫ": "🍏",
            "ГОСЗАКУПКИ": "🏛️",
            "МОРЕПРОДУКТЫ": "🦐",
            "ТРЕЙДИНГ": "📈",
            "ДМИТРИЙ БЛОГ": "📝",
        }
        for project in projects:
            m = summary[project]
            sent, replied = m["sent"], m["replied"]
            conversion = f"{replied / sent * 100:.1f}%" if sent else "—"
            project_lines = [
                f"\n<b>{project_icons.get(project, '📊')} {html.escape(project)}</b>",
                f"• Отправлено: <b>{sent}</b>",
            ]
            if project == "АЙФОНЫ":
                # The installed follow-up service is scoped to the Apple
                # outreach accounts. Its ledger records only successful
                # deliveries, separately from initial outreach.
                followups = self.metrics.followup_summary(start, end)
                project_lines.append(f"• Отправлено дожим: <b>{followups['sent']}</b>")
            project_lines.append(
                f"• Ответили: <b>{replied}</b> ({conversion})",
            )
            result.extend(project_lines)
            def dual_conversion(value: int) -> str:
                from_sent = f"{value / sent * 100:.1f}%" if sent else "—"
                from_replied = f"{value / replied * 100:.1f}%" if replied else "—"
                return f"(из отправленных: {from_sent}, из ответивших: {from_replied})"

            requests = m["requests"]
            if project == "ГОСЗАКУПКИ":
                result.append(f"• 2-е сообщение (ссылка на канал): <b>{m['second_sent']}</b>")
            elif project == "ДМИТРИЙ БЛОГ":
                second_rate = f"{m['second_sent'] / replied * 100:.1f}%" if replied else "—"
                result.append(f"• 2-е сообщение (ссылка на канал): <b>{m['second_sent']}</b> (из ответивших: {second_rate})")
            if project == "ДМИТРИЙ БЛОГ":
                try:
                    historical_adjustment = int(self.store.get("dmitry_blog_historical_requests_adjustment", "0"))
                except ValueError:
                    historical_adjustment = 0
                if kind == "all" and historical_adjustment:
                    result.append(f"• Заявки, зарегистрированные в журнале: <b>{requests}</b>")
                else:
                    result.append(f"• Заявки за период: <b>{requests}</b> {dual_conversion(requests)}")
                total_requests = all_time_summary[project]["requests"] + historical_adjustment
                result.append(f"• Всего заявок: <b>{total_requests}</b>")
            else:
                result.append(f"• Заявки в канал: <b>{requests}</b> {dual_conversion(requests)}")
            if project == "ГОСЗАКУПКИ":
                third_sent = m["third_sent"]
                from_sent = f"{third_sent / sent * 100:.1f}%" if sent else "—"
                from_replied = f"{third_sent / replied * 100:.1f}%" if replied else "—"
                from_second = f"{third_sent / m['second_sent'] * 100:.1f}%" if m["second_sent"] else "—"
                result.append(
                    f"• 3-е сообщение (ссылка на сайт): <b>{third_sent}</b> "
                    f"(из отправленных: {from_sent}, из ответивших: {from_replied}, из 2-го сообщения: {from_second})"
                )
            if project == "АЙФОНЫ":
                result.append(f"• Лиды Маши: <b>{m['leads']}</b> {dual_conversion(m['leads'])}")
                lead_matches = self.metrics.apple_lead_match_stats(start, end)
                result.append(
                    f"• Уникальные совпадения по username: <b>{lead_matches['matched']}</b>, "
                    f"не найдены среди всех лидов Маши: <b>{lead_matches['outside']}</b>"
                )
                if lead_matches["unmatched"]:
                    result.append(f"• Заявки без username, не сверены: <b>{lead_matches['unmatched']}</b>")
                result.append("<b>Базы Apple</b>")
                for base, values in self.metrics.apple_base_summary(start, end).items():
                    rate = f"{values['replied'] / values['sent'] * 100:.1f}%" if values["sent"] else "—"
                    result.append(f"• {base}: отправлено <b>{values['sent']}</b>, ответили <b>{values['replied']}</b> ({rate})")
            elif m["leads"]:
                result.append(f"• Лиды: <b>{m['leads']}</b> {dual_conversion(m['leads'])}")
        return "\n".join(result)

    def format_second_message_analytics(self) -> str:
        rows = self.metrics.second_message_variants("ГОСЗАКУПКИ")
        header = "<b>\U0001F4C8 Конверсия 2-го сообщения · ГОСЗАКУПКИ</b>\nНовые отправки с 25.08.2026. Заявка привязывается к тому же получателю и кампании."
        if not rows:
            return header + "\n\nПока недостаточно новых размеченных отправок для сравнения вариантов."
        result = [header]
        for index, row in enumerate(rows, 1):
            rate = row["requests"] / row["sent"] * 100 if row["sent"] else 0
            text = html.escape(" ".join(row["text"].split()))
            result.append(f"\n<b>{index}. {row['requests']}/{row['sent']} заявок — {rate:.1f}%</b>\n<blockquote>{text}</blockquote>")
        return "\n".join(result)

    def script_pages(self, project: str, active_only: bool = True) -> list[str]:
        rows = [row for row in self.metrics.outreach_texts(project, 0, None) if row["script_label"] != "[медиа/файл]"]
        cutoff = int((dt.datetime.now(self.s.timezone) - dt.timedelta(days=2)).timestamp())
        if active_only:
            rows = [row for row in rows if row["last_sent_at"] >= cutoff]
        period_label = "Актуальные: отправлялись за последние 2 дня" if active_only else "Все скрипты за всё время, включая отключённые"
        if not rows:
            return [f"<b>Скрипты · {html.escape(project)}</b>\n{period_label}\n\nПока нет подходящих скриптов."]
        items = [{"text": row["script_label"], "sent": row["sent"], "replied": row["replied"], "rate": row["replied"] / row["sent"] if row["sent"] else 0} for row in rows]
        total_sent, total_replied = sum(x["sent"] for x in items), sum(x["replied"] for x in items)
        average = total_replied / total_sent if total_sent else 0
        reliable = [x for x in items if x["sent"] >= 20]
        # Absolute labels are misleading: what is strong differs by project.
        # A top script must beat the overall rate by 25% (or by 5 pp); a
        # working script is at/above average, and the remainder needs work.
        top_threshold = max(average * 1.25, average + 0.05)
        top = sorted([x for x in reliable if x["rate"] >= top_threshold], key=lambda x: (x["rate"], x["sent"]), reverse=True)
        working = sorted([x for x in reliable if average <= x["rate"] < top_threshold], key=lambda x: (x["rate"], x["sent"]), reverse=True)
        weak = sorted([x for x in reliable if x["rate"] < average and x["replied"] > 0], key=lambda x: (x["rate"], -x["sent"]))
        tests = sorted([x for x in items if x["sent"] < 20 and x["replied"] > 0], key=lambda x: (x["rate"], x["sent"]), reverse=True)
        zero_tests = [x for x in items if x["sent"] < 20 and not x["replied"]]
        zero_reliable = [x for x in reliable if not x["replied"]]
        def show(index: int, item: dict) -> str:
            text = html.escape(" ".join(item["text"].split()))
            return f"<b>{index}. {item['replied']}/{item['sent']} ответов — {item['rate'] * 100:.1f}%</b>\n<blockquote>{text}</blockquote>"
        header = "\n".join([
            f"<b>Скрипты · {html.escape(project)}</b>",
            period_label,
            f"Всего по текстовым скриптам: <b>{total_replied}/{total_sent}</b> ответов · <b>{average * 100:.1f}%</b>",
        ])
        sections: list[tuple[str, list[dict]]] = [
            (f"🏆 <b>Топовые скрипты</b>\nВыборка от 20 отправок; конверсия от {top_threshold * 100:.1f}% — минимум на 25% выше средней.", top),
            ("✅ <b>Рабочие скрипты</b>\nВыборка от 20 отправок; конверсия на уровне или выше средней.", working),
            ("📉 <b>Нужна доработка</b>\nВыборка от 20 отправок, но конверсия ниже средней.", weak),
            (f"🧪 <b>Ещё тестируем</b>\n{len(tests)} скриптов с ответами, но выборка меньше 20 отправок — выводы делать рано.", tests),
        ]
        blocks = [header]
        for title, group in sections:
            if not group: continue
            blocks.append(title)
            blocks.extend(show(index, item) for index, item in enumerate(group, 1))
        zero_items = sorted(zero_reliable + zero_tests, key=lambda x: x["sent"], reverse=True)
        zero_count = len(zero_items)
        if zero_count:
            zero_sent = sum(item["sent"] for item in zero_items)
            if active_only:
                blocks.append(f"⚪ <b>Без ответов</b>\n{zero_count} актуальных скриптов: 0 ответов на {zero_sent} отправок.")
                blocks.extend(show(index, item) for index, item in enumerate(zero_items, 1))
            else:
                blocks.append(f"⚪ <b>Без ответов</b>\nЗа всё время: <b>{zero_count}</b> скриптов не получили ответов после <b>{zero_sent}</b> отправок. Тексты этих скриптов скрыты, чтобы отчёт оставался компактным.")
        pages: list[str] = []
        current = ""
        for block in blocks:
            candidate = f"{current}\n\n{block}" if current else block
            if current and len(candidate) > 3500:
                pages.append(current); current = block
            else:
                current = candidate
        if current: pages.append(current)
        return pages

    def format_scripts(self, project: str, active_only: bool = True) -> str:
        """Kept for diagnostic use; Telegram sends all pages via the callback."""
        return "\n\n".join(self.script_pages(project, active_only))

    async def send_script_pages(self, chat_id: int, project: str, active_only: bool = True) -> None:
        pages = self.script_pages(project, active_only)
        for page_number, page in enumerate(pages, 1):
            suffix = f"\n\n<i>Страница {page_number}/{len(pages)}</i>" if len(pages) > 1 else ""
            await self.tg.send(chat_id, page + suffix, self.scripts_keyboard(project))

    async def send_report(self, kind: str, start: int | None = None, end: int | None = None, label: str | None = None, period_start: str | None = None) -> bool:
        recipients = self.store.user_ids()
        if not recipients:
            log.warning("Report not sent: there are no subscribed users")
            return False
        text = self.format_report(kind, start, end, label)
        delivered_to_all = True
        for user_id in recipients:
            if period_start and self.store.report_delivered(kind, period_start, user_id):
                continue
            try:
                message = await self.tg.send(user_id, text, self.keyboard())
                if period_start:
                    self.store.mark_report_delivered(kind, period_start, user_id)
                    self.store.save_report_message(kind, period_start, user_id, message["message_id"])
            except Exception:
                delivered_to_all = False
                log.exception("Could not send %s report to user %s", kind, user_id)
        return delivered_to_all

    async def send_tenders_report(self, kind: str, start: int | None = None, end: int | None = None, label: str | None = None) -> bool:
        chat_ids = self.store.tenders_report_chat_ids()
        if not chat_ids:
            return False
        delivered_to_all = True
        for chat_id in chat_ids:
            try:
                await self.tg.send(
                    chat_id,
                    self.format_report(kind, start, end, label, "ГОСЗАКУПКИ"),
                    self.project_keyboard("tenders"),
                )
            except Exception:
                delivered_to_all = False
                log.exception("Could not send tenders %s report to chat %s", kind, chat_id)
        return delivered_to_all

    async def send_blog_report(self, kind: str, start: int | None = None, end: int | None = None, label: str | None = None, period_start: str | None = None) -> bool:
        chat_ids = self.store.blog_report_chat_ids()
        if not chat_ids:
            return False
        delivered = True
        for chat_id in chat_ids:
            try:
                message = await self.tg.send(chat_id, self.format_report(kind, start, end, label, "ДМИТРИЙ БЛОГ"))
                if period_start:
                    self.store.save_report_message(kind, period_start, chat_id, message["message_id"])
            except Exception:
                delivered = False
                log.exception("Could not send blog %s report to chat %s", kind, chat_id)
        return delivered

    async def send_tenders_weekly_history(self, chat_id: int) -> None:
        first_week = dt.date(2026, 7, 13)
        current_week = dt.datetime.now(self.s.timezone).date() - dt.timedelta(days=dt.datetime.now(self.s.timezone).weekday())
        week_start = first_week
        while week_start < current_week:
            start = dt.datetime.combine(week_start, dt.time.min, self.s.timezone)
            end = start + dt.timedelta(days=7)
            label = f"{week_start:%d.%m}–{(week_start + dt.timedelta(days=6)):%d.%m.%Y}"
            await self.tg.send(chat_id, self.format_report("week", int(start.timestamp()), int(end.timestamp()), label, "ГОСЗАКУПКИ"), self.project_keyboard("tenders"))
            week_start += dt.timedelta(days=7)


    async def send_week_days(self, chat_id: int, project_filter: str | None = None, keyboard: dict | None = None) -> None:
        now = dt.datetime.now(self.s.timezone)
        monday = now.date() - dt.timedelta(days=now.weekday())
        last_day = now.date() - dt.timedelta(days=1)
        if last_day < monday:
            await self.tg.send(chat_id, "За текущую неделю ещё нет завершённых дней.", keyboard or self.keyboard())
            return
        for offset in range((last_day - monday).days + 1):
            day = monday + dt.timedelta(days=offset)
            start = dt.datetime.combine(day, dt.time.min, self.s.timezone)
            end = start + dt.timedelta(days=1)
            await self.tg.send(chat_id, self.format_report("day", int(start.timestamp()), int(end.timestamp()), day.strftime("%d.%m.%Y"), project_filter), keyboard or self.keyboard())

    async def handle_update(self, update: dict[str, Any]) -> None:
        message = update.get("message")
        callback = update.get("callback_query")
        channel_post = update.get("channel_post")
        join_request = update.get("chat_join_request")
        if join_request:
            await self.handle_dmitry_blog_join_request(join_request)
            return
        if channel_post:
            await self.handle_lead_channel_post(channel_post)
            return
        if callback:
            user_id = callback["from"]["id"]
            if not self.has_access(user_id): return
            await self.tg.call("answerCallbackQuery", callback_query_id=callback["id"])
            chat_id = callback["message"]["chat"]["id"]
            action = callback["data"]
            project_codes = {"apple": "АЙФОНЫ", "tenders": "ГОСЗАКУПКИ", "seafood": "МОРЕПРОДУКТЫ", "trading": "ТРЕЙДИНГ", "blog": "ДМИТРИЙ БЛОГ"}
            if action == "week_days":
                await self.send_week_days(chat_id)
            elif action == "project_menu":
                await self.tg.send(chat_id, "<b>Отдельный проект</b>\nВыберите проект.", self.project_menu_keyboard())
            elif action.startswith("project:"):
                code = action.split(":", 1)[1]
                project = project_codes.get(code)
                if project:
                    await self.tg.send(chat_id, f"<b>{html.escape(project)}</b>\nВыберите период.", self.project_keyboard(code))
            elif action.startswith("project_report:"):
                _, code, kind = action.split(":", 2)
                project = project_codes.get(code)
                if project and kind in {"day", "week", "previous_week", "all"}:
                    await self.tg.send(chat_id, self.format_report(kind, project_filter=project), self.project_keyboard(code))
            elif action.startswith("project_week_days:"):
                code = action.split(":", 1)[1]
                project = project_codes.get(code)
                if project:
                    await self.send_week_days(chat_id, project, self.project_keyboard(code))
            elif action == "second_message_analytics":
                await self.tg.send(chat_id, self.format_second_message_analytics(), self.keyboard())
            elif action == "scripts_menu":
                await self.tg.send(chat_id, "<b>Скрипты</b>\nВыбери проект. Сначала показываются только актуальные варианты за последние 2 дня.", self.scripts_keyboard())
            elif action == "scripts_tenders":
                await self.send_script_pages(chat_id, "ГОСЗАКУПКИ")
            elif action == "scripts_trading":
                await self.send_script_pages(chat_id, "ТРЕЙДИНГ")
            elif action == "scripts_blog":
                await self.send_script_pages(chat_id, "ДМИТРИЙ БЛОГ")
            elif action == "scripts_all_tenders":
                await self.send_script_pages(chat_id, "ГОСЗАКУПКИ", active_only=False)
            elif action == "scripts_all_trading":
                await self.send_script_pages(chat_id, "ТРЕЙДИНГ", active_only=False)
            elif action == "scripts_all_blog":
                await self.send_script_pages(chat_id, "ДМИТРИЙ БЛОГ", active_only=False)
            elif action == "reports_menu":
                await self.tg.send(chat_id, "Отчёты по проектам.", self.keyboard())
            else:
                await self.tg.send(chat_id, self.format_report(action), self.keyboard())
            return
        if not message or not message.get("text"): return
        user_id, chat_id = message["from"]["id"], message["chat"]["id"]
        command = message["text"].split()[0].split("@", 1)[0]
        if command == "/start":
            self.store.add_user(user_id)
            if not self.store.get("admin_ids"):
                self.store.set("admin_ids", str(user_id)); self.store.set("report_chat_id", str(chat_id))
                await self.tg.send(chat_id, "✅ Этот чат назначен для автоматических отчётов. Доступ получили вы.", self.keyboard()); return
            await self.tg.send(chat_id, "✅ Доступ к статистике открыт.", self.keyboard())
            return
        if command == "/connect_tenders_reports":
            added = self.store.add_tenders_report_chat(chat_id)
            if added:
                await self.tg.send(chat_id, "✅ Чат подключён к отчётам «Госзакупки». Сейчас загружу недельные отчёты за завершённые периоды; далее будут ежедневные, еженедельные и ежемесячные отчёты.")
                await self.send_tenders_weekly_history(chat_id)
            else:
                await self.tg.send(chat_id, "ℹ️ Этот чат уже подключён к отчётам «Госзакупки».")
            return
        if command == "/connect_dmitry_blog_reports":
            added = self.store.add_blog_report_chat(chat_id)
            await self.tg.send(chat_id, "✅ Чат подключён к ежедневным, еженедельным и ежемесячным отчётам «Дмитрий блог»." if added else "ℹ️ Этот чат уже подключён к отчётам «Дмитрий блог».")
            return
        if not self.has_access(user_id): return
        if command == "/set_report_chat":
            if self.is_admin(user_id):
                self.store.set("report_chat_id", str(chat_id)); await self.tg.send(chat_id, "✅ Этот чат назначен для автоматических отчётов.", self.keyboard())
        elif command == "/yesterday": await self.tg.send(chat_id, self.format_report("day"), self.keyboard())
        elif command == "/week": await self.tg.send(chat_id, self.format_report("week"), self.keyboard())
        elif command == "/previous_week": await self.tg.send(chat_id, self.format_report("previous_week"), self.keyboard())
        elif command == "/all": await self.tg.send(chat_id, self.format_report("all"), self.keyboard())
        elif command == "/help": await self.tg.send(chat_id, "Команды: /yesterday, /week, /previous_week, /all, /set_report_chat", self.keyboard())

    async def handle_lead_channel_post(self, post: dict[str, Any]) -> None:
        """Count every photo post in the configured Apple or seafood lead channel."""
        chat_id = post["chat"]["id"]
        apple_channel = self.store.apple_leads_channel_id()
        seafood_channel = self.store.seafood_leads_channel_id()
        if chat_id == apple_channel:
            project, source = "АЙФОНЫ", "masha_apple_leads"
        elif chat_id == seafood_channel:
            project, source = "МОРЕПРОДУКТЫ", "seafood_channel_leads"
        elif seafood_channel is None and apple_channel is not None:
            self.store.set_seafood_leads_channel_id(chat_id)
            project, source = "МОРЕПРОДУКТЫ", "seafood_channel_leads"
            log.info("Seafood lead channel bound to %s", chat_id)
        else:
            return
        if not post.get("photo"):
            return
        inserted = self.store.add_event({
            "source": source,
            "event_key": f"channel-lead:{project}:{chat_id}:{post['message_id']}",
            "project": project,
            "event_type": "lead",
            "occurred_at": post.get("date", int(dt.datetime.now(dt.timezone.utc).timestamp())),
            "payload": {
                "channel_id": chat_id,
                "message_id": post["message_id"],
                "username": username_from_post(post),
            },
        })
        log.info("%s photo lead %s: post=%s", project, "recorded" if inserted else "already present", post["message_id"])

    async def handle_dmitry_blog_join_request(self, request: dict[str, Any]) -> None:
        """Count Blog join requests only; this handler never approves them."""
        configured_chat_id = self.store.get("dmitry_blog_requests_channel_id").strip()
        chat_id = request["chat"]["id"]
        # The analytics bot is only added to the Blog's applicant channel.
        # Bind it silently on the first join request: this avoids requiring a
        # setup post in the channel and never sends anything there.
        if not configured_chat_id:
            self.store.set("dmitry_blog_requests_channel_id", str(chat_id))
            log.info("Dmitry Blog request channel bound silently to %s", chat_id)
        elif int(configured_chat_id) != chat_id:
            return
        applicant = request["from"]
        user_id = applicant["id"]
        inserted = self.store.add_event({
            "source": "dmitry_blog_join_requests",
            "event_key": f"join-request:dmitry-blog:{chat_id}:{user_id}",
            "project": "ДМИТРИЙ БЛОГ",
            "event_type": "join_request",
            "occurred_at": request.get("date", int(dt.datetime.now(dt.timezone.utc).timestamp())),
            "payload": {
                "channel_id": chat_id,
                "user_id": user_id,
                "username": applicant.get("username", ""),
            },
        })
        log.info("Dmitry Blog join request %s: user=%s", "recorded" if inserted else "already present", user_id)

    async def polling_loop(self):
        while True:
            try:
                for update in await self.tg.poll(self.offset):
                    self.offset = update["update_id"] + 1; self.store.set("telegram_offset", str(self.offset))
                    await self.handle_update(update)
            except asyncio.CancelledError: raise
            except Exception:
                log.exception("Polling failed"); await asyncio.sleep(5)

    async def schedule_loop(self):
        while True:
            now = dt.datetime.now(self.s.timezone)
            # Daily reports close the current calendar day, instead of being
            # sent just after midnight for the prior day.  This makes their
            # period unambiguous and keeps the Blog's daily and total request
            # figures side by side.
            if now.time() >= dt.time(23, 59):
                today = now.date().isoformat()
                start = dt.datetime.combine(now.date(), dt.time.min, self.s.timezone)
                end = int(now.timestamp()) + 1
                if not self.store.report_sent("day", today):
                    if await self.send_report("day", int(start.timestamp()), end, now.strftime("%d.%m.%Y"), today):
                        self.store.mark_report_sent("day", today)
                if not self.store.report_sent("dmitry_blog_day", today):
                    if await self.send_blog_report("day", int(start.timestamp()), end, now.strftime("%d.%m.%Y"), today):
                        self.store.mark_report_sent("dmitry_blog_day", today)
            # Retry after midnight if a restart or connection failure skipped 00:01.
            # sent_reports makes each delivery idempotent.
            if now.time() >= dt.time(0, 1):
                yesterday = (now.date() - dt.timedelta(days=1)).isoformat()
                if not self.store.report_sent("tenders_day", yesterday):
                    if await self.send_tenders_report("day"):
                        self.store.mark_report_sent("tenders_day", yesterday)
                if now.weekday() == 0:
                    week_start = now.date() - dt.timedelta(days=7)
                    week_end = dt.datetime.combine(now.date(), dt.time.min, self.s.timezone)
                    week = week_start.isoformat()
                    if not self.store.report_sent("week", week):
                        if await self.send_report("week", int(dt.datetime.combine(week_start, dt.time.min, self.s.timezone).timestamp()), int(week_end.timestamp()), f"{week_start:%d.%m}–{(now.date() - dt.timedelta(days=1)):%d.%m.%Y}", week):
                            self.store.mark_report_sent("week", week)
                    if not self.store.report_sent("tenders_week", week):
                        if await self.send_tenders_report("week", int(dt.datetime.combine(week_start, dt.time.min, self.s.timezone).timestamp()), int(week_end.timestamp()), f"{week_start:%d.%m}–{(now.date() - dt.timedelta(days=1)):%d.%m.%Y}"):
                            self.store.mark_report_sent("tenders_week", week)
                if now.day == 1:
                    month_end = dt.datetime(now.year, now.month, 1, tzinfo=self.s.timezone)
                    previous_month_start = (month_end - dt.timedelta(days=1)).replace(day=1)
                    previous_month = previous_month_start.strftime("%Y-%m")
                    if not self.store.report_sent("tenders_month", previous_month):
                        if await self.send_tenders_report(
                            "month",
                            int(previous_month_start.timestamp()),
                            int(month_end.timestamp()),
                            previous_month_start.strftime("%B %Y"),
                        ):
                            self.store.mark_report_sent("tenders_month", previous_month)
                    if not self.store.report_sent("month", previous_month):
                        if await self.send_report("month", int(previous_month_start.timestamp()), int(month_end.timestamp()), previous_month_start.strftime("%B %Y"), previous_month):
                            self.store.mark_report_sent("month", previous_month)
            # Blog reports deliberately close at the end of Sunday and at the
            # end of the calendar month, so the final day is included.
            if now.weekday() == 6 and now.time() >= dt.time(23, 59):
                week_start = now.date() - dt.timedelta(days=6)
                week_key = week_start.isoformat()
                if not self.store.report_sent("dmitry_blog_week", week_key):
                    start = dt.datetime.combine(week_start, dt.time.min, self.s.timezone)
                    if await self.send_blog_report("week", int(start.timestamp()), int(now.timestamp()) + 1, f"{week_start:%d.%m}–{now.date():%d.%m.%Y}", week_key):
                        self.store.mark_report_sent("dmitry_blog_week", week_key)
            tomorrow = now.date() + dt.timedelta(days=1)
            if tomorrow.month != now.month and now.time() >= dt.time(23, 59):
                month_key = now.strftime("%Y-%m")
                if not self.store.report_sent("dmitry_blog_month", month_key):
                    start = dt.datetime(now.year, now.month, 1, tzinfo=self.s.timezone)
                    if await self.send_blog_report("month", int(start.timestamp()), int(now.timestamp()) + 1, now.strftime("%B %Y"), month_key):
                        self.store.mark_report_sent("dmitry_blog_month", month_key)
            await asyncio.sleep(30)

    async def ingest(self, request: web.Request) -> web.Response:
        if self.s.ingest_token and request.headers.get("Authorization") != f"Bearer {self.s.ingest_token}":
            log.warning("Rejected analytics event: invalid or missing bearer token from %s", request.remote)
            raise web.HTTPUnauthorized()
        payload = await request.json()
        required = {"source", "event_key", "project", "event_type"}
        if not required <= payload.keys(): raise web.HTTPBadRequest(text="Missing event fields")
        payload["occurred_at"] = int(payload.get("occurred_at", dt.datetime.now(dt.timezone.utc).timestamp()))
        inserted = self.store.add_event(payload)
        return web.json_response({"ok": True, "inserted": inserted})

    async def run(self):
        await self.tg.start()
        app = web.Application(); app.router.add_post("/events", self.ingest)
        runner = web.AppRunner(app); await runner.setup()
        site = web.TCPSite(runner, self.s.api_host, self.s.api_port); await site.start()
        log.info("Analytics ingestion listening on %s:%s", self.s.api_host, self.s.api_port)
        try: await asyncio.gather(self.polling_loop(), self.schedule_loop())
        finally: await runner.cleanup(); await self.tg.close()


if __name__ == "__main__":
    asyncio.run(AnalyticsBot(Settings.from_env()).run())
