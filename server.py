#!/usr/bin/env python3
"""
SkyChat server — Python 3 stdlib only (no pip dependencies).
Run:  python3 server.py   (listens on port 8000, or $PORT)
DB:   sky_chat.db (SQLite, created next to this file)

API (all JSON):
  GET  /api/health
  POST /api/register  {name, username, phone, password}
  POST /api/login     {identifier, password}  (identifier = username or phone)
  GET  /api/users     (auth)
  GET  /api/messages?peerId=<id>  (auth)
  POST /api/messages  (auth) {peerId, type, text, imageBase64, voiceDurationSec, replyToId}
  POST /api/messages/<id>/reaction  (auth) {reaction}
  GET  /api/threads   (auth)
Auth: Authorization: Bearer <token>
"""
import hashlib
import json
import os
import secrets
import sqlite3
import uuid
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "sky_chat.db")
PORT = int(os.environ.get("PORT", "8000"))
MAX_IMAGE_B64 = 3 * 1024 * 1024  # ~3 MB of base64 text
MAX_BODY = 5 * 1024 * 1024
VALID_TYPES = {"text", "image", "hiddenImage", "voice"}
VALID_REACTIONS = {"❤️", "😂", "👍", "🔥", "😮"}


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db():
    conn = get_db()
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS users (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            username TEXT NOT NULL UNIQUE,
            phone TEXT NOT NULL UNIQUE,
            bio TEXT NOT NULL DEFAULT 'متاح للمحادثة على SkyChat ✨',
            password_hash TEXT NOT NULL,
            salt TEXT NOT NULL,
            is_online INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS tokens (
            token TEXT PRIMARY KEY,
            user_id TEXT NOT NULL REFERENCES users(id),
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS messages (
            id TEXT PRIMARY KEY,
            sender_id TEXT NOT NULL REFERENCES users(id),
            receiver_id TEXT NOT NULL REFERENCES users(id),
            type TEXT NOT NULL,
            text TEXT NOT NULL DEFAULT '',
            image_base64 TEXT,
            voice_duration_sec INTEGER,
            reply_to_id TEXT,
            reaction TEXT,
            view_count INTEGER NOT NULL DEFAULT 0,
            is_read INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_messages_pair
            ON messages(sender_id, receiver_id, created_at);
        CREATE INDEX IF NOT EXISTS idx_messages_receiver
            ON messages(receiver_id, created_at);
        """
    )
    conn.commit()
    conn.close()


def hash_password(password, salt_hex):
    salt = bytes.fromhex(salt_hex)
    return hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 200_000).hex()


def public_user(row):
    return {
        "id": row["id"],
        "name": row["name"],
        "username": row["username"],
        "phone": row["phone"],
        "bio": row["bio"],
        "isOnline": bool(row["is_online"]),
    }


def public_message(row):
    return {
        "id": row["id"],
        "senderId": row["sender_id"],
        "receiverId": row["receiver_id"],
        "type": row["type"],
        "text": row["text"] or "",
        "imageBase64": row["image_base64"],
        "voiceDurationSec": row["voice_duration_sec"],
        "replyToId": row["reply_to_id"],
        "reaction": row["reaction"],
        "viewCount": row["view_count"],
        "isRead": bool(row["is_read"]),
        "timestamp": row["created_at"],
    }


class Handler(BaseHTTPRequestHandler):
    # Silence default logging to stderr per-request (keep it quiet but useful)
    def log_message(self, fmt, *args):
        print("%s - %s" % (self.address_string(), fmt % args), flush=True)

    # ---------- helpers ----------
    def _send_json(self, status, obj):
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.end_headers()
        self.wfile.write(body)

    def _error(self, status, msg):
        self._send_json(status, {"error": msg})

    def _read_json(self):
        length = int(self.headers.get("Content-Length") or 0)
        if length <= 0:
            return {}
        if length > MAX_BODY:
            raise ValueError("body too large")
        raw = self.rfile.read(length)
        if not raw:
            return {}
        return json.loads(raw.decode("utf-8"))

    def _auth_user_id(self):
        auth = self.headers.get("Authorization") or ""
        if not auth.startswith("Bearer "):
            return None
        token = auth[len("Bearer "):].strip()
        if not token:
            return None
        conn = get_db()
        try:
            row = conn.execute(
                "SELECT user_id FROM tokens WHERE token = ?", (token,)
            ).fetchone()
            return row["user_id"] if row else None
        finally:
            conn.close()

    def _require_auth(self):
        uid = self._auth_user_id()
        if uid is None:
            self._error(401, "unauthorized")
            return None
        return uid

    # ---------- HTTP verbs ----------
    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        qs = parse_qs(parsed.query)

        if path == "/api/health":
            return self._send_json(200, {"ok": True})

        uid = self._require_auth()
        if uid is None:
            return

        if path == "/api/users":
            conn = get_db()
            try:
                rows = conn.execute(
                    "SELECT * FROM users WHERE id != ? ORDER BY name ASC", (uid,)
                ).fetchall()
                return self._send_json(200, {"users": [public_user(r) for r in rows]})
            finally:
                conn.close()

        if path == "/api/messages":
            peer_id = (qs.get("peerId") or [""])[0].strip()
            if not peer_id:
                return self._error(400, "peerId is required")
            conn = get_db()
            try:
                peer = conn.execute("SELECT id FROM users WHERE id = ?", (peer_id,)).fetchone()
                if peer is None:
                    return self._error(404, "peer not found")
                rows = conn.execute(
                    """SELECT * FROM messages
                       WHERE (sender_id = ? AND receiver_id = ?)
                          OR (sender_id = ? AND receiver_id = ?)
                       ORDER BY created_at ASC, rowid ASC""",
                    (uid, peer_id, peer_id, uid),
                ).fetchall()
                # Mark messages sent TO the requester as read
                conn.execute(
                    "UPDATE messages SET is_read = 1 WHERE receiver_id = ? AND sender_id = ? AND is_read = 0",
                    (uid, peer_id),
                )
                conn.commit()
                return self._send_json(200, {"messages": [public_message(r) for r in rows]})
            finally:
                conn.close()

        if path == "/api/threads":
            conn = get_db()
            try:
                # All peers the user has exchanged messages with, plus
                # the latest message per peer.
                rows = conn.execute(
                    """SELECT
                         CASE WHEN sender_id = ? THEN receiver_id ELSE sender_id END AS peer_id,
                         MAX(created_at) AS last_at
                       FROM messages WHERE sender_id = ? OR receiver_id = ?
                       GROUP BY peer_id ORDER BY last_at DESC""",
                    (uid, uid, uid),
                ).fetchall()
                threads = []
                for r in rows:
                    peer = conn.execute("SELECT * FROM users WHERE id = ?", (r["peer_id"],)).fetchone()
                    if peer is None:
                        continue
                    last = conn.execute(
                        """SELECT * FROM messages
                           WHERE (sender_id = ? AND receiver_id = ?)
                              OR (sender_id = ? AND receiver_id = ?)
                           ORDER BY created_at DESC, rowid DESC LIMIT 1""",
                        (uid, r["peer_id"], r["peer_id"], uid),
                    ).fetchone()
                    threads.append({
                        "peer": public_user(peer),
                        "lastMessage": public_message(last) if last else None,
                        "lastMessageAt": r["last_at"],
                    })
                return self._send_json(200, {"threads": threads})
            finally:
                conn.close()

        return self._error(404, "not found")

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path

        try:
            body = self._read_json()
        except ValueError:
            return self._error(413, "body too large")
        except Exception:
            return self._error(400, "invalid JSON")
        if not isinstance(body, dict):
            return self._error(400, "invalid JSON")

        # ---- public endpoints ----
        if path == "/api/register":
            name = str(body.get("name") or "").strip()
            username = str(body.get("username") or "").strip().replace("@", "").lower()
            phone = str(body.get("phone") or "").strip()
            password = str(body.get("password") or "")
            if not name or not username or not phone or not password:
                return self._error(400, "name, username, phone and password are required")
            if len(password) < 3:
                return self._error(400, "password is too short")
            if len(username) > 50 or len(phone) > 30 or len(name) > 100:
                return self._error(400, "field too long")
            conn = get_db()
            try:
                exists = conn.execute(
                    "SELECT id FROM users WHERE username = ? OR phone = ?",
                    (username, phone),
                ).fetchone()
                if exists:
                    return self._error(400, "username or phone already registered")
                user_id = uuid.uuid4().hex[:16]
                salt = secrets.token_hex(16)
                pw_hash = hash_password(password, salt)
                conn.execute(
                    """INSERT INTO users (id, name, username, phone, bio,
                                          password_hash, salt, is_online, created_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?)""",
                    (user_id, name, username, phone,
                     "متاح للمحادثة على SkyChat ✨", pw_hash, salt, now_iso()),
                )
                token = secrets.token_hex(32)
                conn.execute(
                    "INSERT INTO tokens (token, user_id, created_at) VALUES (?, ?, ?)",
                    (token, user_id, now_iso()),
                )
                conn.commit()
                row = conn.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
                return self._send_json(201, {"user": public_user(row), "token": token})
            except sqlite3.IntegrityError:
                return self._error(400, "username or phone already registered")
            finally:
                conn.close()

        if path == "/api/login":
            identifier = str(body.get("identifier") or "").strip().replace("@", "").lower()
            password = str(body.get("password") or "")
            if not identifier or not password:
                return self._error(400, "identifier and password are required")
            conn = get_db()
            try:
                row = conn.execute(
                    "SELECT * FROM users WHERE username = ? OR phone = ?",
                    (identifier, identifier),
                ).fetchone()
                if row is None:
                    return self._error(401, "invalid credentials")
                if hash_password(password, row["salt"]) != row["password_hash"]:
                    return self._error(401, "invalid credentials")
                conn.execute("UPDATE users SET is_online = 1 WHERE id = ?", (row["id"],))
                token = secrets.token_hex(32)
                conn.execute(
                    "INSERT INTO tokens (token, user_id, created_at) VALUES (?, ?, ?)",
                    (token, row["id"], now_iso()),
                )
                conn.commit()
                return self._send_json(200, {"user": public_user(row), "token": token})
            finally:
                conn.close()

        # ---- authenticated endpoints ----
        uid = self._require_auth()
        if uid is None:
            return

        if path == "/api/messages":
            peer_id = str(body.get("peerId") or "").strip()
            mtype = str(body.get("type") or "text").strip()
            text = str(body.get("text") or "")
            image_b64 = body.get("imageBase64")
            voice_sec = body.get("voiceDurationSec")
            reply_to = body.get("replyToId")
            if not peer_id:
                return self._error(400, "peerId is required")
            if mtype not in VALID_TYPES:
                return self._error(400, "invalid message type")
            if mtype == "text" and not text.strip():
                return self._error(400, "text message cannot be empty")
            if mtype in ("image", "hiddenImage"):
                if not image_b64 or not isinstance(image_b64, str):
                    return self._error(400, "imageBase64 is required for image messages")
                if len(image_b64) > MAX_IMAGE_B64:
                    return self._error(413, "image too large (max ~3MB base64)")
            if mtype == "voice":
                try:
                    voice_sec = int(voice_sec) if voice_sec is not None else 1
                except (TypeError, ValueError):
                    voice_sec = 1
                voice_sec = max(1, min(voice_sec, 3600))
            else:
                voice_sec = None
            if reply_to is not None:
                reply_to = str(reply_to)
            conn = get_db()
            try:
                peer = conn.execute("SELECT id FROM users WHERE id = ?", (peer_id,)).fetchone()
                if peer is None:
                    return self._error(404, "peer not found")
                if peer_id == uid:
                    return self._error(400, "cannot message yourself")
                if reply_to:
                    ok = conn.execute(
                        """SELECT id FROM messages WHERE id = ?
                           AND ((sender_id = ? AND receiver_id = ?)
                             OR (sender_id = ? AND receiver_id = ?))""",
                        (reply_to, uid, peer_id, peer_id, uid),
                    ).fetchone()
                    if ok is None:
                        return self._error(400, "replyToId not found in this thread")
                msg_id = uuid.uuid4().hex[:20]
                created = now_iso()
                conn.execute(
                    """INSERT INTO messages (id, sender_id, receiver_id, type, text,
                                             image_base64, voice_duration_sec,
                                             reply_to_id, reaction, view_count, is_read, created_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, 0, 0, ?)""",
                    (msg_id, uid, peer_id, mtype, text,
                     image_b64 if mtype in ("image", "hiddenImage") else None,
                     voice_sec, reply_to, created),
                )
                conn.commit()
                row = conn.execute("SELECT * FROM messages WHERE id = ?", (msg_id,)).fetchone()
                return self._send_json(201, {"message": public_message(row)})
            finally:
                conn.close()

        if path.startswith("/api/messages/") and path.endswith("/reaction"):
            msg_id = path[len("/api/messages/"):-len("/reaction")].strip("/")
            reaction = body.get("reaction")
            if reaction is not None and reaction not in VALID_REACTIONS:
                return self._error(400, "invalid reaction")
            conn = get_db()
            try:
                row = conn.execute(
                    """SELECT * FROM messages WHERE id = ?
                       AND (sender_id = ? OR receiver_id = ?)""",
                    (msg_id, uid, uid),
                ).fetchone()
                if row is None:
                    return self._error(404, "message not found")
                conn.execute(
                    "UPDATE messages SET reaction = ? WHERE id = ?", (reaction, msg_id)
                )
                conn.commit()
                row = conn.execute("SELECT * FROM messages WHERE id = ?", (msg_id,)).fetchone()
                return self._send_json(200, {"message": public_message(row)})
            finally:
                conn.close()

        return self._error(404, "not found")


if __name__ == "__main__":
    init_db()
    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    print(f"SkyChat server running on http://0.0.0.0:{PORT}  (db: {DB_PATH})", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopping server...", flush=True)
