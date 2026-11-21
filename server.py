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
  POST /api/messages  (auth) {peerId, type, text, imageBase64, audioBase64, voiceDurationSec, replyToId}
  PATCH /api/messages/<id>  (auth) {text}
  DELETE /api/messages/<id>  (auth)
  DELETE /api/chats/<peerId>  (auth)
  POST /api/messages/<id>/reaction  (auth) {reaction}
  POST /api/messages/<id>/star  (auth) {starred}
  GET  /api/starred   (auth)
  POST /api/messages/<id>/pin  (auth) {pinned}
  POST /api/privacy   (auth) {hidePhone, hideOnline, findByPhone, findByUsername, readReceipts}
  GET  /api/me        (auth)
  GET  /api/threads   (auth)
  GET  /api/friends   (auth)
  POST /api/friends   (auth) {peerId}
  DELETE /api/friends/<peerId>  (auth)
  POST /api/blocks    (auth) {peerId}
  DELETE /api/blocks/<peerId>  (auth)
  POST /api/typing    (auth) {peerId, typing}
  GET  /api/typing?peerId=<id>  (auth)
  POST /api/devices   (auth) {fcmToken}
  DELETE /api/devices (auth) {fcmToken}
  POST /api/mute      (auth) {peerId, muted}
Auth: Authorization: Bearer <token>
FCM push (optional): set FCM_SERVICE_ACCOUNT_JSON to the service-account
JSON; without it notifications are skipped silently. FCM_DRY_RUN=1 logs
payloads instead of sending; FCM_VALIDATE_ONLY=1 adds validate_only.
"""
import base64
import hashlib
import json
import os
import secrets
import sqlite3
import threading
import time
import urllib.request
import uuid
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs, urlencode

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# مسار قاعدة البيانات؛ يمكن تثبيته عبر متغير بيئة (مثلاً مع Volume
# في Railway: SKYCHAT_DB_PATH=/data/sky_chat.db) حتى لا تُمسح البيانات
# عند إعادة النشر. بدون المتغير تُستخدم sky_chat.db بجانب server.py.
DB_PATH = os.environ.get("SKYCHAT_DB_PATH") or os.path.join(BASE_DIR, "sky_chat.db")
PORT = int(os.environ.get("PORT", "8000"))
MAX_IMAGE_B64 = 4 * 1024 * 1024  # ~4 MB of base64 text
MAX_AUDIO_B64 = 7 * 1024 * 1024  # ~7 MB of base64 text (~5 MB of audio)
MAX_BODY = 12 * 1024 * 1024  # audio ~5MB becomes ~6.7MB of base64
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
            username TEXT UNIQUE,
            phone TEXT UNIQUE,
            bio TEXT NOT NULL DEFAULT 'متاح للمحادثة على SkyChat ✨',
            password_hash TEXT NOT NULL,
            salt TEXT NOT NULL,
            is_online INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL,
            hide_phone INTEGER NOT NULL DEFAULT 0,
            hide_online INTEGER NOT NULL DEFAULT 0,
            find_by_phone INTEGER NOT NULL DEFAULT 1,
            find_by_username INTEGER NOT NULL DEFAULT 1,
            read_receipts INTEGER NOT NULL DEFAULT 1
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
        CREATE TABLE IF NOT EXISTS friends (
            user_id TEXT NOT NULL REFERENCES users(id),
            friend_id TEXT NOT NULL REFERENCES users(id),
            created_at TEXT NOT NULL,
            PRIMARY KEY (user_id, friend_id)
        );
        CREATE TABLE IF NOT EXISTS blocks (
            user_id TEXT NOT NULL REFERENCES users(id),
            blocked_id TEXT NOT NULL REFERENCES users(id),
            created_at TEXT NOT NULL,
            PRIMARY KEY (user_id, blocked_id)
        );
        CREATE TABLE IF NOT EXISTS devices (
            user_id TEXT NOT NULL REFERENCES users(id),
            fcm_token TEXT PRIMARY KEY,
            updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS stars (
            message_id TEXT NOT NULL REFERENCES messages(id),
            user_id TEXT NOT NULL REFERENCES users(id),
            PRIMARY KEY (message_id, user_id)
        );
        CREATE TABLE IF NOT EXISTS muted (
            user_id TEXT NOT NULL REFERENCES users(id),
            peer_id TEXT NOT NULL REFERENCES users(id),
            PRIMARY KEY (user_id, peer_id)
        );
        CREATE TABLE IF NOT EXISTS typing (
            user_id TEXT NOT NULL REFERENCES users(id),
            peer_id TEXT NOT NULL REFERENCES users(id),
            updated_at TEXT NOT NULL,
            PRIMARY KEY (user_id, peer_id)
        );
        """
    )
    # Migration: older databases required BOTH username and phone
    # (NOT NULL). Registration now accepts username OR phone, so rebuild
    # the users table with nullable username/phone when needed.
    # NOTE: do NOT use "ALTER TABLE users RENAME" here — SQLite would
    # rewrite the foreign keys in tokens/messages to point at the
    # renamed table, and dropping it would break them. Instead create
    # a new table, copy the rows, drop the old one and rename the new
    # one back, with foreign keys off during the swap.
    cols = {r[1]: r[3] for r in conn.execute("PRAGMA table_info(users)")}
    if cols.get("username") == 1 or cols.get("phone") == 1:
        conn.execute("PRAGMA foreign_keys = OFF")
        conn.execute(
            """CREATE TABLE users_new (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                username TEXT UNIQUE,
                phone TEXT UNIQUE,
                bio TEXT NOT NULL DEFAULT 'متاح للمحادثة على SkyChat ✨',
                password_hash TEXT NOT NULL,
                salt TEXT NOT NULL,
                is_online INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL
            )"""
        )
        conn.execute(
            """INSERT INTO users_new (id, name, username, phone, bio,
                                     password_hash, salt, is_online, created_at)
               SELECT id, name, username, phone, bio,
                      password_hash, salt, is_online, created_at
               FROM users"""
        )
        conn.execute("DROP TABLE users")
        conn.execute("ALTER TABLE users_new RENAME TO users")
        conn.execute("PRAGMA foreign_keys = ON")
    # Migration: privacy columns for databases created before v3.
    cols = {r[1] for r in conn.execute("PRAGMA table_info(users)")}
    for col, default in (
        ("hide_phone", 0),
        ("hide_online", 0),
        ("find_by_phone", 1),
        ("find_by_username", 1),
        ("read_receipts", 1),
    ):
        if col not in cols:
            conn.execute(
                f"ALTER TABLE users ADD COLUMN {col} INTEGER NOT NULL DEFAULT {default}"
            )
    # Migration: v4 columns for databases created before v4.
    msg_cols = {r[1] for r in conn.execute("PRAGMA table_info(messages)")}
    for col, ddl in (
        ("edited", "INTEGER NOT NULL DEFAULT 0"),
        ("edited_at", "TEXT"),
        ("pinned", "INTEGER NOT NULL DEFAULT 0"),
        ("audio_base64", "TEXT"),
    ):
        if col not in msg_cols:
            conn.execute(f"ALTER TABLE messages ADD COLUMN {col} {ddl}")
    user_cols = {r[1] for r in conn.execute("PRAGMA table_info(users)")}
    if "last_seen" not in user_cols:
        conn.execute("ALTER TABLE users ADD COLUMN last_seen TEXT")
    conn.commit()
    conn.close()


def hash_password(password, salt_hex):
    salt = bytes.fromhex(salt_hex)
    return hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 200_000).hex()


def public_privacy(row):
    return {
        "hidePhone": bool(row["hide_phone"]),
        "hideOnline": bool(row["hide_online"]),
        "findByPhone": bool(row["find_by_phone"]),
        "findByUsername": bool(row["find_by_username"]),
        "readReceipts": bool(row["read_receipts"]),
    }


def _row_col(row, name, default=None):
    # Safe column read: returns default when the column is absent
    # (e.g. a row from a database that predates a migration).
    return row[name] if name in row.keys() else default


def _is_typing_recent(updated_at):
    # A typing indicator is fresh only within the last 5 seconds.
    if not updated_at:
        return False
    try:
        ts = datetime.fromisoformat(updated_at)
    except (ValueError, TypeError):
        return False
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - ts).total_seconds() <= 5


def public_user(row, viewer_id=None):
    # viewer_id هو من يشاهد البيانات؛ إعدادات الخصوصية تُخفى الرقم
    # وحالة الاتصال عن الآخرين، أما صاحب الحساب فيرى بياناته كاملة.
    is_self = viewer_id is not None and viewer_id == row["id"]
    hide_phone = bool(row["hide_phone"]) and not is_self
    hide_online = bool(row["hide_online"]) and not is_self
    last_seen = _row_col(row, "last_seen")
    if hide_online:
        last_seen = None
    return {
        "id": row["id"],
        "name": row["name"],
        "username": row["username"],
        "phone": "" if hide_phone else row["phone"],
        "bio": row["bio"],
        "isOnline": False if hide_online else bool(row["is_online"]),
        "lastSeen": last_seen,
    }


def public_message(row, viewer_id=None, conn=None):
    # viewer_id + conn let us report whether the viewer starred this
    # message (stars table). Without them, "starred" is False.
    starred = False
    if viewer_id is not None and conn is not None:
        hit = conn.execute(
            "SELECT 1 FROM stars WHERE message_id = ? AND user_id = ?",
            (row["id"], viewer_id),
        ).fetchone()
        starred = hit is not None
    return {
        "id": row["id"],
        "senderId": row["sender_id"],
        "receiverId": row["receiver_id"],
        "type": row["type"],
        "text": row["text"] or "",
        "imageBase64": row["image_base64"],
        "audioBase64": _row_col(row, "audio_base64"),
        "voiceDurationSec": row["voice_duration_sec"],
        "replyToId": row["reply_to_id"],
        "reaction": row["reaction"],
        "viewCount": row["view_count"],
        "isRead": bool(row["is_read"]),
        "edited": bool(_row_col(row, "edited", 0)),
        "editedAt": _row_col(row, "edited_at"),
        "pinned": bool(_row_col(row, "pinned", 0)),
        "starred": starred,
        "timestamp": row["created_at"],
    }


# ---------------------------------------------------------------------------
# FCM push notifications (HTTP v1 API, stdlib only).
# The service-account key comes from the FCM_SERVICE_ACCOUNT_JSON env var
# (the full JSON content: project_id, client_email, private_key,
# private_key_id). Without it, sending is skipped silently — the server
# works fully offline and push is simply off.
# RS256 is implemented in pure Python: the PKCS#8 PEM key is parsed with
# a tiny ASN.1 reader, RSA numbers (n, d) are extracted, and the
# EMSA-PKCS1-v1_5 signature is computed with pow(). No third-party
# dependency (firebase-admin) is needed.
# Testability: FCM_DRY_RUN=1 records payloads in FCM_DRY_RUN_LOG instead
# of sending anything; build_fcm_payload() is pure and unit-testable.
# ---------------------------------------------------------------------------
FCM_DRY_RUN_LOG = []  # payloads recorded when FCM_DRY_RUN=1 (tests)

_FCM_SCOPE = "https://www.googleapis.com/auth/firebase.messaging"
_FCM_TOKEN_URL = "https://oauth2.googleapis.com/token"
_SHA256_DER_PREFIX = bytes.fromhex("3031300d060960864801650304020105000420")


def _b64url_encode(data):
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _der_read_length(data, pos):
    first = data[pos]
    pos += 1
    if first < 0x80:
        return first, pos
    nbytes = first & 0x7F
    if nbytes == 0 or nbytes > 4:
        raise ValueError("unsupported DER length")
    length = int.from_bytes(data[pos:pos + nbytes], "big")
    return length, pos + nbytes


def _der_read_tlv(data, pos):
    tag = data[pos]
    pos += 1
    length, pos = _der_read_length(data, pos)
    value = data[pos:pos + length]
    if len(value) != length:
        raise ValueError("truncated DER value")
    return tag, value, pos + length


def _rsa_private_numbers_from_pem(pem_text):
    """Extract RSA (n, d) from a PKCS#8 PEM private key (DER)."""
    lines = [ln.strip() for ln in pem_text.strip().splitlines()
             if ln.strip() and not ln.strip().startswith("-----")]
    der = base64.b64decode("".join(lines))
    tag, seq, _ = _der_read_tlv(der, 0)
    if tag != 0x30:
        raise ValueError("private key is not a DER SEQUENCE")
    # PrivateKeyInfo: version INTEGER, algorithm SEQUENCE,
    # privateKey OCTET STRING (contains RSAPrivateKey).
    pos = 0
    _, _, pos = _der_read_tlv(seq, pos)       # version
    _, _, pos = _der_read_tlv(seq, pos)       # algorithm
    otag, octet, _ = _der_read_tlv(seq, pos)  # privateKey
    if otag != 0x04:
        raise ValueError("expected OCTET STRING privateKey")
    tag, rsa_seq, _ = _der_read_tlv(octet, 0)
    if tag != 0x30:
        raise ValueError("not an RSAPrivateKey SEQUENCE")
    # RSAPrivateKey: version, n, e, d, p, q, dp, dq, qinv, ...
    ints = []
    p2 = 0
    while p2 < len(rsa_seq):
        itag, ival, p2 = _der_read_tlv(rsa_seq, p2)
        if itag != 0x02:
            raise ValueError("expected INTEGER in RSAPrivateKey")
        ints.append(int.from_bytes(ival, "big"))
    if len(ints) < 4:
        raise ValueError("RSAPrivateKey too short")
    return ints[1], ints[3]  # n, d


def _rs256_sign(private_key_pem, data):
    """RSASSA-PKCS1-v1_5 signature with SHA-256, pure Python."""
    n, d = _rsa_private_numbers_from_pem(private_key_pem)
    digest = hashlib.sha256(data).digest()
    t = _SHA256_DER_PREFIX + digest
    k = (n.bit_length() + 7) // 8
    if k < len(t) + 11:
        raise ValueError("RSA key too small for SHA-256 signature")
    em = b"\x00\x01" + b"\xff" * (k - len(t) - 3) + b"\x00" + t
    m = int.from_bytes(em, "big")
    s = pow(m, d, n)
    return s.to_bytes(k, "big")


def _make_fcm_jwt(service_account):
    now = int(time.time())
    header = {"alg": "RS256", "typ": "JWT"}
    if service_account.get("private_key_id"):
        header["kid"] = service_account["private_key_id"]
    claims = {
        "iss": service_account["client_email"],
        "scope": _FCM_SCOPE,
        "aud": _FCM_TOKEN_URL,
        "iat": now,
        "exp": now + 3600,
    }

    def _enc(obj):
        raw = json.dumps(obj, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        return _b64url_encode(raw)

    signing_input = _enc(header) + "." + _enc(claims)
    sig = _rs256_sign(service_account["private_key"], signing_input.encode("ascii"))
    return signing_input + "." + _b64url_encode(sig)


def _get_fcm_access_token(service_account):
    assertion = _make_fcm_jwt(service_account)
    body = urlencode({
        "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
        "assertion": assertion,
    }).encode("utf-8")
    req = urllib.request.Request(
        _FCM_TOKEN_URL, data=body,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read().decode("utf-8"))["access_token"]


def build_fcm_payload(token, sender_name, body_text, sender_id, validate_only=False):
    """Pure builder: the exact FCM HTTP v1 payload for one device token."""
    message = {
        "token": token,
        "notification": {"title": sender_name, "body": body_text},
        "data": {"peerId": str(sender_id), "peerName": sender_name},
    }
    if validate_only:
        message["validate_only"] = True
    return {"message": message}


def send_fcm_notification(sender_id, sender_name, receiver_id, body_text):
    """Send a push to all of the receiver's registered devices.

    Never raises: any failure is only logged, so a notification problem
    can never break or delay a message send. Returns the payloads that
    were sent/recorded, or None when sending was skipped (no service
    account configured, receiver muted the sender, or an error).
    """
    try:
        raw = os.environ.get("FCM_SERVICE_ACCOUNT_JSON")
        if not raw:
            return None  # push not configured — skip silently
        try:
            sa = json.loads(raw)
        except Exception as e:
            print(f"[FCM] bad FCM_SERVICE_ACCOUNT_JSON: {e}", flush=True)
            return None
        conn = get_db()
        try:
            muted = conn.execute(
                "SELECT 1 FROM muted WHERE user_id = ? AND peer_id = ?",
                (receiver_id, sender_id),
            ).fetchone()
            if muted:
                return None  # receiver muted this conversation
            tokens = [r["fcm_token"] for r in conn.execute(
                "SELECT fcm_token FROM devices WHERE user_id = ?",
                (receiver_id,),
            ).fetchall()]
        finally:
            conn.close()
        if not tokens:
            return []
        validate_only = os.environ.get("FCM_VALIDATE_ONLY") == "1"
        payloads = [
            build_fcm_payload(t, sender_name, body_text, sender_id, validate_only)
            for t in tokens
        ]
        if os.environ.get("FCM_DRY_RUN") == "1":
            FCM_DRY_RUN_LOG.extend(payloads)
            print(f"[FCM dry-run] would send {len(payloads)} notification(s)",
                  flush=True)
            return payloads
        access_token = _get_fcm_access_token(sa)
        url = ("https://fcm.googleapis.com/v1/projects/"
               f"{sa['project_id']}/messages:send")
        for payload in payloads:
            req = urllib.request.Request(
                url,
                data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
                headers={"Content-Type": "application/json",
                         "Authorization": f"Bearer {access_token}"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=15) as resp:
                resp.read()
        return payloads
    except Exception as e:
        print(f"[FCM] send failed: {e}", flush=True)
        return None


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
        self.send_header("Access-Control-Allow-Methods", "GET, POST, PATCH, DELETE, OPTIONS")
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
        # Update last_seen on every authenticated request.
        try:
            conn = get_db()
            try:
                conn.execute(
                    "UPDATE users SET last_seen = ? WHERE id = ?", (now_iso(), uid)
                )
                conn.commit()
            finally:
                conn.close()
        except Exception:
            pass  # last_seen must never break an authenticated request
        return uid

    # ---------- HTTP verbs ----------
    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, PATCH, DELETE, OPTIONS")
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
            # بحث فقط — لا توجد قائمة عامة بكل المسجلين. بدون كلمة بحث
            # تُرجع قائمة فارغة. البحث باليوزر أو الرقم فقط، ويحترم
            # إعدادات من يتم البحث عنه (السماح بالعثور عليه باليوزر/بالرقم).
            q = (qs.get("q") or [""])[0].strip().replace("@", "").lower()
            if len(q) < 2:
                return self._send_json(200, {"users": []})
            conn = get_db()
            try:
                # Blocked in either direction: never surface someone who
                # blocked the requester, or whom the requester blocked.
                blocked_ids = {
                    r["x"] for r in conn.execute(
                        "SELECT blocked_id AS x FROM blocks WHERE user_id = ?",
                        (uid,),
                    )
                } | {
                    r["x"] for r in conn.execute(
                        "SELECT user_id AS x FROM blocks WHERE blocked_id = ?",
                        (uid,),
                    )
                }
                rows = conn.execute(
                    "SELECT * FROM users WHERE id != ? AND (LOWER(username) LIKE ? OR phone LIKE ?)",
                    (uid, f"%{q}%", f"%{q}%"),
                ).fetchall()
                users = []
                for r in rows:
                    if r["id"] in blocked_ids:
                        continue
                    by_username = (r["username"] or "").lower().find(q) >= 0
                    by_phone = (r["phone"] or "").find(q) >= 0
                    if (by_username and r["find_by_username"]) or (
                        by_phone and r["find_by_phone"]
                    ):
                        users.append(public_user(r, viewer_id=uid))
                return self._send_json(200, {"users": users[:20]})
            finally:
                conn.close()

        if path == "/api/me":
            conn = get_db()
            try:
                row = conn.execute("SELECT * FROM users WHERE id = ?", (uid,)).fetchone()
                if row is None:
                    return self._error(404, "user not found")
                muted_ids = [r["peer_id"] for r in conn.execute(
                    "SELECT peer_id FROM muted WHERE user_id = ?", (uid,))]
                friend_ids = [r["friend_id"] for r in conn.execute(
                    "SELECT friend_id FROM friends WHERE user_id = ?", (uid,))]
                blocked_ids = [r["blocked_id"] for r in conn.execute(
                    "SELECT blocked_id FROM blocks WHERE user_id = ?", (uid,))]
                return self._send_json(
                    200,
                    {
                        "user": public_user(row, viewer_id=uid),
                        "privacy": public_privacy(row),
                        "mutedPeerIds": muted_ids,
                        "friendIds": friend_ids,
                        "blockedIds": blocked_ids,
                    },
                )
            finally:
                conn.close()

        if path == "/api/friends":
            conn = get_db()
            try:
                rows = conn.execute(
                    """SELECT u.* FROM friends f
                       JOIN users u ON u.id = f.friend_id
                       WHERE f.user_id = ? ORDER BY u.name ASC""",
                    (uid,),
                ).fetchall()
                return self._send_json(
                    200, {"friends": [public_user(r, viewer_id=uid) for r in rows]}
                )
            finally:
                conn.close()

        if path == "/api/starred":
            conn = get_db()
            try:
                rows = conn.execute(
                    """SELECT m.* FROM stars s
                       JOIN messages m ON m.id = s.message_id
                       WHERE s.user_id = ?
                       ORDER BY m.created_at DESC, m.rowid DESC""",
                    (uid,),
                ).fetchall()
                out = []
                for r in rows:
                    other_id = r["receiver_id"] if r["sender_id"] == uid else r["sender_id"]
                    peer = conn.execute(
                        "SELECT * FROM users WHERE id = ?", (other_id,)
                    ).fetchone()
                    item = public_message(r, viewer_id=uid, conn=conn)
                    item["peer"] = public_user(peer, viewer_id=uid) if peer else None
                    out.append(item)
                # مفتاحان بنفس القائمة للتوافق: التطبيق يقرأ "messages"
                # والتوثيق/الاختبارات تستخدم "starred".
                return self._send_json(200, {"messages": out, "starred": out})
            finally:
                conn.close()

        if path == "/api/typing":
            peer_id = (qs.get("peerId") or [""])[0].strip()
            if not peer_id:
                return self._error(400, "peerId is required")
            conn = get_db()
            try:
                row = conn.execute(
                    "SELECT updated_at FROM typing WHERE user_id = ? AND peer_id = ?",
                    (peer_id, uid),
                ).fetchone()
                typing = bool(row and _is_typing_recent(row["updated_at"]))
                return self._send_json(200, {"typing": typing})
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
                # Mark messages sent TO the requester as read — but only
                # if the requester allows read receipts. Otherwise the
                # sender never sees their messages as read.
                me = conn.execute(
                    "SELECT read_receipts FROM users WHERE id = ?", (uid,)
                ).fetchone()
                if me is not None and me["read_receipts"]:
                    conn.execute(
                        "UPDATE messages SET is_read = 1 WHERE receiver_id = ? AND sender_id = ? AND is_read = 0",
                        (uid, peer_id),
                    )
                    conn.commit()
                typing_row = conn.execute(
                    "SELECT updated_at FROM typing WHERE user_id = ? AND peer_id = ?",
                    (peer_id, uid),
                ).fetchone()
                peer_typing = bool(typing_row and _is_typing_recent(typing_row["updated_at"]))
                return self._send_json(200, {
                    "messages": [public_message(r, viewer_id=uid, conn=conn) for r in rows],
                    "peerTyping": peer_typing,
                })
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
                        "peer": public_user(peer, viewer_id=uid),
                        "lastMessage": public_message(last, viewer_id=uid, conn=conn) if last else None,
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
            # التسجيل بيوزر أو رقم موبايل فقط — مش شرط الاتنين.
            if not username and not phone:
                return self._error(400, "username or phone is required")
            if not password:
                return self._error(400, "password is required")
            if not name:
                name = username or phone
            if len(password) < 3:
                return self._error(400, "password is too short")
            if len(username) > 50 or len(phone) > 30 or len(name) > 100:
                return self._error(400, "field too long")
            conn = get_db()
            try:
                conditions = []
                params = []
                if username:
                    conditions.append("username = ?")
                    params.append(username)
                if phone:
                    conditions.append("phone = ?")
                    params.append(phone)
                exists = conn.execute(
                    "SELECT id FROM users WHERE " + " OR ".join(conditions),
                    params,
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
                    (user_id, name, username or None, phone or None,
                     "متاح للمحادثة على SkyChat ✨", pw_hash, salt, now_iso()),
                )
                token = secrets.token_hex(32)
                conn.execute(
                    "INSERT INTO tokens (token, user_id, created_at) VALUES (?, ?, ?)",
                    (token, user_id, now_iso()),
                )
                conn.commit()
                row = conn.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
                return self._send_json(201, {"user": public_user(row, viewer_id=row["id"]), "token": token})
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
                return self._send_json(200, {"user": public_user(row, viewer_id=row["id"]), "token": token})
            finally:
                conn.close()

        # ---- authenticated endpoints ----
        uid = self._require_auth()
        if uid is None:
            return

        if path == "/api/privacy":
            fields = {
                "hide_phone": "hidePhone",
                "hide_online": "hideOnline",
                "find_by_phone": "findByPhone",
                "find_by_username": "findByUsername",
                "read_receipts": "readReceipts",
            }
            updates = {}
            for col, key in fields.items():
                if key in body and isinstance(body[key], bool):
                    updates[col] = 1 if body[key] else 0
            conn = get_db()
            try:
                if updates:
                    sets = ", ".join(f"{col} = ?" for col in updates)
                    conn.execute(
                        f"UPDATE users SET {sets} WHERE id = ?",
                        (*updates.values(), uid),
                    )
                    conn.commit()
                row = conn.execute("SELECT * FROM users WHERE id = ?", (uid,)).fetchone()
                if row is None:
                    return self._error(404, "user not found")
                return self._send_json(200, {"privacy": public_privacy(row)})
            finally:
                conn.close()

        if path == "/api/messages":
            peer_id = str(body.get("peerId") or "").strip()
            mtype = str(body.get("type") or "text").strip()
            text = str(body.get("text") or "")
            image_b64 = body.get("imageBase64")
            audio_b64 = body.get("audioBase64")
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
                if audio_b64 is not None:
                    if not isinstance(audio_b64, str):
                        return self._error(400, "audioBase64 must be a string")
                    if len(audio_b64) > MAX_AUDIO_B64:
                        return self._error(413, "audio too large (max ~5MB base64)")
            else:
                voice_sec = None
                audio_b64 = None
            if reply_to is not None:
                reply_to = str(reply_to)
            conn = get_db()
            try:
                peer = conn.execute("SELECT id FROM users WHERE id = ?", (peer_id,)).fetchone()
                if peer is None:
                    return self._error(404, "peer not found")
                if peer_id == uid:
                    return self._error(400, "cannot message yourself")
                blocked = conn.execute(
                    "SELECT 1 FROM blocks WHERE user_id = ? AND blocked_id = ?",
                    (peer_id, uid),
                ).fetchone()
                if blocked:
                    return self._error(403, "you are blocked by this user")
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
                                             image_base64, audio_base64, voice_duration_sec,
                                             reply_to_id, reaction, view_count, is_read, created_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, 0, 0, ?)""",
                    (msg_id, uid, peer_id, mtype, text,
                     image_b64 if mtype in ("image", "hiddenImage") else None,
                     audio_b64 if mtype == "voice" else None,
                     voice_sec, reply_to, created),
                )
                conn.commit()
                row = conn.execute("SELECT * FROM messages WHERE id = ?", (msg_id,)).fetchone()
                # Push notification, fire-and-forget: a daemon thread so a
                # slow/failing FCM call can never delay or fail the reply.
                sender = conn.execute("SELECT name FROM users WHERE id = ?", (uid,)).fetchone()
                sender_name = sender["name"] if sender else ""
                if mtype == "text":
                    notif_body = text[:200] if text.strip() else "رسالة جديدة"
                elif mtype in ("image", "hiddenImage"):
                    notif_body = "📷 صورة"
                elif mtype == "voice":
                    notif_body = "🎤 رسالة صوتية"
                else:
                    notif_body = "رسالة جديدة"
                try:
                    threading.Thread(
                        target=send_fcm_notification,
                        args=(uid, sender_name, peer_id, notif_body),
                        daemon=True,
                    ).start()
                except Exception as e:
                    print(f"[FCM] could not start notify thread: {e}", flush=True)
                return self._send_json(201, {"message": public_message(row, viewer_id=uid, conn=conn)})
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
                return self._send_json(200, {"message": public_message(row, viewer_id=uid, conn=conn)})
            finally:
                conn.close()

        if path.startswith("/api/messages/") and path.endswith("/star"):
            msg_id = path[len("/api/messages/"):-len("/star")].strip("/")
            starred = body.get("starred", True)
            if not isinstance(starred, bool):
                return self._error(400, "starred must be a boolean")
            conn = get_db()
            try:
                row = conn.execute(
                    """SELECT * FROM messages WHERE id = ?
                       AND (sender_id = ? OR receiver_id = ?)""",
                    (msg_id, uid, uid),
                ).fetchone()
                if row is None:
                    return self._error(404, "message not found")
                if starred:
                    conn.execute(
                        "INSERT OR IGNORE INTO stars (message_id, user_id) VALUES (?, ?)",
                        (msg_id, uid),
                    )
                else:
                    conn.execute(
                        "DELETE FROM stars WHERE message_id = ? AND user_id = ?",
                        (msg_id, uid),
                    )
                conn.commit()
                return self._send_json(200, {"starred": starred})
            finally:
                conn.close()

        if path.startswith("/api/messages/") and path.endswith("/pin"):
            msg_id = path[len("/api/messages/"):-len("/pin")].strip("/")
            pinned = body.get("pinned", True)
            if not isinstance(pinned, bool):
                return self._error(400, "pinned must be a boolean")
            conn = get_db()
            try:
                row = conn.execute(
                    """SELECT * FROM messages WHERE id = ?
                       AND (sender_id = ? OR receiver_id = ?)""",
                    (msg_id, uid, uid),
                ).fetchone()
                if row is None:
                    return self._error(404, "message not found")
                a, b = row["sender_id"], row["receiver_id"]
                if pinned:
                    # One pinned message per conversation: unpin the rest
                    # of this thread first.
                    conn.execute(
                        """UPDATE messages SET pinned = 0
                           WHERE ((sender_id = ? AND receiver_id = ?)
                              OR (sender_id = ? AND receiver_id = ?))
                             AND id != ?""",
                        (a, b, b, a, msg_id),
                    )
                    conn.execute("UPDATE messages SET pinned = 1 WHERE id = ?", (msg_id,))
                else:
                    conn.execute("UPDATE messages SET pinned = 0 WHERE id = ?", (msg_id,))
                conn.commit()
                return self._send_json(200, {"pinned": pinned, "messageId": msg_id})
            finally:
                conn.close()

        if path == "/api/friends":
            peer_id = str(body.get("peerId") or "").strip()
            if not peer_id:
                return self._error(400, "peerId is required")
            if peer_id == uid:
                return self._error(400, "cannot add yourself as a friend")
            conn = get_db()
            try:
                peer = conn.execute("SELECT * FROM users WHERE id = ?", (peer_id,)).fetchone()
                if peer is None:
                    return self._error(404, "peer not found")
                exists = conn.execute(
                    "SELECT 1 FROM friends WHERE user_id = ? AND friend_id = ?",
                    (uid, peer_id),
                ).fetchone()
                if exists:
                    return self._error(400, "already friends")
                conn.execute(
                    "INSERT INTO friends (user_id, friend_id, created_at) VALUES (?, ?, ?)",
                    (uid, peer_id, now_iso()),
                )
                conn.commit()
                return self._send_json(200, {"ok": True, "friend": public_user(peer, viewer_id=uid)})
            finally:
                conn.close()

        if path == "/api/blocks":
            peer_id = str(body.get("peerId") or "").strip()
            if not peer_id:
                return self._error(400, "peerId is required")
            if peer_id == uid:
                return self._error(400, "cannot block yourself")
            conn = get_db()
            try:
                peer = conn.execute("SELECT id FROM users WHERE id = ?", (peer_id,)).fetchone()
                if peer is None:
                    return self._error(404, "peer not found")
                conn.execute(
                    "INSERT OR IGNORE INTO blocks (user_id, blocked_id, created_at) VALUES (?, ?, ?)",
                    (uid, peer_id, now_iso()),
                )
                conn.commit()
                return self._send_json(200, {"ok": True})
            finally:
                conn.close()

        if path == "/api/typing":
            peer_id = str(body.get("peerId") or "").strip()
            typing = body.get("typing", True)
            if not isinstance(typing, bool):
                return self._error(400, "typing must be a boolean")
            if not peer_id:
                return self._error(400, "peerId is required")
            conn = get_db()
            try:
                peer = conn.execute("SELECT id FROM users WHERE id = ?", (peer_id,)).fetchone()
                if peer is None:
                    return self._error(404, "peer not found")
                if typing:
                    conn.execute(
                        "INSERT OR REPLACE INTO typing (user_id, peer_id, updated_at) VALUES (?, ?, ?)",
                        (uid, peer_id, now_iso()),
                    )
                else:
                    conn.execute(
                        "DELETE FROM typing WHERE user_id = ? AND peer_id = ?",
                        (uid, peer_id),
                    )
                conn.commit()
                return self._send_json(200, {"ok": True})
            finally:
                conn.close()

        if path == "/api/devices":
            token = str(body.get("fcmToken") or "").strip()
            if not token:
                return self._error(400, "fcmToken is required")
            conn = get_db()
            try:
                conn.execute(
                    "INSERT OR REPLACE INTO devices (user_id, fcm_token, updated_at) VALUES (?, ?, ?)",
                    (uid, token, now_iso()),
                )
                conn.commit()
                return self._send_json(200, {"ok": True})
            finally:
                conn.close()

        if path == "/api/mute":
            peer_id = str(body.get("peerId") or "").strip()
            muted = body.get("muted", True)
            if not isinstance(muted, bool):
                return self._error(400, "muted must be a boolean")
            if not peer_id:
                return self._error(400, "peerId is required")
            conn = get_db()
            try:
                peer = conn.execute("SELECT id FROM users WHERE id = ?", (peer_id,)).fetchone()
                if peer is None:
                    return self._error(404, "peer not found")
                if muted:
                    conn.execute(
                        "INSERT OR IGNORE INTO muted (user_id, peer_id) VALUES (?, ?)",
                        (uid, peer_id),
                    )
                else:
                    conn.execute(
                        "DELETE FROM muted WHERE user_id = ? AND peer_id = ?",
                        (uid, peer_id),
                    )
                conn.commit()
                return self._send_json(200, {"muted": muted})
            finally:
                conn.close()

        return self._error(404, "not found")


    def do_PATCH(self):
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

        uid = self._require_auth()
        if uid is None:
            return

        if path.startswith("/api/messages/"):
            msg_id = path[len("/api/messages/"):].strip("/")
            if not msg_id or "/" in msg_id:
                return self._error(404, "not found")
            text = body.get("text")
            if text is None or not isinstance(text, str) or not text.strip():
                return self._error(400, "text is required")
            conn = get_db()
            try:
                row = conn.execute("SELECT * FROM messages WHERE id = ?", (msg_id,)).fetchone()
                if row is None:
                    return self._error(404, "message not found")
                if row["sender_id"] != uid:
                    return self._error(403, "only the sender can edit a message")
                if row["type"] != "text":
                    return self._error(400, "only text messages can be edited")
                conn.execute(
                    "UPDATE messages SET text = ?, edited = 1, edited_at = ? WHERE id = ?",
                    (text, now_iso(), msg_id),
                )
                conn.commit()
                row = conn.execute("SELECT * FROM messages WHERE id = ?", (msg_id,)).fetchone()
                return self._send_json(200, {"message": public_message(row, viewer_id=uid, conn=conn)})
            finally:
                conn.close()

        return self._error(404, "not found")

    def do_DELETE(self):
        parsed = urlparse(self.path)
        path = parsed.path
        # DELETE /api/devices carries its token in a JSON body.
        body = {}
        if path == "/api/devices":
            try:
                body = self._read_json()
            except ValueError:
                return self._error(413, "body too large")
            except Exception:
                return self._error(400, "invalid JSON")
            if not isinstance(body, dict):
                return self._error(400, "invalid JSON")

        uid = self._require_auth()
        if uid is None:
            return

        conn = get_db()
        try:
            if path.startswith("/api/messages/"):
                msg_id = path[len("/api/messages/"):].strip("/")
                if not msg_id or "/" in msg_id:
                    return self._error(404, "not found")
                row = conn.execute("SELECT * FROM messages WHERE id = ?", (msg_id,)).fetchone()
                if row is None:
                    return self._error(404, "message not found")
                if row["sender_id"] != uid:
                    return self._error(403, "only the sender can delete a message")
                conn.execute("DELETE FROM stars WHERE message_id = ?", (msg_id,))
                conn.execute("DELETE FROM messages WHERE id = ?", (msg_id,))
                conn.commit()
                return self._send_json(200, {"ok": True})

            if path.startswith("/api/chats/"):
                peer_id = path[len("/api/chats/"):].strip("/")
                if not peer_id or "/" in peer_id:
                    return self._error(404, "not found")
                # Delete for both sides: every message between the two.
                ids = [r["id"] for r in conn.execute(
                    """SELECT id FROM messages
                       WHERE (sender_id = ? AND receiver_id = ?)
                          OR (sender_id = ? AND receiver_id = ?)""",
                    (uid, peer_id, peer_id, uid),
                ).fetchall()]
                for mid in ids:
                    conn.execute("DELETE FROM stars WHERE message_id = ?", (mid,))
                cur = conn.execute(
                    """DELETE FROM messages
                       WHERE (sender_id = ? AND receiver_id = ?)
                          OR (sender_id = ? AND receiver_id = ?)""",
                    (uid, peer_id, peer_id, uid),
                )
                conn.commit()
                return self._send_json(200, {"ok": True, "deleted": cur.rowcount})

            if path.startswith("/api/friends/"):
                peer_id = path[len("/api/friends/"):].strip("/")
                if not peer_id or "/" in peer_id:
                    return self._error(404, "not found")
                conn.execute(
                    "DELETE FROM friends WHERE user_id = ? AND friend_id = ?",
                    (uid, peer_id),
                )
                conn.commit()
                return self._send_json(200, {"ok": True})

            if path.startswith("/api/blocks/"):
                peer_id = path[len("/api/blocks/"):].strip("/")
                if not peer_id or "/" in peer_id:
                    return self._error(404, "not found")
                conn.execute(
                    "DELETE FROM blocks WHERE user_id = ? AND blocked_id = ?",
                    (uid, peer_id),
                )
                conn.commit()
                return self._send_json(200, {"ok": True})

            if path == "/api/devices":
                token = str(body.get("fcmToken") or "").strip()
                if not token:
                    return self._error(400, "fcmToken is required")
                conn.execute(
                    "DELETE FROM devices WHERE user_id = ? AND fcm_token = ?",
                    (uid, token),
                )
                conn.commit()
                return self._send_json(200, {"ok": True})
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
