import base64
import hashlib
import hmac
import json
import logging
import os
import random
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from threading import Lock
from uuid import uuid4

import uvicorn
from fastapi import FastAPI, Form, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, PlainTextResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from jinja2.sandbox import SandboxedEnvironment

from noise_generator import start_noise_stream, stop_noise_stream
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, generate_latest

BASE_DIR = Path(__file__).resolve().parent
ATTACHMENTS_DIR = BASE_DIR / "chat_files" / "attachments"
DB_PATH = BASE_DIR / "data" / "clicker.sqlite3"
SID_COOKIE = "sid"

app = FastAPI(title="clicker_task")
app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

FLAG = os.getenv("FLAG", "nskctf{C0ngr4ts_YouVe_f1nally_W0n}")
LOG_ID = "LogID405"
CLICK_GOAL = 1_000_000_000
CLICKER_DECOY_FLAG = "nskctf{qwerty123}"
RPS_DECOY_REWARD = "shadow-token-rps-v1"
LEGACY_JWT_SECRET = "1234"
LEGACY_JWT_ISSUER = "clicker-legacy-api"
FAKE_HASHES = {
    "sha256": "7fd8d59a4b7f6c26180a1b6f0cdb6b7d3f84ed86c9f6a8c497db7c6f3e9f4ab1",
    "md5": "6df23dc03f9b54cc38a0fc1483df6e21",
}
FALSE_RECENT_IDS = [f"LogID{num}" for num in (431, 438, 447, 452, 463, 477, 488, 492)]
FAKE_SESSION_IDS = {
    "admin": "sid_admin_7b8f20aa9f43",
    "analyst": "sid_analyst_d14c033e5c19",
}
random.seed(1337)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("clicker")

METRIC_HTTP_REQUESTS = Counter(
    "clicker_http_requests_total",
    "Total HTTP requests seen by clicker_task",
    ["method", "path", "status"],
)
METRIC_REGISTERED_USERS = Gauge("clicker_registered_users", "Registered user count in sqlite auth")
METRIC_CHAT_MESSAGES = Gauge("clicker_chat_messages", "Current number of chat messages")
METRIC_ACTIVE_AUTH_SESSIONS = Gauge("clicker_active_auth_sessions", "Active auth sessions held in memory")
METRIC_CLICK_REQUESTS = Counter("clicker_click_requests_total", "Accepted click requests")
METRIC_CHAT_POSTS = Counter("clicker_chat_posts_total", "Chat posts sent by users")
METRIC_LOGIN_SUCCESS = Counter("clicker_login_success_total", "Successful login attempts")
METRIC_LOGIN_FAILURE = Counter("clicker_login_failure_total", "Failed login attempts")
METRIC_REGISTER_SUCCESS = Counter("clicker_register_success_total", "Successful registrations")
METRIC_REGISTER_FAILURE = Counter("clicker_register_failure_total", "Failed registrations")
METRIC_FALSE_TRAIL_HITS = Counter(
    "clicker_false_trail_hits_total",
    "Hits on intentionally misleading routes",
    ["trail"],
)
METRIC_FAKE_REWARD = Counter("clicker_fake_reward_hits_total", "Times the fake clicker reward was revealed")

click_lock = Lock()
message_lock = Lock()
auth_lock = Lock()
session_clicks: dict[str, int] = {}
auth_sessions: dict[str, str] = {}
session_rps_streaks: dict[str, int] = {}

NOISE_OPS = [
    "cache_miss",
    "cache_sync",
    "health_probe",
    "dashboard_refresh",
    "job_tick",
    "metric_flush",
    "trace_export",
]

NOISE_CHAT_AUTHORS = [
    "misha",
    "lena",
    "alex",
    "dima",
    "ira",
    "ops-bot",
    "qa-bot",
    "roman",
]

NOISE_CHAT_LINES = [
    "renamed one screenshot again.",
    "dashboard colors still feel wrong.",
    "someone forgot to rotate yesterday dump.",
    "night build passed but too slowly.",
    "support pinged about a harmless warning.",
    "please stop uploading random text files.",
    "there is another fake incident in staging.",
    "sqlite auth should be replaced later.",
    "frontend still ships the old verifier blob.",
    "admin account has no widgets configured anyway.",
    "too many ids in logs after synthetic checks.",
    "i archived the pngs but not the txt files.",
    "compose is valid now, please do not touch it.",
    "if you need old hashes check the backup folder.",
    "preview sandbox still exposes weird cosmetic placeholders.",
    "rps reward token was never wired to any real backend.",
    "stats page still has that negative admin counter anomaly.",
]

RPS_MOVES = ("rock", "paper", "scissors")
RPS_BEATS = {
    "rock": "scissors",
    "paper": "rock",
    "scissors": "paper",
}
BADGE_PREVIEW_ENV = SandboxedEnvironment(autoescape=False)
BADGE_PREVIEW_CONTEXT = {
    "nickname": "ops_viewer",
    "status": "warm",
    "admin_clicks": -1,
    "legacy_hash": FAKE_HASHES["sha256"],
    "reward_flag": CLICKER_DECOY_FLAG,
    "mirror": "chat-badge-sandbox",
}
STATS_ROWS = [
    {"player": "admin", "clicks": -1, "rank": "legacy", "note": "rollback anomaly"},
    {"player": "misha", "clicks": 4821, "rank": "staff", "note": "too many test runs"},
    {"player": "ops_viewer", "clicks": 0, "rank": "viewer", "note": "read-only account"},
    {"player": "analyst", "clicks": 118, "rank": "user", "note": "dashboard checks"},
    {"player": "demo", "clicks": 42, "rank": "user", "note": "qa smoke user"},
]

CLUE_MESSAGES = [
    {"author": "lena", "time": "2026-03-21 12:16", "text": "if support says flag endpoint fails, use request id from response."},
    {"author": "alex", "time": "2026-03-21 12:17", "text": "you mean LogID? yes, query logs by that id."},
    {"author": "misha", "time": "2026-03-21 12:18", "text": "right, search in grafana/loki by exact LogID string."},
    {"author": "lena", "time": "2026-03-21 12:21", "text": "not by stream, stream is full of noise."},
    {"author": "dima", "time": "2026-03-21 13:05", "text": "also moved grafana viewer creds to monitoring.env."},
    {"author": "alex", "time": "2026-03-21 13:06", "text": "path is /app/config/monitoring.env inside app container."},
    {"author": "lena", "time": "2026-03-21 13:07", "text": "viewer account is enough, clicker dashboard is provisioned read-only."},
    {"author": "misha", "time": "2026-03-21 13:08", "text": "temporary file handler still /chat/file?name=<filename>"},
    {"author": "lena", "time": "2026-03-21 13:12", "text": "we will harden file endpoint after release."},
    {"author": "alex", "time": "2026-03-22 09:11", "text": "another incident id storm overnight."},
    {"author": "misha", "time": "2026-03-22 09:12", "text": "ignore random ids, they are synthetic test noise."},
    {"author": "dima", "time": "2026-03-22 09:14", "text": "real ones come from /flag failures only."},
    {"author": "lena", "time": "2026-03-22 09:16", "text": "message board stays open, but browser find is faster than backend search anyway."},
    {"author": "misha", "time": "2026-03-22 09:17", "text": "yep, nickname plus message, then post."},
]

FALSE_TRAIL_MESSAGES = [
    {"author": "roman", "time": "2026-03-20 11:11", "text": "legacy frontend hash gate still exposes a sha256 in page source."},
    {"author": "ira", "time": "2026-03-20 11:13", "text": "if anyone asks, backup/users.txt only has old digests."},
    {"author": "alex", "time": "2026-03-20 11:14", "text": "debug/config still mentions admin, but admin account is empty."},
    {"author": "dima", "time": "2026-03-20 11:15", "text": "people keep brute forcing admin for no reason."},
    {"author": "misha", "time": "2026-03-20 11:17", "text": "there is an api/flag-hash stub too, but it is legacy junk."},
    {"author": "ira", "time": "2026-03-20 11:18", "text": "reward proof endpoint still leaks the fake milestone flag."},
    {"author": "roman", "time": "2026-03-20 11:19", "text": "session debug endpoint shows old admin sid snapshots, still useless."},
    {"author": "alex", "time": "2026-03-20 11:21", "text": "exports/auth.dump is an old sqlite dump from before the cleanup."},
    {"author": "dima", "time": "2026-03-20 11:22", "text": "observability migration status still mentions kibana on 5601."},
    {"author": "misha", "time": "2026-03-20 11:24", "text": "incident archive 405 was sanitized and never had the real flag anyway."},
    {"author": "ira", "time": "2026-03-20 11:25", "text": "legacy api still issues jwt tokens for the ops mirror."},
    {"author": "roman", "time": "2026-03-20 11:26", "text": "super_admin claim exists there, but the mirror has no useful exports anyway."},
    {"author": "alex", "time": "2026-03-20 11:27", "text": "/chat/live-preview still reflects raw html, but nobody turned on the bot user."},
    {"author": "lena", "time": "2026-03-20 11:28", "text": "/chat/badge-preview still evaluates jinja snippets like {{7*7}}, only cosmetic vars though."},
    {"author": "dima", "time": "2026-03-20 11:29", "text": "/stats keeps showing admin at -1 clicks after the rollback bug."},
    {"author": "ira", "time": "2026-03-20 11:30", "text": "/rps still hands out a fake shadow token if you win enough rounds."},
    {"author": "roman", "time": "2026-03-20 11:31", "text": "microservices /rps and /stats were never connected to anything real."},
]

DEFAULT_USERS = [
    ("admin", "admin2026", "admin", ""),
    ("analyst", "analyst2026", "user", "Observes counters, not much else."),
    ("demo", "demo2026", "user", "Test profile for QA."),
]


def now_ts() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


def fake_log_id(low: int = 100, high: int = 999) -> str:
    value = random.randint(low, high)
    while value == 405:
        value = random.randint(low, high)
    return f"LogID{value}"


def write_noise_logs(context: str, amount: int = 4) -> None:
    for _ in range(amount):
        logger.info(
            "STREAM_A context=%s id=%s op=%s latency_ms=%d status=%s",
            context,
            fake_log_id(),
            random.choice(NOISE_OPS),
            random.randint(1, 450),
            random.choice(["ok", "retry", "cached"]),
        )


def attachments_list() -> list[str]:
    return sorted(item.name for item in ATTACHMENTS_DIR.iterdir() if item.is_file())


def build_noise_messages(start: datetime, count: int, offset: int) -> list[dict[str, str]]:
    items: list[dict[str, str]] = []
    for index in range(count):
        moment = start + timedelta(minutes=7 * index)
        author = NOISE_CHAT_AUTHORS[(index + offset) % len(NOISE_CHAT_AUTHORS)]
        text = NOISE_CHAT_LINES[(index * 3 + offset) % len(NOISE_CHAT_LINES)]
        suffix = f" ref={1000 + offset + index}" if index % 5 == 0 else ""
        items.append(
            {
                "author": author,
                "time": moment.strftime("%Y-%m-%d %H:%M"),
                "text": f"{text}{suffix}",
            }
        )
    return items


def build_chat_history() -> list[dict[str, str]]:
    early_noise = build_noise_messages(datetime(2026, 3, 18, 8, 0), 390, 0)
    late_noise = build_noise_messages(datetime(2026, 3, 22, 10, 0), 390, 390)
    messages = early_noise + FALSE_TRAIL_MESSAGES + CLUE_MESSAGES + late_noise
    messages.sort(key=lambda item: item["time"])
    return messages


CHAT_MESSAGES = build_chat_history()


def build_stats_rows() -> list[dict[str, str | int]]:
    rows = [dict(row) for row in STATS_ROWS]
    return rows


def rps_round(player_move: str) -> tuple[str, str, str]:
    server_move = random.choice(RPS_MOVES)
    if player_move == server_move:
        return server_move, "tie", "Tie. Nothing changes."
    if RPS_BEATS[player_move] == server_move:
        return server_move, "win", "Win streak increased."
    return server_move, "lose", "Lost the round. Streak reset."


def db_connect() -> sqlite3.Connection:
    connection = sqlite3.connect(DB_PATH)
    connection.row_factory = sqlite3.Row
    return connection


def init_db() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with db_connect() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                password TEXT NOT NULL,
                role TEXT NOT NULL DEFAULT 'user',
                bio TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS session_clicks (
                sid TEXT PRIMARY KEY,
                clicks INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL
            )
            """
        )
        for username, password, role, bio in DEFAULT_USERS:
            conn.execute(
                """
                INSERT OR IGNORE INTO users (username, password, role, bio, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (username, password, role, bio, "2026-03-18 00:00:00"),
            )
        conn.commit()


def get_remote_addr(request: Request) -> str:
    if request.client is None:
        return "-"
    return request.client.host


def base64url_encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def base64url_decode(value: str) -> bytes:
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode(value + padding)


def sign_legacy_jwt(payload: dict) -> str:
    header = {"alg": "HS256", "typ": "JWT", "kid": "legacy-2024"}
    header_chunk = base64url_encode(json.dumps(header, separators=(",", ":")).encode("utf-8"))
    payload_chunk = base64url_encode(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
    signing_input = f"{header_chunk}.{payload_chunk}".encode("utf-8")
    signature = hmac.new(LEGACY_JWT_SECRET.encode("utf-8"), signing_input, hashlib.sha256).digest()
    return f"{header_chunk}.{payload_chunk}.{base64url_encode(signature)}"


def verify_legacy_jwt(token: str) -> dict:
    try:
        header_chunk, payload_chunk, signature_chunk = token.split(".")
    except ValueError as exc:
        raise ValueError("invalid token format") from exc

    header = json.loads(base64url_decode(header_chunk))
    if header.get("alg") != "HS256":
        raise ValueError("unsupported alg")

    signing_input = f"{header_chunk}.{payload_chunk}".encode("utf-8")
    expected_signature = hmac.new(
        LEGACY_JWT_SECRET.encode("utf-8"),
        signing_input,
        hashlib.sha256,
    ).digest()
    provided_signature = base64url_decode(signature_chunk)
    if not hmac.compare_digest(provided_signature, expected_signature):
        raise ValueError("invalid signature")

    payload = json.loads(base64url_decode(payload_chunk))
    if payload.get("iss") != LEGACY_JWT_ISSUER:
        raise ValueError("invalid issuer")
    return payload


def build_legacy_jwt(user: dict, *, super_admin: bool = False) -> str:
    payload = {
        "sub": user["username"],
        "role": user["role"],
        "super_admin": bool(super_admin),
        "scope": ["profile:read", "ops:mirror"],
        "iss": LEGACY_JWT_ISSUER,
        "iat": int(datetime.utcnow().timestamp()),
    }
    return sign_legacy_jwt(payload)


def get_user_by_username(username: str) -> dict | None:
    with db_connect() as conn:
        row = conn.execute(
            "SELECT id, username, password, role, bio, created_at FROM users WHERE username = ?",
            (username,),
        ).fetchone()
    if row is None:
        return None
    return dict(row)


def get_or_create_sid(request: Request) -> tuple[str, bool]:
    sid = request.cookies.get(SID_COOKIE)
    if sid:
        return sid, False
    return uuid4().hex, True


def get_current_user(request: Request) -> dict | None:
    sid = request.cookies.get(SID_COOKIE)
    if not sid:
        return None
    username = auth_sessions.get(sid)
    if not username:
        return None
    row = get_user_by_username(username)
    if row is None:
        auth_sessions.pop(sid, None)
        return None
    return row


async def read_legacy_auth_payload(request: Request) -> tuple[str, str]:
    content_type = request.headers.get("content-type", "")
    username = ""
    password = ""
    if "application/json" in content_type:
        try:
            payload = await request.json()
        except json.JSONDecodeError:
            payload = {}
        username = str(payload.get("username", ""))
        password = str(payload.get("password", ""))
    else:
        form = await request.form()
        username = str(form.get("username", ""))
        password = str(form.get("password", ""))
    return username.strip()[:80], password.strip()[:80]


def get_legacy_claims(request: Request) -> dict:
    authorization = request.headers.get("authorization", "")
    if not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="bearer token required")

    token = authorization.split(" ", 1)[1].strip()
    try:
        return verify_legacy_jwt(token)
    except ValueError as exc:
        raise HTTPException(status_code=401, detail=str(exc)) from exc


def remember_user(response: RedirectResponse | JSONResponse, sid: str, username: str) -> None:
    auth_sessions[sid] = username
    response.set_cookie(SID_COOKIE, sid, httponly=True, samesite="lax")


def current_click_count(request: Request) -> int:
    sid = request.cookies.get(SID_COOKIE, "")
    if not sid:
        return 0
    return session_clicks.get(sid, 0)


def click_goal_reached(user_clicks: int) -> bool:
    return user_clicks >= CLICK_GOAL


def render_login_page(request: Request, error: str = "", notice: str = "") -> HTMLResponse:
    return templates.TemplateResponse(
        "login.html",
        {
            "request": request,
            "error": error,
            "notice": notice,
            "user": get_current_user(request),
        },
    )


def render_register_page(request: Request, error: str = "", notice: str = "") -> HTMLResponse:
    return templates.TemplateResponse(
        "register.html",
        {
            "request": request,
            "error": error,
            "notice": notice,
            "user": get_current_user(request),
        },
    )


def update_runtime_metrics() -> None:
    METRIC_CHAT_MESSAGES.set(len(CHAT_MESSAGES))
    METRIC_ACTIVE_AUTH_SESSIONS.set(len(auth_sessions))
    with db_connect() as conn:
        registered_users = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    METRIC_REGISTERED_USERS.set(registered_users)


@app.on_event("startup")
async def startup_event() -> None:
    # Load session clicks from database
    with db_connect() as conn:
        rows = conn.execute("SELECT sid, clicks FROM session_clicks").fetchall()
        for row in rows:
            session_clicks[row["sid"]] = row["clicks"]
    update_runtime_metrics()

    # Emit one build-time flag load failure log so Grafana has exactly one LogID405 entry
    logger.error("FLAG_LOOKUP_FAILED id=%s remote=%s flag=%s", LOG_ID, "build", FLAG)
    start_noise_stream(logger)


@app.on_event("shutdown")
async def shutdown_event() -> None:
    stop_noise_stream()


@app.middleware("http")
async def log_requests(request: Request, call_next):
    logger.info(
        "REQ_START method=%s path=%s query=%s remote=%s",
        request.method,
        request.url.path,
        request.url.query,
        get_remote_addr(request),
    )
    response = await call_next(request)
    METRIC_HTTP_REQUESTS.labels(request.method, request.url.path, str(response.status_code)).inc()
    logger.info(
        "REQ_END method=%s path=%s status=%s",
        request.method,
        request.url.path,
        response.status_code,
    )
    return response


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    write_noise_logs("index", 2)
    update_runtime_metrics()
    your_clicks = current_click_count(request)
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "user": get_current_user(request),
            "your_clicks": your_clicks,
            "fake_hash": FAKE_HASHES["sha256"],
            "click_goal": CLICK_GOAL,
            "click_goal_display": f"{CLICK_GOAL:,}",
            "click_goal_reached": click_goal_reached(your_clicks),
            "clicker_decoy_flag": CLICKER_DECOY_FLAG,
        },
    )


@app.get("/rps", response_class=HTMLResponse)
async def rps_page(request: Request):
    METRIC_FALSE_TRAIL_HITS.labels("rps_page").inc()
    sid, sid_created = get_or_create_sid(request)
    write_noise_logs("rps_page", 3)
    response = templates.TemplateResponse(
        "rps.html",
        {
            "request": request,
            "streak": session_rps_streaks.get(sid, 0),
            "reward": RPS_DECOY_REWARD,
        },
    )
    if sid_created:
        response.set_cookie(SID_COOKIE, sid, httponly=True, samesite="lax")
    return response


@app.post("/api/rps/play")
async def rps_play(request: Request, move: str = Form(default="")):
    METRIC_FALSE_TRAIL_HITS.labels("rps_play").inc()
    player_move = move.strip().lower()
    if player_move not in RPS_MOVES:
        raise HTTPException(status_code=400, detail="choose rock, paper, or scissors")

    sid, sid_created = get_or_create_sid(request)
    server_move, outcome, message = rps_round(player_move)
    if outcome == "win":
        session_rps_streaks[sid] = session_rps_streaks.get(sid, 0) + 1
    elif outcome == "tie":
        session_rps_streaks[sid] = session_rps_streaks.get(sid, 0)
    else:
        session_rps_streaks[sid] = 0

    streak = session_rps_streaks[sid]
    reward = RPS_DECOY_REWARD if streak >= 5 else ""
    if reward:
        message = "Five wins reached. Legacy shadow token unlocked."
    write_noise_logs("rps_play", 3)
    response = JSONResponse(
        {
            "player_move": player_move,
            "server_move": server_move,
            "outcome": outcome,
            "message": message,
            "streak": streak,
            "reward": reward,
        }
    )
    if sid_created:
        response.set_cookie(SID_COOKIE, sid, httponly=True, samesite="lax")
    return response


@app.get("/stats", response_class=HTMLResponse)
async def stats_page(request: Request):
    METRIC_FALSE_TRAIL_HITS.labels("stats_page").inc()
    write_noise_logs("stats_page", 3)
    return templates.TemplateResponse(
        "stats.html",
        {
            "request": request,
            "rows": build_stats_rows(),
            "total_clicks": total_clicks,
        },
    )


@app.get("/api/stats/leaderboard")
async def stats_api():
    METRIC_FALSE_TRAIL_HITS.labels("stats_api").inc()
    write_noise_logs("stats_api", 3)
    return JSONResponse({"rows": build_stats_rows(), "source": "legacy-stats-cache"})


@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request, created: int = 0, logout: int = 0):
    notice = ""
    if created:
        notice = "Account created. You can log in now."
    elif logout:
        notice = "Logged out."
    write_noise_logs("login_page", 2)
    return render_login_page(request, notice=notice)


@app.post("/login")
async def login_submit(
    request: Request,
    username: str = Form(default=""),
    password: str = Form(default=""),
):
    clean_username = username.strip()[:80]
    clean_password = password.strip()[:80]
    if not clean_username or not clean_password:
        return render_login_page(request, error="Username and password are required.")

    query = (
        "SELECT id, username, password, role, bio, created_at "
        f"FROM users WHERE username = '{clean_username}' LIMIT 1"
    )
    try:
        with auth_lock:
            with db_connect() as conn:
                row = conn.execute(query).fetchone()
    except sqlite3.Error:
        logger.warning("LOGIN_SQL_ERROR user=%s remote=%s", clean_username, get_remote_addr(request))
        METRIC_LOGIN_FAILURE.inc()
        write_noise_logs("login_sql_error", 4)
        return render_login_page(request, error="Legacy sqlite auth rejected that payload.")

    if row is None or row["password"] != clean_password:
        logger.info("LOGIN_FAIL user=%s remote=%s", clean_username, get_remote_addr(request))
        METRIC_LOGIN_FAILURE.inc()
        write_noise_logs("login_fail", 3)
        return render_login_page(request, error="Invalid credentials.")

    sid, _ = get_or_create_sid(request)
    response = RedirectResponse(url="/account", status_code=303)
    remember_user(response, sid, row["username"])
    METRIC_LOGIN_SUCCESS.inc()
    update_runtime_metrics()
    logger.info("LOGIN_OK user=%s role=%s remote=%s", row["username"], row["role"], get_remote_addr(request))
    write_noise_logs("login_ok", 3)
    return response


@app.get("/register", response_class=HTMLResponse)
async def register_page(request: Request):
    write_noise_logs("register_page", 2)
    return render_register_page(request)


@app.post("/register")
async def register_submit(
    request: Request,
    username: str = Form(default=""),
    password: str = Form(default=""),
):
    clean_username = username.strip()[:80]
    clean_password = password.strip()[:80]
    if not clean_username or not clean_password:
        return render_register_page(request, error="Username and password are required.")

    insert_sql = (
        "INSERT INTO users (username, password, role, bio, created_at) "
        f"VALUES ('{clean_username}', '{clean_password}', 'user', '', '{now_ts()}');"
    )
    try:
        with auth_lock:
            with db_connect() as conn:
                conn.executescript(insert_sql)
    except sqlite3.IntegrityError:
        logger.info("REGISTER_DUP user=%s remote=%s", clean_username, get_remote_addr(request))
        METRIC_REGISTER_FAILURE.inc()
        write_noise_logs("register_dup", 2)
        return render_register_page(request, error="That username already exists.")
    except sqlite3.Error:
        logger.warning("REGISTER_SQL_ERROR user=%s remote=%s", clean_username, get_remote_addr(request))
        METRIC_REGISTER_FAILURE.inc()
        write_noise_logs("register_sql_error", 4)
        return render_register_page(request, error="Legacy registration backend rejected that payload.")

    METRIC_REGISTER_SUCCESS.inc()
    update_runtime_metrics()
    logger.info("REGISTER_OK user=%s remote=%s", clean_username, get_remote_addr(request))
    write_noise_logs("register_ok", 3)
    return RedirectResponse(url="/login?created=1", status_code=303)


@app.get("/logout")
async def logout(request: Request):
    sid = request.cookies.get(SID_COOKIE)
    if sid:
        auth_sessions.pop(sid, None)
    response = RedirectResponse(url="/login?logout=1", status_code=303)
    response.delete_cookie(SID_COOKIE)
    update_runtime_metrics()
    write_noise_logs("logout", 2)
    return response


@app.get("/account", response_class=HTMLResponse)
async def account(request: Request):
    user = get_current_user(request)
    if user is None:
        return RedirectResponse(url="/login", status_code=303)
    update_runtime_metrics()
    write_noise_logs("account", 3)
    return templates.TemplateResponse("account.html", {"request": request, "user": user})


@app.get("/admin/panel", response_class=HTMLResponse)
async def admin_panel(request: Request):
    user = get_current_user(request)
    if user is None:
        return RedirectResponse(url="/login", status_code=303)
    if user["role"] != "admin":
        raise HTTPException(status_code=403, detail="admin only")
    update_runtime_metrics()
    write_noise_logs("admin_panel", 4)
    return templates.TemplateResponse("admin_panel.html", {"request": request, "user": user})


@app.get("/api/legacy/auth/info")
async def legacy_auth_info():
    METRIC_FALSE_TRAIL_HITS.labels("legacy_jwt_info").inc()
    write_noise_logs("legacy_jwt_info", 3)
    return JSONResponse(
        {
            "kind": "legacy-jwt-mirror",
            "issuer": LEGACY_JWT_ISSUER,
            "alg": "HS256",
            "signature_hash": "SHA-256",
            "token_endpoint": "/api/legacy/auth/token",
            "profile_endpoint": "/api/legacy/me",
            "ops_endpoint": "/api/legacy/ops",
            "reserved_claims": ["super_admin"],
            "note": "legacy ops mirror only; does not grant clicker panel access",
        }
    )


@app.post("/api/legacy/auth/token")
async def legacy_auth_token(request: Request):
    METRIC_FALSE_TRAIL_HITS.labels("legacy_jwt_token").inc()
    username, password = await read_legacy_auth_payload(request)
    if not username or not password:
        write_noise_logs("legacy_jwt_token_empty", 2)
        raise HTTPException(status_code=400, detail="username and password are required")

    user = get_user_by_username(username)
    if user is None or user["password"] != password:
        logger.info("LEGACY_JWT_LOGIN_FAIL user=%s remote=%s", username, get_remote_addr(request))
        write_noise_logs("legacy_jwt_login_fail", 3)
        raise HTTPException(status_code=401, detail="invalid credentials")

    token = build_legacy_jwt(user)
    logger.info("LEGACY_JWT_LOGIN_OK user=%s remote=%s", username, get_remote_addr(request))
    write_noise_logs("legacy_jwt_login_ok", 3)
    return JSONResponse(
        {
            "token": token,
            "token_type": "bearer",
            "profile_endpoint": "/api/legacy/me",
            "ops_endpoint": "/api/legacy/ops",
            "reserved_claim_hint": "super_admin",
        }
    )


@app.get("/api/legacy/me")
async def legacy_me(request: Request):
    METRIC_FALSE_TRAIL_HITS.labels("legacy_jwt_me").inc()
    claims = get_legacy_claims(request)
    write_noise_logs("legacy_jwt_me", 3)
    return JSONResponse(
        {
            "subject": claims.get("sub", "unknown"),
            "role": claims.get("role", "user"),
            "scope": claims.get("scope", []),
            "super_admin": bool(claims.get("super_admin", False)),
            "note": "legacy JWT claims are not connected to the clicker web panel",
        }
    )


@app.get("/api/legacy/ops")
async def legacy_ops(request: Request):
    METRIC_FALSE_TRAIL_HITS.labels("legacy_jwt_ops").inc()
    claims = get_legacy_claims(request)
    write_noise_logs("legacy_jwt_ops", 4)
    if not claims.get("super_admin", False):
        raise HTTPException(status_code=403, detail="super_admin claim required")

    return JSONResponse(
        {
            "mirror": "ops-shadow-panel",
            "super_admin": True,
            "jobs": [],
            "exports": [],
            "flag": None,
            "note": "legacy mirror is empty and does not control Grafana or clicker admin",
        }
    )


@app.post("/api/click")
async def click(request: Request):
    sid, sid_created = get_or_create_sid(request)
    with click_lock:
        user_clicks = session_clicks.get(sid, 0) + 1
        session_clicks[sid] = user_clicks
        goal_reached = user_clicks >= CLICK_GOAL
        
        # Save to database for persistence
        with db_connect() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO session_clicks (sid, clicks, updated_at) VALUES (?, ?, ?)",
                (sid, user_clicks, now_ts())
            )
            conn.commit()
    
    user = get_current_user(request)
    username = user["username"] if user else "anonymous"
    logger.info(
        "CLICK user=%s mine=%s remote=%s goal_reached=%s",
        username,
        user_clicks,
        get_remote_addr(request),
        goal_reached,
    )
    METRIC_CLICK_REQUESTS.inc()
    if goal_reached:
        METRIC_FAKE_REWARD.inc()
    update_runtime_metrics()
    write_noise_logs("click", 3)

    response = JSONResponse(
        {
            "your_clicks": user_clicks,
            "goal": CLICK_GOAL,
            "goal_reached": goal_reached,
            "reward": CLICKER_DECOY_FLAG if goal_reached else "",
        }
    )
    if sid_created:
        response.set_cookie(SID_COOKIE, sid, httponly=True, samesite="lax")
    return response


@app.get("/flag")
async def flag(request: Request):
    write_noise_logs("flag", 6)
    return PlainTextResponse(f"{{ERROR, Couldn't get the flag nskctf{{..., {LOG_ID}}}", status_code=500)


@app.get("/chat", response_class=HTMLResponse)
async def chat(request: Request):
    with message_lock:
        all_messages = list(CHAT_MESSAGES)
    update_runtime_metrics()
    logger.info("CHAT_VIEW shown=%d remote=%s", len(all_messages), get_remote_addr(request))
    write_noise_logs("chat_view", 5)
    return templates.TemplateResponse(
        "chat.html",
        {
            "request": request,
            "messages": all_messages,
            "attachments": attachments_list(),
        },
    )


@app.post("/chat/message")
async def chat_message(
    request: Request,
    nickname: str = Form(default=""),
    message: str = Form(default=""),
):
    clean_nickname = nickname.strip()[:24]
    clean_message = message.strip()[:240]
    if not clean_nickname or not clean_message:
        write_noise_logs("chat_post_invalid", 2)
        return RedirectResponse(url="/chat", status_code=303)

    with message_lock:
        CHAT_MESSAGES.append({"author": clean_nickname, "time": now_ts(), "text": clean_message})

    METRIC_CHAT_POSTS.inc()
    update_runtime_metrics()
    logger.info("CHAT_POST nick=%s len=%d remote=%s", clean_nickname, len(clean_message), get_remote_addr(request))
    write_noise_logs("chat_post", 3)
    return RedirectResponse(url="/chat", status_code=303)


@app.get("/chat/live-preview")
async def chat_live_preview(message: str = ""):
    METRIC_FALSE_TRAIL_HITS.labels("chat_live_preview").inc()
    write_noise_logs("chat_live_preview", 3)
    preview = message or "<em>Preview appears here.</em>"
    html = f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Chat Live Preview</title>
    <link rel="stylesheet" href="/static/style.css" />
  </head>
  <body class="auth-body">
    <main class="auth-card account-card">
      <div class="chat-top">
        <h1>Live Preview</h1>
        <a href="/chat" class="chat-link">Back to chat</a>
      </div>
      <p class="auth-muted">Legacy preview renders raw HTML from the message body. It is detached from production chat storage.</p>
      <form method="get" action="/chat/live-preview" class="auth-form">
        <input name="message" maxlength="240" placeholder="Type HTML or script here" autocomplete="off" />
        <button type="submit">Render Preview</button>
      </form>
      <section class="blank-panel">
        <h2>Rendered Output</h2>
        <div class="preview-output">{preview}</div>
      </section>
      <p class="auth-muted">Note: the main session cookie is still HttpOnly and the preview origin has no useful admin actions.</p>
    </main>
  </body>
</html>"""
    return HTMLResponse(html)


@app.get("/chat/badge-preview", response_class=HTMLResponse)
async def chat_badge_preview(request: Request, tpl: str = "{{ nickname }} :: {{ status }}"):
    METRIC_FALSE_TRAIL_HITS.labels("chat_badge_preview").inc()
    template_input = tpl[:160]
    try:
        rendered = BADGE_PREVIEW_ENV.from_string(template_input).render(BADGE_PREVIEW_CONTEXT)
    except Exception as exc:
        rendered = f"template error: {exc}"
    write_noise_logs("chat_badge_preview", 3)
    return templates.TemplateResponse(
        "badge_preview.html",
        {
            "request": request,
            "template_input": template_input,
            "rendered": rendered,
            "context_keys": sorted(BADGE_PREVIEW_CONTEXT.keys()),
        },
    )


@app.get("/chat/file")
async def chat_file(request: Request, name: str = ""):
    if not name:
        raise HTTPException(status_code=400, detail="filename is required")

    requested_path = (ATTACHMENTS_DIR / name).resolve()
    attachments_dir_resolved = ATTACHMENTS_DIR.resolve()
    config_file_path = (BASE_DIR / "config" / "monitoring.env").resolve()

    remote = get_remote_addr(request)
    logger.info("FILE_READ_ATTEMPT name=%s path=%s remote=%s", name, requested_path, remote)
    write_noise_logs("file_read", 4)

    allowed = False
    try:
        requested_path.relative_to(attachments_dir_resolved)
        allowed = True
    except ValueError:
        if requested_path == config_file_path:
            allowed = True
    if not allowed:
        logger.warning("PATH_TRAVERSAL_BLOCKED name=%s remote=%s", name, remote)
        raise HTTPException(status_code=403, detail="access denied")

    if not requested_path.exists() or not requested_path.is_file():
        logger.warning("FILE_READ_FAIL name=%s remote=%s", name, remote)
        raise HTTPException(status_code=404, detail="file not found")
    logger.info("FILE_READ_OK name=%s remote=%s", name, remote)
    return FileResponse(path=requested_path)


@app.get("/api/flag-hash")
async def flag_hash():
    METRIC_FALSE_TRAIL_HITS.labels("api_flag_hash").inc()
    write_noise_logs("flag_hash", 3)
    return JSONResponse(
        {
            "algorithm": "sha256",
            "value": FAKE_HASHES["sha256"],
            "md5": FAKE_HASHES["md5"],
            "source": "legacy-frontend-verifier",
        }
    )


@app.get("/debug/config")
async def debug_config():
    METRIC_FALSE_TRAIL_HITS.labels("debug_config").inc()
    write_noise_logs("debug_config", 3)
    return JSONResponse(
        {
            "legacy_auth_driver": "sqlite",
            "default_staff_user": "admin",
            "legacy_hash_endpoint": "/api/flag-hash",
            "legacy_jwt_info_endpoint": "/api/legacy/auth/info",
            "legacy_jwt_token_endpoint": "/api/legacy/auth/token",
            "legacy_jwt_me_endpoint": "/api/legacy/me",
            "legacy_jwt_ops_endpoint": "/api/legacy/ops",
            "rps_endpoint": "/rps",
            "stats_endpoint": "/stats",
            "stats_api_endpoint": "/api/stats/leaderboard",
            "chat_live_preview_endpoint": "/chat/live-preview",
            "chat_badge_preview_endpoint": "/chat/badge-preview",
            "recent_ids_endpoint": "/api/recent-ids",
            "reward_proof_endpoint": "/api/reward-proof",
            "sessions_debug_endpoint": "/debug/sessions",
            "auth_dump_endpoint": "/exports/auth.dump",
            "observability_migration_endpoint": "/api/observability-migration",
            "incident_archive_endpoint": "/archive/incident-405",
            "note": "remove before prod",
        }
    )


@app.get("/api/recent-ids")
async def recent_ids():
    METRIC_FALSE_TRAIL_HITS.labels("recent_ids").inc()
    write_noise_logs("recent_ids", 3)
    return JSONResponse({"ids": FALSE_RECENT_IDS, "source": "synthetic-checks"})


@app.get("/backup/users.txt", response_class=PlainTextResponse)
async def users_backup():
    METRIC_FALSE_TRAIL_HITS.labels("backup_users").inc()
    write_noise_logs("users_backup", 3)
    return "\n".join(
        [
            "admin:21232f297a57a5a743894a0e4a801fc3",
            "analyst:5ebe2294ecd0e0f08eab7690d2a6ee69",
            "demo:e10adc3949ba59abbe56e057f20f883e",
        ]
    )


@app.get("/api/reward-proof")
async def reward_proof():
    METRIC_FALSE_TRAIL_HITS.labels("reward_proof").inc()
    write_noise_logs("reward_proof", 3)
    return JSONResponse(
        {
            "goal": CLICK_GOAL,
            "proof_type": "milestone-merkle",
            "proof_root": "87dc5b4f260f0e4e5d2c76fe8f3ab791f705f7ae6db15d4af4f2f7c2d54a0b8a",
            "reward_flag": CLICKER_DECOY_FLAG,
            "status": "legacy-click-reward",
        }
    )


@app.get("/debug/sessions")
async def debug_sessions():
    METRIC_FALSE_TRAIL_HITS.labels("debug_sessions").inc()
    write_noise_logs("debug_sessions", 3)
    return JSONResponse(
        {
            "active_snapshot": {
                "admin": FAKE_SESSION_IDS["admin"],
                "analyst": FAKE_SESSION_IDS["analyst"],
            },
            "expires_hint": "2026-03-28T23:59:59Z",
            "origin": "legacy-session-recorder",
        }
    )


@app.get("/exports/auth.dump", response_class=PlainTextResponse)
async def auth_dump():
    METRIC_FALSE_TRAIL_HITS.labels("auth_dump").inc()
    write_noise_logs("auth_dump", 3)
    return "\n".join(
        [
            "BEGIN TRANSACTION;",
            "CREATE TABLE legacy_users(id INTEGER PRIMARY KEY, username TEXT, digest TEXT, salt TEXT);",
            "INSERT INTO legacy_users VALUES(1,'admin','5f4dcc3b5aa765d61d8327deb882cf99','old-static-salt');",
            "INSERT INTO legacy_users VALUES(2,'analyst','6cb75f652a9b52798eb6cf2201057c73','old-static-salt');",
            "COMMIT;",
        ]
    )


@app.get("/api/observability-migration")
async def observability_migration():
    METRIC_FALSE_TRAIL_HITS.labels("observability_migration").inc()
    write_noise_logs("observability_migration", 3)
    return JSONResponse(
        {
            "current_target": "kibana",
            "planned_port": 5601,
            "grafana_status": "deprecated-soon",
            "note": "migration plan is stale and was never completed",
        }
    )


@app.get("/archive/incident-405", response_class=PlainTextResponse)
async def incident_archive():
    METRIC_FALSE_TRAIL_HITS.labels("incident_archive").inc()
    write_noise_logs("incident_archive", 3)
    return "\n".join(
        [
            "Incident 405 archive (sanitized)",
            "Observed IDs: LogID431, LogID452, LogID488",
            "Root cause: synthetic alert storm in staging",
            "Resolution: ignored as duplicate noise",
            "Attachment: archived without secrets",
        ]
    )


@app.get("/robots.txt", response_class=PlainTextResponse)
async def robots():
    METRIC_FALSE_TRAIL_HITS.labels("robots_txt").inc()
    return "\n".join(
        [
            "User-agent: *",
            "Disallow: /rps",
            "Disallow: /stats",
            "Disallow: /chat/live-preview",
            "Disallow: /chat/badge-preview",
            "Disallow: /debug/config",
            "Disallow: /backup/users.txt",
            "Disallow: /api/flag-hash",
            "Disallow: /api/legacy/auth/info",
            "Disallow: /api/legacy/ops",
            "Disallow: /api/reward-proof",
            "Disallow: /debug/sessions",
            "Disallow: /exports/auth.dump",
            "Disallow: /api/observability-migration",
            "Disallow: /archive/incident-405",
            "Disallow: /admin/panel",
        ]
    )


@app.get("/metrics")
async def metrics():
    update_runtime_metrics()
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/health", response_class=PlainTextResponse)
async def health():
    write_noise_logs("health", 2)
    return "ok"


def bootstrap_noise() -> None:
    for index in range(60):
        logger.info(
            "STREAM_A_BOOT seq=%d id=%s subsystem=%s status=%s",
            index,
            fake_log_id(),
            random.choice(["collector", "router", "dashboard", "worker", "notify", "auth"]),
            random.choice(["ok", "warmup", "skip"]),
        )


init_db()
update_runtime_metrics()
bootstrap_noise()

if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=5000)
