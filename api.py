import os
import csv
import json
import time
import shlex
import asyncio
import datetime as dt
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from flask import Flask, request, jsonify
from flask_cors import CORS

# ---- Database (MariaDB) ----
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER", "chatuser")
DB_PASS = os.getenv("DB_PASS", "strongpassword")
DB_NAME = os.getenv("DB_NAME", "chatbot")

USE_DB = True
try:
    import pymysql
    db_conn = pymysql.connect(host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASS, database=DB_NAME, autocommit=True)
    with db_conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS conversations (
                id INT AUTO_INCREMENT PRIMARY KEY,
                session_id VARCHAR(64),
                provider VARCHAR(50),
                message TEXT,
                type ENUM('user','bot','brainstorm') DEFAULT 'bot',
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)
    print("[DB] Connected.")
except Exception as e:
    print("[DB] Disabled (", e, ")")
    USE_DB = False
    db_conn = None

# ---- Files / Datasets ----
DATASET_DIR = Path(os.getenv("DATASET_DIR", "/var/www/htdocs/datasets"))
DATASET_DIR.mkdir(parents=True, exist_ok=True)

# ---- Providers and tgpt ----
VALID_PROVIDERS = ["pollinations", "sky", "phind", "koboldai", "kimi"]
GROUP_LIST = VALID_PROVIDERS[:]  # group = all listed

TGPT_BIN = os.getenv("TGPT_BIN", "tgpt")
TGPT_TIMEOUT = int(os.getenv("TGPT_TIMEOUT", "75"))  # seconds

def _now_str():
    return dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def save_to_db(session_id: str, provider: str, message: str, mtype: str):
    if not USE_DB: return
    try:
        with db_conn.cursor() as cur:
            cur.execute(
                "INSERT INTO conversations (session_id, provider, message, type, timestamp) VALUES (%s,%s,%s,%s,%s)",
                (session_id, provider, message, mtype, _now_str())
            )
    except Exception as e:
        print("[DB] insert failed:", e)

def fetch_history(session_id: str, limit: int = 500):
    if not USE_DB: return []
    try:
        with db_conn.cursor() as cur:
            cur.execute(
                "SELECT provider, message, DATE_FORMAT(timestamp,'%%Y-%%m-%%d %%H:%%i:%%s') FROM conversations WHERE session_id=%s ORDER BY id ASC LIMIT %s",
                (session_id, limit)
            )
            rows = cur.fetchall()
            return [{"provider": r[0], "message": r[1], "timestamp": r[2]} for r in rows]
    except Exception as e:
        print("[DB] history failed:", e)
        return []

def dataset_path(filename: str, fmt: str):
    safe_name = (filename or f"dataset_{int(time.time())}").strip().replace("/", "_")
    ext = ".csv" if fmt == "csv" else ".json"
    return DATASET_DIR / f"{safe_name}{ext}"

def append_dataset_lines(fp: Path, session_id: str, fmt: str, entries):
    if fmt == "csv":
        exists = fp.exists()
        with fp.open("a", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            if not exists:
                writer.writerow(["timestamp","session_id","provider","message","type"])
            for e in entries:
                writer.writerow([e["timestamp"], session_id, e["provider"], e["message"], e.get("type","bot")])
    else:
        # JSON lines (one object per line)
        with fp.open("a", encoding="utf-8") as f:
            for e in entries:
                obj = {"timestamp": e["timestamp"], "session_id": session_id, "provider": e["provider"], "message": e["message"], "type": e.get("type","bot")}
                f.write(json.dumps(obj, ensure_ascii=False) + "\n")

def append_dataset_dialog(fp: Path, session_id: str, fmt: str, dialog_list):
    payload = {"session_id": session_id, "dialog": dialog_list, "saved_at": _now_str()}
    if fmt == "csv":
        # store the whole dialog as one CSV row (json encoded)
        exists = fp.exists()
        with fp.open("a", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            if not exists:
                writer.writerow(["session_id","saved_at","dialog_json"])
            writer.writerow([session_id, payload["saved_at"], json.dumps(dialog_list, ensure_ascii=False)])
    else:
        # JSON: append one object per dialog
        with fp.open("a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")

def build_brainstorm_prompt(messages):
    # messages: list of {"provider": ..., "reply": ...}
    lines = []
    for m in messages[-12:]:  # last 12 to keep it manageable
        prov = m.get("provider","unknown")
        txt = m.get("reply","").strip()
        if not txt: continue
        lines.append(f"{prov}: {txt}")
    guidance = (
        "You are in a multi-bot brainstorming. "
        "Improve and refine ideas; don't repeat verbatim. "
        "Be concise but thorough. If code is needed, include it. "
        "Cite which bot you're responding to when relevant. "
        "⚙️ Coding Guidelines:"
        "- Never resend the entire file unless explicitly asked.  "
        "- Always suggest localized changes:"
        "  • “Insert below line 120 …”  "
        "  • “Replace line 85 with …”  "
        "  • “Add this block before function main()”  "
        "- If line numbers may differ, use keyword anchors:  "
        "  • “Insert after the line containing `def clear_history`”.  "
        "- If multiple edits are required, list them step by step.  "
        "- Keep diffs small, precise, and easy to apply.  "
        "🎯 Goal:"
        "- Collaborate constructively.  "
        "- Avoid duplication, instead build on each other's points.  "
        "- Keep memory efficient: no unnecessary repetition.  "

    )
    return guidance + "\n\nContext so far:\n" + "\n".join(lines) + "\n\nYour improved contribution:"

def run_tgpt(provider: str, prompt: str):
    # Build command
    # Example: tgpt -w --provider sky "prompt"
    cmd = [TGPT_BIN, "-w", "--provider", provider, prompt]
    try:
        out = asyncio.run(asyncio.wait_for(asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        ), timeout=TGPT_TIMEOUT))
        # Gather output
        async def _collect(proc):
            stdout, stderr = await proc.communicate()
            return stdout.decode("utf-8", errors="ignore").strip(), stderr.decode("utf-8", errors="ignore").strip()
        stdout, stderr = asyncio.run(_collect(out))
        if stderr and not stdout:
            return f"[{provider} error] {stderr[:800]}"
        return stdout or f"[{provider}] (no output)"
    except Exception as e:
        return f"[{provider} error] {str(e)}"

# Fallback: thread pool subprocess for compatibility environments
def run_tgpt_blocking(provider: str, prompt: str):
    import subprocess
    try:
        res = subprocess.run(
            [TGPT_BIN, "-w", "--provider", provider, prompt],
            capture_output=True, text=True, timeout=TGPT_TIMEOUT
        )
        text = (res.stdout or "").strip()
        if not text:
            err = (res.stderr or "").strip()
            text = f"[{provider} error] {err[:800]}" if err else f"[{provider}] (no output)"
        return text
    except Exception as e:
        return f"[{provider} error] {str(e)}"

executor = ThreadPoolExecutor(max_workers=5)

async def ask_providers_parallel(providers, prompt: str):
    loop = asyncio.get_event_loop()
    tasks = []
    for p in providers:
        tasks.append(loop.run_in_executor(executor, run_tgpt_blocking, p, prompt))
    results = await asyncio.gather(*tasks, return_exceptions=True)
    out = []
    for p, r in zip(providers, results):
        if isinstance(r, Exception):
            text = f"[{p} error] {str(r)}"
        else:
            text = str(r)
        out.append({"provider": p, "reply": text, "timestamp": _now_str()})
    return out

# ---- Flask ----
app = Flask(__name__)
CORS(app)

@app.route("/history", methods=["GET"])
def history():
    session_id = request.args.get("session_id", "").strip()
    if not session_id:
        return ("session_id required", 400)
    hist = fetch_history(session_id) if USE_DB else []
    return jsonify({"history": hist})

@app.route("/ask", methods=["POST"])
def ask():
    session_id = (request.form.get("session_id") or "default").strip()
    provider = (request.form.get("provider") or "phind").strip()
    query = (request.form.get("query") or "").strip()

    save_dataset = str(request.form.get("save_dataset","0")).strip() in ("1","true","True","yes","on")
    save_format = (request.form.get("save_format") or "csv").strip().lower()
    save_shape = (request.form.get("save_shape") or "lines").strip().lower()
    filename = (request.form.get("filename") or "").strip()

    if not query and "file" not in request.files:
        return jsonify({"error":"empty query"}), 200

    # record user message
    if query:
        save_to_db(session_id, "user", query, "user")

    # Provider handling
    if provider == "group":
        providers = GROUP_LIST
    else:
        if provider not in VALID_PROVIDERS:
            return jsonify({"error": f"invalid provider '{provider}'"}), 200
        providers = [provider]

    # Make a concise guidance to avoid flooding
    guidance = (
        "Please answer clearly. If code is needed include it. Avoid repeating earlier content verbatim. "
        "If you refer to another bot, name it. Keep replies focused."
    )
    prompt = query if not query else f"{query}\n\n{guidance}"

    # Parallel ask
    replies = asyncio.run(ask_providers_parallel(providers, prompt))

    # Save replies
    entries = []
    for item in replies:
        provider_name = item["provider"]
        msg = item["reply"]
        ts = item["timestamp"]
        save_to_db(session_id, provider_name, msg, "bot")
        entries.append({"provider": provider_name, "message": msg, "timestamp": ts, "type":"bot"})

    # Dataset save (optional)
    saved = None
    if save_dataset:
        fp = dataset_path(filename, save_format)
        if save_shape == "dialog":
            # fetch full session dialog (user+bot) for a snapshot
            dialog = fetch_history(session_id) if USE_DB else entries
            append_dataset_dialog(fp, session_id, save_format, dialog)
        else:
            append_dataset_lines(fp, session_id, save_format, entries)
        saved = str(fp)

    # If single provider, flatten
    if len(replies) == 1:
        resp = {**replies[0]}
        if saved: resp["saved_to"] = saved
        return jsonify(resp)

    return jsonify({"multi": replies, **({"saved_to": saved} if saved else {})})

@app.route("/clear-history", methods=["POST"])
def clear_history():
    session_id = request.form.get("session_id")
    try:
        with db_conn.cursor() as cur:
            cur.execute("DELETE FROM conversations WHERE session_id = %s", (session_id,))
        return jsonify({"message": "History cleared successfully"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/brainstorm", methods=["POST"])
def brainstorm():
    data = request.get_json(force=True, silent=True) or {}
    session_id = (data.get("session_id") or "default").strip()
    providers = data.get("providers") or ["group"]
    messages = data.get("messages") or []
    save_dataset = bool(data.get("save_dataset"))
    save_format = (data.get("save_format") or "csv").strip().lower()
    save_shape = (data.get("save_shape") or "lines").strip().lower()
    filename = (data.get("filename") or "").strip()

    # Resolve providers
    if providers == ["group"] or (len(providers)==1 and providers[0]=="group"):
        providers = GROUP_LIST
    else:
        providers = [p for p in providers if p in VALID_PROVIDERS]
        if not providers:
            providers = GROUP_LIST

    prompt = build_brainstorm_prompt(messages)

    replies = asyncio.run(ask_providers_parallel(providers, prompt))

    # Save to DB and dataset
    entries = []
    for item in replies:
        provider_name = item["provider"]
        msg = item["reply"]
        ts = item["timestamp"]
        save_to_db(session_id, provider_name, msg, "brainstorm")
        entries.append({"provider": provider_name, "message": msg, "timestamp": ts, "type":"brainstorm"})

    saved = None
    if save_dataset:
        fp = dataset_path(filename, save_format)
        if save_shape == "dialog":
            dialog = fetch_history(session_id) if USE_DB else entries
            append_dataset_dialog(fp, session_id, save_format, dialog)
        else:
            append_dataset_lines(fp, session_id, save_format, entries)
        saved = str(fp)

    return jsonify({"multi": replies, **({"saved_to": saved} if saved else {})})

if __name__ == "__main__":
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "8080"))
    debug = os.getenv("DEBUG", "1") == "1"
    app.run(host=host, port=port, debug=debug)
