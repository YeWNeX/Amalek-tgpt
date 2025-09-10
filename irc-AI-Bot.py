import requests
import irc.bot
import irc.strings
from irc.client import ip_numstr_to_quad, ip_quad_to_numstr
import pymysql
import datetime

# --- IRC / Bot Config ---
SERVER = "irc.dal.net"
PORT = 6667
CHANNELS = ["#Amalek", "#FreeEgypt"]
NICK = "SkyBot"
REALNAME = "Beagles"
API_URL = "http://127.0.0.1:8080/ask"  # Flask API

# --- DB Config ---
DB_HOST = "localhost"
DB_PORT = 3306
DB_USER = "chatuser"
DB_PASS = "strongpassword"
DB_NAME = "chatbot"

# --- Max line length for IRC ---
IRC_MAX_LINE = 400  # conservative to avoid exceeding 512 bytes

# --- Utilities ---
def now_str():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# Connect to DB
try:
    db_conn = pymysql.connect(
        host=DB_HOST, port=DB_PORT, user=DB_USER,
        password=DB_PASS, database=DB_NAME, autocommit=True
    )
    print("[DB] Connected successfully.")
except Exception as e:
    print("[DB] Connection failed:", e)
    db_conn = None

# --- Memory helpers ---
def save_fact(session_id, provider, message, mtype="bot"):
    if not db_conn:
        return
    try:
        with db_conn.cursor() as cur:
            cur.execute(
                "INSERT INTO memory (session_id, provider, message, type, timestamp) "
                "VALUES (%s,%s,%s,%s,%s)",
                (session_id, provider, message, mtype, now_str())
            )
    except Exception as e:
        print("[DB] save_fact error:", e)

def recall_facts(session_id, limit=50):
    if not db_conn:
        return []
    try:
        with db_conn.cursor() as cur:
            cur.execute(
                "SELECT provider, message FROM memory WHERE session_id=%s "
                "ORDER BY id DESC LIMIT %s", (session_id, limit)
            )
            return [{"provider": r[0], "message": r[1]} for r in cur.fetchall()]
    except Exception as e:
        print("[DB] recall_fact error:", e)
        return []

# --- IRC Bot ---
class SkyBot(irc.bot.SingleServerIRCBot):
    def __init__(self):
        irc.bot.SingleServerIRCBot.__init__(self, [(SERVER, PORT)], NICK, REALNAME)

    def on_welcome(self, c, e):
        for channel in CHANNELS:
            c.join(channel)
            print(f"[DEBUG] Joined channel: {channel}")

    def on_pubmsg(self, c, e):
        user_msg = e.arguments[0].strip()
        print(f"[DEBUG] Public message from {e.source.nick}: {user_msg}")
        self.handle_message(c, e.target, e.source.nick, user_msg)

    def on_privmsg(self, c, e):
        user_msg = e.arguments[0].strip()
        print(f"[DEBUG] Private message from {e.source.nick}: {user_msg}")
        self.handle_message(c, e.source.nick, e.source.nick, user_msg)

    def handle_message(self, c, target, nick, user_msg):
        try:
            # Recall memory and append to message if needed
            past_facts = recall_facts(nick)
            memory_context = "\n".join([f"{f['provider']}: {f['message']}" for f in past_facts[-12:]])
            full_query = f"{user_msg}\n\nMemory context:\n{memory_context}" if memory_context else user_msg

            payload = {
                "session_id": nick,
                "provider": "sky",
                "query": full_query
            }
            print("[DEBUG] Sending payload to API:", payload)
            r = requests.post(API_URL, data=payload, timeout=60)
            print("[DEBUG] API status:", r.status_code)
            print("[DEBUG] API raw response:", r.text)

            if r.status_code == 200:
                data = r.json()
                reply = data.get("reply") or data.get("message") or ""
                if reply:
                    save_fact(nick, "sky", reply)
                    # Split multi-line messages to avoid carriage return issues
                    lines = reply.replace("\r","").split("\n")
                    for line in lines:
                        for chunk_start in range(0, len(line), IRC_MAX_LINE):
                            chunk = line[chunk_start:chunk_start+IRC_MAX_LINE]
                            c.privmsg(target, chunk)
                else:
                    c.privmsg(target, "No reply from API.")
            else:
                c.privmsg(target, f"API error {r.status_code}")
        except Exception as ex:
            print("[DEBUG] Exception:", str(ex))
            c.privmsg(target, f"Error: {ex}")

# --- Start Bot ---
if __name__ == "__main__":
    bot = SkyBot()
    bot.start()
