# Doma PulseBot (Production Guide)

Enterprise-ready Telegram bot for high-margin domain opportunity alerts, with official API-first integration, adaptive scheduling, and anti-ban request controls.

## 1) Zero-to-Hero Setup (Beginner Friendly)

### Prerequisites
- Linux/macOS terminal (or WSL on Windows)
- Python 3.10+
- Git

Check versions:

```bash
python3 --version
git --version
```

### Step A — Clone the repository

```bash
git clone https://github.com/Eslam-tech5/doma-pulsebot.git
cd doma-pulsebot
```

### Step B — Create and activate virtual environment

```bash
python3 -m venv .venv
source .venv/bin/activate
```

You should now see `(.venv)` in your terminal prompt.

### Step C — Install dependencies

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

### Step D — Create environment file

```bash
cp .env.example .env
```

Open `.env` and fill all required values.

### Step E — Run locally

```bash
python bot.py
```

## 2) Required Environment Variables

Create `.env` from `.env.example` and set all values:

```dotenv
# =========================
# Telegram
# =========================
TELEGRAM_TOKEN=YOUR_TELEGRAM_BOT_TOKEN
DEFAULT_CHAT_ID=YOUR_TELEGRAM_CHAT_ID

# =========================
# Official Domain Data Sources
# =========================
GODADDY_API_BASE_URL=https://api.godaddy.com
GODADDY_API_KEY=YOUR_GODADDY_API_KEY
GODADDY_API_SECRET=YOUR_GODADDY_API_SECRET
GODADDY_CHECK_TYPE=FAST

# =========================
# Watcher Filters
# =========================
ALLOWED_TLDS=.ae,.tech,.my,.com,.app
MAX_DOMAINS_PER_CYCLE=200
ALERT_DB_PATH=alerts.db

# =========================
# Pricing / Opportunity Rules
# =========================
ARBITRAGE_MIN_GAP_USD=20
ARBITRAGE_MIN_RATIO=1.8
MIN_BRANDABILITY_SCORE=35
HIGH_VALUE_KEYWORDS=ai,crypto,cloud,data,dev,app,bot,pay,trade,labs
NEGATIVE_KEYWORDS=gibberish,unbrandable,unpronounceable,nonsense,meaningless,awkward,spammy
APPRAISAL_CACHE_TTL_SECONDS=86400
APPRAISAL_BATCH_SIZE=10
APPRAISAL_CONCURRENCY=5
SCAN_CONCURRENCY=50
VIP_RELOAD_SECONDS=3600

# =========================
# Scheduling (Golden Hours UTC)
# =========================
# Legacy/default poll
WATCHER_POLL_SECONDS=30
# Eco mode (outside peak windows)
ECO_POLL_SECONDS=120
# Turbo mode (inside peak windows)
TURBO_POLL_SECONDS=8
# Comma-separated hour ranges in UTC (start-end), supports wrap windows
# Same-hour ranges (e.g., 18-18) are auto-converted to one-hour windows (18-19).
TURBO_HOURS_UTC=18-21

# =========================
# Reliability / Anti-Ban Controls
# =========================
HTTP_TIMEOUT_SECONDS=20
PROXY_URL=
HUMAN_DELAY_MIN_SECONDS=0.8
HUMAN_DELAY_MAX_SECONDS=2.5
MAX_RETRY_ATTEMPTS=4
RETRY_BASE_SECONDS=1.2
MAX_BACKOFF_SECONDS=45
QUOTA_COOLDOWN_SECONDS=180
CIRCUIT_BREAKER_FAILURE_THRESHOLD=4
CIRCUIT_BREAKER_OPEN_SECONDS=120
```

## 3) Run 24/7 in Production

### Option A — nohup (simple)

```bash
cd /absolute/path/to/doma-pulsebot
source .venv/bin/activate
# Optional safety check: verify a bot process is not already running before starting another one.
ps aux | grep "python bot.py" | grep -v grep
nohup python bot.py > bot.log 2>&1 &
echo $! > bot.pid
```

Stop:

```bash
kill "$(cat bot.pid)"
```

### Option B — tmux (interactive)

```bash
cd /absolute/path/to/doma-pulsebot
tmux new -s pulsebot
source .venv/bin/activate
python bot.py
```

Detach: `Ctrl+B` then `D`  
Reattach:

```bash
tmux attach -t pulsebot
```

### Option C — systemd (recommended for servers)

Create `/etc/systemd/system/doma-pulsebot.service`:

```ini
[Unit]
Description=Doma PulseBot
After=network.target

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/absolute/path/to/doma-pulsebot
EnvironmentFile=/absolute/path/to/doma-pulsebot/.env
ExecStart=/absolute/path/to/doma-pulsebot/.venv/bin/python /absolute/path/to/doma-pulsebot/bot.py
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

Enable and start:

```bash
sudo systemctl daemon-reload
sudo systemctl enable doma-pulsebot
sudo systemctl start doma-pulsebot
sudo systemctl status doma-pulsebot
```

Logs:

```bash
sudo journalctl -u doma-pulsebot -f
```

## 4) Operational Notes

- The watcher now uses **UTC Golden Hours** (`TURBO_HOURS_UTC`) for turbo polling.
- Same-hour `TURBO_HOURS_UTC` entries (like `18-18`) are auto-converted to a 1-hour window (`18-19`).
- Outside Golden Hours, it uses eco polling to preserve quotas.
- Requests include full-jitter retry backoff + silent 429 cooldown handling + GoDaddy circuit breaker protection.
- Keep API keys private and rotate keys if any leak is suspected.
