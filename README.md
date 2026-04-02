
# 🤖 Doma PulseBot

A real-time Telegram bot for **domain sale alerts on the Doma Protocol** — built for **Track 3: Bots & Event Subscriptions** of the Doma Hackathon 2025.

---

## 🧠 What It Does

**Doma PulseBot** now tracks real-world domain opportunities and sends **real-time alerts** on Telegram — with:

- 🔔 Auto-updating alerts every 30 seconds  
- 🌐 Domain name, price, source, and buy link included  
- 🎯 Laser filters for `.app`, `.dev`, `.com` + keyword + length rules
- 🔥 HOT DEAL scoring for near-reg premium keywords / deep discounts
- 🧠 SQLite dedupe to avoid repeat alerts
- 🎛 Inline action buttons for instant buy + Whois lookup
- ✅ `/start` to subscribe  
- 🛑 `/unsubscribe` to stop  
- ⚙️ `/filter` to customize (expandable)  
- 📊 `/stats` for market snapshot  

---

## 🎯 Track: Bots & Event Subscriptions

> *“Develop automated bots or subscription services for domain alerts… integrating Doma for on-chain notifications to drive user acquisition, txns, and community engagement.”*

### ✅ How It Fits

| Feature                  | Included |
|--------------------------|----------|
| Automated alerts         | ✅        |
| Subscription system      | ✅        |
| Telegram integration     | ✅        |
| Doma domain events       | ✅ (simulated) |
| Direct buy links         | ✅        |
| Ready for on-chain API   | ✅ Easily pluggable |

---

## 🛠 Tech Stack

- Python 3.10+
- `python-telegram-bot` v20+
- `asyncio` background polling
- `.env` for token security
- Simulated domain events

---

## 🔍 Real Data Sources (Watcher)

The watcher replaces the old mock generator and supports:

- **GoDaddy Availability API** (real-time availability + price checks)
- **ExpiredDomains source URL** via ethical scraping (HTML parsing with BeautifulSoup)

Both sources feed a common async pipeline with filtering, scoring, and dedupe.

---

## 🚀 Demo Features

Try these Telegram commands:

- `/start` → Subscribe to alerts  
- `/stats` → View daily stats  
- `/filter` → (Coming soon)  
- `/unsubscribe` → Stop alerts  

Sample Alert Format:
```
🔥 New Domain Sale!
🌐 Domain: matrix.core
💰 Price: 9.02 USDC
🔗 View on Doma: https://start.doma.xyz/market/matrix.core
```

---

## 📝 How to Run Locally

### 1. Clone the repo
```bash
git clone https://github.com/your-username/doma-pulsebot.git
cd doma-pulsebot
```

### 2. Install dependencies
```bash
pip install -r requirements.txt
```

### 3. Set your token in .env
```
TELEGRAM_TOKEN=your_bot_token
DEFAULT_CHAT_ID=your_chat_id_here

# Optional but recommended (real API source)
GODADDY_API_KEY=your_api_key
GODADDY_API_SECRET=your_api_secret
# true for GoDaddy OTE sandbox, false for production API
GODADDY_USE_OTE=false

# Optional scraping source URL (you must comply with source ToS/robots)
EXPIRED_DOMAINS_URL=

# Watcher filters/tuning
ALLOWED_TLDS=.app,.dev,.com
DOMAIN_KEYWORDS=ai,app,bot,cloud,code,data,dev,labs,ml,saas,tech,web
MAX_SLD_LENGTH=5
STANDARD_REG_MAX_USD=15
DISCOUNT_TRIGGER_PERCENT=50
WATCHER_POLL_SECONDS=30
ALERT_DB_PATH=alerts.db
```

### 4. Run the bot
```bash
python bot.py
```

---

## 📁 Project Structure

```
doma-pulsebot/
├── bot.py               # Main bot logic
├── doma_events.py       # Real async watcher + sources + scoring + dedupe
├── doma_utils.py        # Formatting helpers
├── filters.json         # Filter config (WIP)
├── requirements.txt     # Python deps
├── .env.example         # Env template
└── README.md            # You're reading it!
```

---

## 🛰 Deployment Notes

While currently running locally, this bot is cloud-ready:

- 🧑‍💻 Deploy to: AWS EC2, Railway, Render, Fly.io, or any VPS
- 🤖 Run in background via `screen`, `systemd`, `pm2`
- ✅ Accepts `.env` for token/Chat ID
- 🔁 Works 24/7 with polling

For production, plug into Doma's real event API or contract logs.

---

## 🎥 Demo Video

🔗 Watch Demo Video : https://youtu.be/TqzAFw2qQ9Y  
(Demo shows: `/start`, receiving alert, `/stats`, `/filter` UI)

---

## 🏆 Hackathon Submission

- Track: 3 – Bots & Event Subscriptions
- Team: Solo developer
- Telegram Bot: [@DomaPulseBot](https://t.me/DomaPulseBot)
- GitHub: [github.com/KISHU-PT/doma-pulsebot](https://github.com/KISHU-PT/doma-pulsebot)
- Twitter: [@DocChain25](https://twitter.com/DocChain25)

---

## 📌 Future Roadmap

- 🔗 Connect to Doma on-chain events  
- 🎯 Add TLD + price filters per user  
- ⚡ Enable sniping or real-time bidding logic  
- 🖥️ Add web dashboard to manage alerts  

---

## ⚡ Inspiration

Doma is turning domains into real digital assets. This bot ensures users never miss a chance to grab the next rare or valuable domain — before someone else does.

---

## 📬 Contact

- Twitter: [@DocChain25](https://twitter.com/DocChain25)  
- Telegram: [@DomaPulseBot](https://t.me/DomaPulseBot)
