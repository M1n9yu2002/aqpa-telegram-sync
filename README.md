# AQPA Telegram Sync

AQPA 是一個以 Google Sheets 為資料來源、SQLite 為本地快取、Telegram 為操作入口的台股追蹤小工具。

目前是 Phase 1，重點放在：

- 從 Google Sheets 讀取持股與觀察清單
- 驗證與正規化資料
- 同步進本地 SQLite
- 透過 Telegram 指令觸發同步與查詢持股

---

## Current Features

目前已完成：

- Google Sheets 作為唯一資料來源
- SQLite 作為 read-replica cache
- `Positions` 工作表同步
- `Watchlist` 工作表同步
- 失敗不覆蓋既有 SQLite 資料
- `sync_runs` 紀錄每次同步結果
- `event_log` 紀錄同步錯誤
- Telegram `/sync` 指令
- Telegram `/positions` 指令
- 限制只有允許的 Telegram chat ID 可使用 bot

---

## Project Structure

```text
aqpa/
├── adapters/
│   ├── sheets_client.py
│   └── telegram_client.py
├── app/
│   └── main.py
├── bots/
│   └── handlers.py
├── configs/
│   ├── logging.yaml
│   └── settings.py
├── data/
│   └── .gitkeep
├── storage/
│   ├── repositories.py
│   └── schema.sql
├── sync/
│   ├── normalizer.py
│   ├── sync_manager.py
│   └── validators.py
├── .env.example
├── .gitignore
└── README.md