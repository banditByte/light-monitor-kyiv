# light-monitor-kyiv

Для роботи бота використовюється [Публічне сховище даних про планові відключення електроенергії в Україні.](https://github.com/Baskerville42/outage-data-ua/tree/main) та YASNO (API)

Бот запускається щогодини (cron), збирає інформацію та надсилає у Телеграм канал графікі для одної або декількох черг одразу за наявності оновлень.

Старі повідомлення видаляються, залишаються лише 3 останніх.


Приклад того, як виглядають повідомлення про оновлення графіків у Телеграм:

============ ◉ 12.1 ◉ ============

📆  02.02 (Понеділок) [outage-data-ua, yasno]:

🟩 00:00 - 04:00 … (4 години)  
🟠 04:00 - 11:00 … (7 годин)  
🟩 11:00 - 13:00 … (2 години)  
🟠 13:00 - 20:30 … (7.5 години)  
🟩 20:30 - 22:00 … (1.5 години)  
🟠 22:00 - 24:00 … (2 години)

🟩 Світло є: 7.5 години  
🟠 Світла нема: 16.5 години

■  ■  ■  ■  ■  ■  ■  ■  ■  ■  ■  ■  ■  ■  ■  ■  ■

📆  03.02 (Вівторок) [outage-data-ua, yasno]:

🟠 00:00 - 05:00 … (5 годин)  
🟩 05:00 - 10:00 … (5 годин)  
🟠 10:00 - 17:00 … (7 годин)  
🟩 17:00 - 19:00 … (2 години)  
🟠 19:00 - 24:00 … (5 годин)

🟩 Світло є: 7 годин  
🟠 Світла нема: 17 годин

============ ◉ 18.1 ◉ ============

📆  02.02 (Понеділок) [outage-data-ua, yasno]:

🟠 00:00 - 01:30 … (1.5 години)  
🟩 01:30 - 07:00 … (5.5 години)  
🟠 07:00 - 13:00 … (6 годин)  
🟩 13:00 - 16:30 … (3.5 години)  
🟠 16:30 - 23:30 … (7 годин)  
🟩 23:30 - 24:00 … (0.5 години)

🟩 Світло є: 9.5 години  
🟠 Світла нема: 14.5 години

■  ■  ■  ■  ■  ■  ■  ■  ■  ■  ■  ■  ■  ■  ■  ■  ■

📆  03.02 (Вівторок) [outage-data-ua, yasno]:

🟩 00:00 - 05:30 … (5.5 години)  
🟠 05:30 - 10:30 … (5 годин)  
🟩 10:30 - 14:00 … (3.5 години)  
🟠 14:00 - 21:00 … (7 годин)  
🟩 21:00 - 24:00 … (3 години)

🟩 Світло є: 12 годин  
🟠 Світла нема: 12 годин

🕐 Оновлено: 02.02.2026 ⠅18:51 (Київ)


##### Що вам потрібно, аби змусити код працювати на себе:

1. **Клонувати цей GitHub-репозиторій**  
   (можете створити новий public repo в акаунті).

2. **Додати два секрети** в Settings → Secrets для цього репозиторію:  
   - `TELEGRAM_BOT_TOKEN` — токен бота (наприклад, `12345:ABC...`)
   - `TELEGRAM_CHANNEL_ID` — chat_id вашого каналу (наприклад, `@mychannel` або числовий Id `-100...`)

3. **Пересвідчитись**, що репозиторій має необхідні дозволи  
   ("Allow all actions and reusable workflows").

##### Налаштування регіону
У файлі "config.json" можна налаштувати регіон:
```
  "groups": ["GPV12.1", "GPV18.1"],
  "region": "kyiv",   //   <== 
  
  "sources": {
    "github": {
      "enabled": true,
      "name": "GitHub-ДТЕК"
    },
    "yasno": {
      "enabled": false
    }
  },
```

"region": "kyiv" // місто Київ

"region": "kyiv-region" // Київська область

"region": "dnipro" // місто Дніпро

"region": "odesa" // місто Одеса

Про всяк випадок вимкніть у sources "yasno" ("enabled": false), залишивши лише "github".

Дивіться актуальні назви для регіонів у "https://github.com/Baskerville42/outage-data-ua/tree/main/data"

Налаштування для регіонів я не тестив, тож ...

##### Налаштування інтервалу запуску
Інтервал запуску налаштовується у файлі .github/workflows/check_outages.yml
```
on:
  schedule:
    # ┌───────────── хвилини (0 - 59)
    # │ ┌───────────── години (0 - 23)
    # │ │ ┌───────────── дні місяця (1 - 31)
    # │ │ │ ┌───────────── місяці (1 - 12)
    # │ │ │ │ ┌───────────── дні тижня (0 - 6)
    # * * * * *
    - cron: '0 */1 * * *'  # <-- МІНЯТИ ТУТ (зараз: кожну 1 годину)
```
###### Зважайте на обмеження безкоштовного акаунту github!

<details>

<summary>Step-by-step guide [English]</summary>

#### What the script creates / updates

*   last\_schedules.json — cache of last schedules (used to detect changes).
*   message\_ids.json — stores Telegram message ids to delete older messages (keeps up to MAX\_MESSAGES).
*   The GitHub Action tries to commit these files back to the repo so the cache persists between runs.


#### Step-by-step setup (GitHub Actions)

Prerequisites

*   GitHub account.
*   A Telegram bot token and a channel (see step 4).
*   Optional: fork the repository or create a new repo (you can keep it public).

*   Fork or clone the repository
    
    *   Fork on GitHub or clone locally:
        *   git clone [https://github.com/banditByte/light-monitor-kyiv.git](https://github.com/banditByte/light-monitor-kyiv.git)
*   Create (or verify) configuration
    
    *   Edit config.json if you want different groups, region or Yasno IDs. Example keys:
        *   groups — list of group names (GPVxx.x) monitored
        *   region — region slug used for the outage-data-ua source (default "kyiv")
        *   yasno\_region\_id and yasno\_dso\_id — used for the Yasno API
    *   The repo includes a default config.json (shown above). Adjust groups / ids as needed.
*   Create a Telegram bot and channel details
    
    *   Create a bot with BotFather to get TELEGRAM\_BOT\_TOKEN (format like 12345:ABC...).
    *   Create a Telegram channel (or use existing). Add the bot to the channel and give it permission to post (bot must be admin or allowed to post).
    *   To get TELEGRAM\_CHANNEL\_ID:
        *   If you have a public channel username, you can use @yourchannelname.
        *   To get numeric id: send a message to the channel, then call: [https://api.telegram.org/bot](https://api.telegram.org/bot)<TELEGRAM\_BOT\_TOKEN>/getUpdates Look for "chat":{"id": -100XXXXXXXXXX} in the returned json (that -100... value is channel id).
        *   You can also use helper bots like @get\_id\_bot or similar.
*   Add repository secrets (Actions) on GitHub
    
    *   Go to your repo → Settings → Secrets and variables → Actions → New repository secret
        *   Add TELEGRAM\_BOT\_TOKEN with the token value.
        *   Add TELEGRAM\_CHANNEL\_ID with the numeric id or @channelusername.
    *   Ensure Actions permissions allow the workflow to run and push cache files:
        *   The workflow sets permission contents: write — make sure repository settings do not block write actions.
        *   If the README suggests "Allow all actions and reusable workflows", enable appropriate settings in Settings → Actions.
*   Confirm the workflow schedule and behavior
    
    *   The included workflow runs hourly by cron: '0 \*/1 \* \* \*' and also supports manual runs (workflow\_dispatch).
    *   The workflow uses Python 3.11 and installs requirements via pip install -r requirements.txt.
    *   On successful run it executes python main.py and the script will:
        *   Fetch outage-data-ua and Yasno API
        *   Format messages and send to Telegram
        *   Keep last\_schedules.json and message\_ids.json (cache files)
        *   Attempt to commit updated cache files back to the repo (git push || true — push is best-effort and may fail if permissions or token are restricted).
*   Test the workflow
    
    *   In the repo on GitHub, open Actions → the workflow → Run workflow (manual dispatch) to test.
    *   Check Action logs for errors (installation, missing env, network).

</details>

---

Аби дізнатися про код детально, дивіться файл налаштувань (config.json) на читійте коментарі.

##### Структура та призначення файлів у репозиторії

```bash
light-monitor-kyiv/
├── .github/
│   └── workflows/
│       └── check_outages.yml   # файл конфігурації GitHub Actions
├── config.json                 # конфігурація груп
├── main.py                     # основний код
├── requirements.txt            # залежності
├── last_schedules.json         # кеш розкладів (створюється автоматично)
├── message_ids.json            # ID повідомлень для видалення (створюється автоматично)
└── README.md                   # цей файл
```


---

##### Корисне
[Отримати номер групи для Києва із портала YASNO](https://sergeymaximenko.github.io/SetTimeGenerator/GetGroupForKiev.html))

[СвітлоБот](https://svitlobot.in.ua/)

[Svitlobot Map](https://svitlobot.pp.ua/map.php)

[Map - Svitlobot](https://svitlobot.obbdev.com/Map)

---

Дякую спільноті https://t.me/svitlobot_api за те, що нагадала: *хочеш, аби все добре працювало і було, як потрібно саме тобі, зроби все сам* ¯\\_(ツ)_/¯
