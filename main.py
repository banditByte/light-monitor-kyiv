import os
import json
import requests
from datetime import datetime, timezone, timedelta
from typing import Optional

# === Configuration ===
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHANNEL_ID = os.environ.get("TELEGRAM_CHANNEL_ID")
CONFIG_FILE = "config.json"
CACHE_FILE = "last_schedules.json"
MESSAGES_FILE = "message_ids.json"

# Kyiv timezone UTC+2
KYIV_TZ = timezone(timedelta(hours=2))

# URLs
GITHUB_DATA_URL = "https://raw.githubusercontent.com/Baskerville42/outage-data-ua/main/data/{region}.json"
YASNO_API_URL = "https://app.yasno.ua/api/blackout-service/public/shutdowns/regions/{region_id}/dsos/{dso_id}/planned-outages"

# Days of week (Ukrainian)
DAYS_UA = {
    0: "Понеділок",
    1: "Вівторок",
    2: "Середа",
    3: "Четвер",
    4: "П'ятниця",
    5: "Субота",
    6: "Неділя"
}

# Separators
SEP_SOURCE = "✧ ✧ ✧ ✧ ✧ ✧ ✧ ✧ ✧ ✧ ✧ ✧ ✧ ✧ ✧ ✧ ✧"
SEP_DAY = "■  ■  ■  ■  ■  ■  ■  ■  ■  ■  ■  ■  ■  ■  ■  ■  ■"

SOURCE_GITHUB = "GitHub-ДТЕК"
SOURCE_YASNO = "yasno"
MAX_MESSAGES = 3


def load_config() -> dict:
    """Load configuration from file"""
    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {
            "groups": ["GPV12.1", "GPV18.1"],
            "region": "kyiv",
            "yasno_region_id": "25",
            "yasno_dso_id": "902"
        }


def get_kyiv_now() -> datetime:
    """Get current time in Kyiv timezone"""
    return datetime.now(KYIV_TZ)


def format_hours(hours: float) -> str:
    """Format hours with proper Ukrainian declension"""
    if hours == int(hours):
        hours = int(hours)
    
    if isinstance(hours, float):
        return f"<b>{hours}</b> години"
    
    if hours % 10 == 1 and hours % 100 != 11:
        return f"<b>{hours}</b> година"
    elif hours % 10 in [2, 3, 4] and hours % 100 not in [12, 13, 14]:
        return f"<b>{hours}</b> години"
    else:
        return f"<b>{hours}</b> годин"


def format_time(minutes: int) -> str:
    """Convert minutes to HH:MM string"""
    hours = minutes // 60
    mins = minutes % 60
    if hours == 24:
        return "24:00"
    return f"{hours:02d}:{mins:02d}"


def format_slot_time(slot: int) -> str:
    """Convert slot index (0-48) to time string"""
    return format_time(slot * 30)


# === Data fetching ===

def fetch_github_data(region: str) -> Optional[dict]:
    """Fetch data from GitHub repository"""
    try:
        url = GITHUB_DATA_URL.format(region=region)
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"GitHub fetch error: {e}")
        return None


def fetch_yasno_data(region_id: str, dso_id: str) -> Optional[dict]:
    """Fetch data from Yasno API"""
    try:
        url = YASNO_API_URL.format(region_id=region_id, dso_id=dso_id)
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json"
        }
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Yasno API fetch error: {e}")
        return None


# === GitHub data parsing ===

def is_all_yes(day_data: dict) -> bool:
    """Check if all hours have 'yes' status (schedule pending)"""
    for hour in range(1, 25):
        if day_data.get(str(hour), "yes") != "yes":
            return False
    return True


def parse_github_day(day_data: dict) -> list[bool]:
    """Parse GitHub day data into 48 half-hour slots (True = power on)"""
    slots = []
    
    for hour in range(1, 25):
        status = day_data.get(str(hour), "yes")
        
        if status == "yes":
            first_half, second_half = True, True
        elif status == "no":
            first_half, second_half = False, False
        elif status == "first":
            first_half, second_half = False, True
        elif status == "second":
            first_half, second_half = True, False
        else:  # maybe, mfirst, msecond
            first_half, second_half = True, True
        
        slots.extend([first_half, second_half])
    
    return slots


def extract_github_schedules(data: dict, groups: list[str]) -> dict:
    """Extract schedules from GitHub data"""
    result = {}
    fact_data = data.get("fact", {}).get("data", {})
    
    if not fact_data:
        return result
    
    sorted_days = sorted(fact_data.keys(), key=lambda x: int(x))
    
    for group in groups:
        result[group] = {}
        
        for day_ts in sorted_days[:2]:
            day_data = fact_data.get(day_ts, {}).get(group)
            if not day_data:
                continue
            
            date = datetime.fromtimestamp(int(day_ts), tz=KYIV_TZ)
            date_str = date.strftime("%Y-%m-%d")
            
            if is_all_yes(day_data):
                result[group][date_str] = {
                    "slots": None,
                    "date": date,
                    "status": "pending"
                }
            else:
                slots = parse_github_day(day_data)
                result[group][date_str] = {
                    "slots": slots,
                    "date": date,
                    "status": "normal"
                }
    
    return result


# === Yasno API parsing ===

def parse_yasno_day(day_data: dict) -> tuple[Optional[list[bool]], str]:
    """Parse Yasno day data. Returns (slots, status)"""
    status = day_data.get("status", "")
    
    if status == "EmergencyShutdowns":
        return None, "emergency"
    
    if not day_data.get("slots"):
        return None, "pending"
    
    slots = [True] * 48
    
    for slot in day_data["slots"]:
        start_idx = slot.get("start", 0) // 30
        end_idx = slot.get("end", 0) // 30
        is_on = (slot.get("type") == "NotPlanned")
        
        for i in range(start_idx, min(end_idx, 48)):
            slots[i] = is_on
    
    return slots, "normal"


def extract_yasno_schedules(data: dict, groups: list[str]) -> dict:
    """Extract schedules from Yasno API data"""
    result = {}
    
    if not data:
        return result
    
    for group in groups:
        group_key = group.replace("GPV", "")
        
        if group_key not in data:
            continue
        
        group_data = data[group_key]
        result[group] = {}
        
        for day_key in ["today", "tomorrow"]:
            day_data = group_data.get(day_key)
            if not day_data or "date" not in day_data:
                continue
            
            date_str_full = day_data["date"]
            date = datetime.fromisoformat(date_str_full)
            date_str = date.strftime("%Y-%m-%d")
            
            slots, status = parse_yasno_day(day_data)
            result[group][date_str] = {
                "slots": slots,
                "date": date,
                "status": status
            }
    
    return result


# === Schedule processing ===

def slots_to_periods(slots: list[bool]) -> list[dict]:
    """Convert slot array to list of periods"""
    if not slots:
        return []
    
    periods = []
    current_status = slots[0]
    start_slot = 0
    
    for i in range(1, len(slots)):
        if slots[i] != current_status:
            hours = (i - start_slot) * 0.5
            periods.append({
                "start": format_slot_time(start_slot),
                "end": format_slot_time(i),
                "is_on": current_status,
                "hours": hours
            })
            current_status = slots[i]
            start_slot = i
    
    hours = (len(slots) - start_slot) * 0.5
    periods.append({
        "start": format_slot_time(start_slot),
        "end": format_slot_time(len(slots)),
        "is_on": current_status,
        "hours": hours
    })
    
    return periods


def schedules_match(slots1: list[bool], slots2: list[bool]) -> bool:
    """Check if two schedules are identical"""
    if not slots1 or not slots2:
        return False
    return slots1 == slots2


# === Caching (schedule-based) ===

def schedules_to_cache_format(github_schedules: dict, yasno_schedules: dict) -> dict:
    """Convert schedules to serializable cache format"""
    cache = {"github": {}, "yasno": {}}
    
    for group, dates in github_schedules.items():
        cache["github"][group] = {}
        for date_str, data in dates.items():
            cache["github"][group][date_str] = {
                "slots": data["slots"],
                "status": data["status"]
            }
    
    for group, dates in yasno_schedules.items():
        cache["yasno"][group] = {}
        for date_str, data in dates.items():
            cache["yasno"][group][date_str] = {
                "slots": data["slots"],
                "status": data["status"]
            }
    
    return cache


def load_cached_schedules() -> dict:
    """Load cached schedules from file"""
    try:
        with open(CACHE_FILE, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {"github": {}, "yasno": {}}


def save_cached_schedules(cache: dict):
    """Save schedules cache to file"""
    with open(CACHE_FILE, "w") as f:
        json.dump(cache, f, indent=2)


def schedules_changed(new_cache: dict, old_cache: dict) -> bool:
    """Compare new and old schedules to detect changes"""
    return new_cache != old_cache


# === Message formatting ===

def format_schedule_message(
    periods: list[dict],
    date: datetime,
    sources: list[str],
    special_status: Optional[str] = None
) -> str:
    """Format schedule message for one day"""
    day_name = DAYS_UA[date.weekday()]
    date_str = date.strftime("%d.%m")
    sources_str = ", ".join(sources)
    
    lines = [f"📆  {date_str} ({day_name}) [{sources_str}]:"]
    lines.append("")
    
    if special_status == "emergency":
        lines.append("🚨 АВАРІЙНЕ ВІДКЛЮЧЕННЯ!")
        return "\n".join(lines)
    
    if special_status == "pending":
        lines.append("⏳ Очікується інформація про графік")
        return "\n".join(lines)
    
    total_on = 0.0
    total_off = 0.0
    
    for period in periods:
        emoji = "🟩" if period["is_on"] else "🟠"
        time_range = f"<code>{period['start']} - {period['end']}</code>"
        hours_text = format_hours(period["hours"])
        
        lines.append(f"{emoji} {time_range} … ({hours_text})")
        
        if period["is_on"]:
            total_on += period["hours"]
        else:
            total_off += period["hours"]
    
    lines.append("")
    lines.append(f"🟩 Світло є: {format_hours(total_on)}")
    lines.append(f"🟠 Світла нема: {format_hours(total_off)}")
    
    return "\n".join(lines)


def format_single_source_message(data: dict, date: datetime, source: str) -> Optional[str]:
    """Format message for a single source"""
    if not data:
        return None
    
    slots = data.get("slots")
    status = data.get("status")
    
    if status == "normal" and slots:
        periods = slots_to_periods(slots)
        return format_schedule_message(periods, date, [source])
    elif status == "pending":
        return format_schedule_message([], date, [source], "pending")
    elif status == "emergency":
        return format_schedule_message([], date, [source], "emergency")
    
    return None


def format_group_message(
    group: str,
    github_schedules: dict,
    yasno_schedules: dict
) -> Optional[str]:
    """Format message for one group - always show both sources if they differ"""
    group_num = group.replace("GPV", "")
    header = f"============ ◉ <b>{group_num}</b> ◉ ============"
    
    # Collect all dates from both sources
    all_dates = set()
    if group in github_schedules:
        all_dates.update(github_schedules[group].keys())
    if group in yasno_schedules:
        all_dates.update(yasno_schedules[group].keys())
    
    if not all_dates:
        return None
    
    sorted_dates = sorted(all_dates)[:2]
    
    # Build messages grouped by date
    day_blocks = []
    
    for date_str in sorted_dates:
        github_data = github_schedules.get(group, {}).get(date_str)
        yasno_data = yasno_schedules.get(group, {}).get(date_str)
        
        # Determine date from available source
        date = None
        if github_data:
            date = github_data["date"]
        elif yasno_data:
            date = yasno_data["date"]
        
        if not date:
            continue
        
        github_slots = github_data.get("slots") if github_data else None
        yasno_slots = yasno_data.get("slots") if yasno_data else None
        github_status = github_data.get("status") if github_data else None
        yasno_status = yasno_data.get("status") if yasno_data else None
        
        # Check if both sources have normal slots and they match
        both_normal = (
            github_status == "normal" and 
            yasno_status == "normal" and 
            github_slots and 
            yasno_slots
        )
        
        source_messages = []
        
        if both_normal and schedules_match(github_slots, yasno_slots):
            # Data matches exactly - show single combined block
            periods = slots_to_periods(github_slots)
            msg = format_schedule_message(periods, date, [SOURCE_GITHUB, SOURCE_YASNO])
            source_messages.append(msg)
        else:
            # Data differs or special status - show both sources separately
            
            # GitHub block
            if github_data:
                msg = format_single_source_message(github_data, date, SOURCE_GITHUB)
                if msg:
                    source_messages.append(msg)
            
            # Yasno block
            if yasno_data:
                msg = format_single_source_message(yasno_data, date, SOURCE_YASNO)
                if msg:
                    source_messages.append(msg)
        
        if source_messages:
            # Join sources for same day with source separator
            day_block = f"\n\n{SEP_SOURCE}\n".join(source_messages)
            day_blocks.append(day_block)
    
    if not day_blocks:
        return None
    
    # Join different days with day separator
    days_text = f"\n\n{SEP_DAY}\n".join(day_blocks)
    return f"{header}\n{days_text}"


def format_full_message(
    github_schedules: dict,
    yasno_schedules: dict,
    groups: list[str]
) -> Optional[str]:
    """Format complete message for all groups"""
    all_group_messages = []
    
    for group in groups:
        msg = format_group_message(group, github_schedules, yasno_schedules)
        if msg:
            all_group_messages.append(msg)
    
    if not all_group_messages:
        return None
    
    now = get_kyiv_now()
    update_time = now.strftime("%d.%m.%Y ⠅%H:%M")
    footer = f"\n\n🕐 Оновлено: {update_time} (Київ)"
    
    return "\n\n\n".join(all_group_messages) + footer


# === Message ID management ===

def load_message_ids() -> list[int]:
    """Load stored message IDs"""
    try:
        with open(MESSAGES_FILE, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return []


def save_message_ids(ids: list[int]):
    """Save message IDs to file"""
    with open(MESSAGES_FILE, "w") as f:
        json.dump(ids, f)


# === Telegram API ===

def send_telegram_message(message: str) -> Optional[int]:
    """Send message to Telegram, return message ID"""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHANNEL_ID:
        print("Telegram credentials not configured")
        return None
    
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    
    payload = {
        "chat_id": TELEGRAM_CHANNEL_ID,
        "text": message,
        "parse_mode": "HTML"
    }
    
    try:
        response = requests.post(url, json=payload, timeout=30)
        response.raise_for_status()
        result = response.json()
        message_id = result.get("result", {}).get("message_id")
        print(f"Message sent, ID: {message_id}")
        return message_id
    except Exception as e:
        print(f"Send error: {e}")
        return None


def pin_message(message_id: int) -> bool:
    """Pin message in channel"""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHANNEL_ID:
        return False
    
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/pinChatMessage"
    
    payload = {
        "chat_id": TELEGRAM_CHANNEL_ID,
        "message_id": message_id,
        "disable_notification": True
    }
    
    try:
        response = requests.post(url, json=payload, timeout=30)
        response.raise_for_status()
        print(f"Message {message_id} pinned")
        return True
    except Exception as e:
        print(f"Pin error: {e}")
        return False


def delete_message(message_id: int) -> bool:
    """Delete message from channel"""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHANNEL_ID:
        return False
    
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/deleteMessage"
    
    payload = {
        "chat_id": TELEGRAM_CHANNEL_ID,
        "message_id": message_id
    }
    
    try:
        response = requests.post(url, json=payload, timeout=30)
        response.raise_for_status()
        print(f"Message {message_id} deleted")
        return True
    except Exception as e:
        print(f"Delete error: {e}")
        return False


def manage_messages(new_message_id: int):
    """Pin new message, delete old ones if > MAX_MESSAGES"""
    message_ids = load_message_ids()
    
    pin_message(new_message_id)
    message_ids.append(new_message_id)
    
    while len(message_ids) > MAX_MESSAGES:
        old_id = message_ids.pop(0)
        delete_message(old_id)
    
    save_message_ids(message_ids)
    print(f"Active messages: {message_ids}")


# === Main ===

def main():
    config = load_config()
    groups = config.get("groups", ["GPV12.1", "GPV18.1"])
    region = config.get("region", "kyiv")
    yasno_region_id = config.get("yasno_region_id", "25")
    yasno_dso_id = config.get("yasno_dso_id", "902")
    
    print(f"Region: {region}")
    print(f"Groups: {', '.join(groups)}")
    print(f"Kyiv time: {get_kyiv_now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Always fetch from both sources
    print("\nFetching GitHub data...")
    github_data = fetch_github_data(region)
    print(f"GitHub data: {'OK' if github_data else 'FAILED'}")
    
    print("Fetching Yasno API data...")
    yasno_data = fetch_yasno_data(yasno_region_id, yasno_dso_id)
    print(f"Yasno data: {'OK' if yasno_data else 'FAILED'}")
    
    if not github_data and not yasno_data:
        print("Failed to fetch data from both sources")
        return
    
    # Extract schedules
    github_schedules = extract_github_schedules(github_data, groups) if github_data else {}
    yasno_schedules = extract_yasno_schedules(yasno_data, groups) if yasno_data else {}
    
    print(f"\nGitHub schedules: {list(github_schedules.keys())}")
    for group, dates in github_schedules.items():
        for date_str, data in dates.items():
            print(f"  {group} / {date_str}: status={data['status']}")
    
    print(f"Yasno schedules: {list(yasno_schedules.keys())}")
    for group, dates in yasno_schedules.items():
        for date_str, data in dates.items():
            print(f"  {group} / {date_str}: status={data['status']}")
    
    # Convert to cache format and compare
    new_cache = schedules_to_cache_format(github_schedules, yasno_schedules)
    old_cache = load_cached_schedules()
    
    if not schedules_changed(new_cache, old_cache):
        print("\nNo changes detected in schedules")
        return
    
    print("\nSchedule changes detected!")
    
    # Format message
    message = format_full_message(github_schedules, yasno_schedules, groups)
    
    if not message:
        print("Failed to format message - no data available")
        return
    
    print("\nGenerated message:")
    print("-" * 50)
    print(message)
    print("-" * 50)
    
    # Send to Telegram
    message_id = send_telegram_message(message)
    
    if message_id:
        manage_messages(message_id)
        save_cached_schedules(new_cache)
        print("Cache saved")
    else:
        print("Failed to send message, cache not updated")


if __name__ == "__main__":
    main()
