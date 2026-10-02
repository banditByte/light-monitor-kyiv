import os
import json
import requests
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from typing import Optional

# === Configuration ===
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHANNEL_ID = os.environ.get("TELEGRAM_CHANNEL_ID")
CONFIG_FILE = "config.json"
CACHE_FILE = "last_schedules.json"
MESSAGES_FILE = "message_ids.json"

KYIV_TZ = ZoneInfo("Europe/Kyiv")

GITHUB_URL = "https://raw.githubusercontent.com/Baskerville42/outage-data-ua/main/data/{region}.json"
YASNO_URL = "https://app.yasno.ua/api/blackout-service/public/shutdowns/regions/{region_id}/dsos/{dso_id}/planned-outages"

DAYS_UA = {
    0: "Понеділок",
    1: "Вівторок",
    2: "Середа",
    3: "Четвер",
    4: "П'ятниця",
    5: "Субота",
    6: "Неділя"
}


def load_config() -> dict:
    """Load config with validation"""
    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            content = f.read()
            try:
                return json.loads(content)
            except json.JSONDecodeError as e:
                print(f"JSON Error in {CONFIG_FILE}:")
                print(f"  Line {e.lineno}, Column {e.colno}: {e.msg}")
                lines = content.split('\n')
                if e.lineno <= len(lines):
                    print(f"  → {lines[e.lineno - 1]}")
                raise SystemExit(1)
    except FileNotFoundError:
        print(f"Config file not found: {CONFIG_FILE}")
        raise SystemExit(1)


def get_kyiv_now() -> datetime:
    return datetime.now(KYIV_TZ)


def format_hours_full(hours: float) -> str:
    """Format hours with full Ukrainian declension"""
    if hours == int(hours):
        hours = int(hours)
    
    if isinstance(hours, float):
        return f"{hours} години"
    if hours % 10 == 1 and hours % 100 != 11:
        return f"{hours} година"
    if hours % 10 in [2, 3, 4] and hours % 100 not in [12, 13, 14]:
        return f"{hours} години"
    return f"{hours} годин"


def format_hours_short(hours: float, cfg: dict) -> str:
    """Format hours short (for table), plain text"""
    suffix = cfg['ui']['text'].get('hours_short', 'год.')
    if hours == int(hours):
        return f"{int(hours)} {suffix}"
    return f"{hours} {suffix}"


def format_hours_short_bold(hours: float, cfg: dict) -> str:
    """Format hours short with bold number (for detail intervals)"""
    suffix = cfg['ui']['text'].get('hours_short', 'год.')
    if hours == int(hours):
        return f"<b>{int(hours)}</b> {suffix}"
    return f"<b>{hours}</b> {suffix}"


def format_slot_time(slot: int) -> str:
    mins = slot * 30
    h, m = mins // 60, mins % 60
    return "24:00" if h == 24 else f"{h:02d}:{m:02d}"


def get_spacing(cfg: dict, space_type: str, default: int = 1) -> str:
    """Get spacing string based on config"""
    spacing = cfg['ui'].get('spacing', {})
    count = spacing.get(space_type, default)
    return "\n" * count


def get_detail_indent(cfg: dict) -> str:
    """Get indent for detail intervals"""
    return cfg['ui']['format'].get('detail_indent', '  ')


# === Data Fetching ===

def fetch_github(cfg: dict) -> Optional[dict]:
    if not cfg['sources']['github'].get('enabled', False):
        print("GitHub source disabled")
        return None
    try:
        url = GITHUB_URL.format(region=cfg['settings']['region'])
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"GitHub error: {e}")
        return None


def fetch_yasno(cfg: dict) -> Optional[dict]:
    yasno_cfg = cfg['sources'].get('yasno', {})
    if not yasno_cfg.get('enabled', False):
        print("Yasno source disabled")
        return None
    try:
        url = YASNO_URL.format(
            region_id=yasno_cfg.get('region_id', '25'),
            dso_id=yasno_cfg.get('dso_id', '902')
        )
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"Yasno error: {e}")
        return None


# === Parsing ===

def parse_github_day(day_data: dict) -> list[bool]:
    slots = []
    for h in range(1, 25):
        s = day_data.get(str(h), "yes")
        if s == "yes":
            slots.extend([True, True])
        elif s == "no":
            slots.extend([False, False])
        elif s == "first":
            slots.extend([False, True])
        elif s == "second":
            slots.extend([True, False])
        else:
            slots.extend([True, True])
    return slots


def extract_github(data: dict, cfg: dict) -> dict:
    groups = cfg['settings']['groups']
    res = {grp: {} for grp in groups}
    if not data:
        return res

    fact = data.get("fact")
    fact_data = fact.get("data") if isinstance(fact, dict) else None

    # Upstream returns [] instead of {} when no actual schedule is published
    if not isinstance(fact_data, dict) or not fact_data:
        if fact_data and not isinstance(fact_data, list):
            print(f"GitHub: unexpected fact.data type "
                  f"{type(fact_data).__name__}: {str(fact_data)[:300]}")
            return res
        today_ts = fact.get("today") if isinstance(fact, dict) else None
        if not today_ts:
            print("GitHub: no fact schedule and no 'today' timestamp")
            return res
        print("GitHub: fact schedule not published yet -> pending")
        today = datetime.fromtimestamp(int(today_ts), tz=KYIV_TZ)
        for grp in groups:
            for dt in (today, today + timedelta(days=1)):
                res[grp][dt.strftime("%Y-%m-%d")] = {
                    "slots": None, "date": dt, "status": "pending"
                }
        return res

    timestamps = sorted((ts for ts in fact_data if str(ts).isdigit()), key=int)[:2]
    for grp in groups:
        for ts in timestamps:
            d = (fact_data.get(ts) or {}).get(grp)
            if not isinstance(d, dict):
                continue

            dt = datetime.fromtimestamp(int(ts), tz=KYIV_TZ)
            d_str = dt.strftime("%Y-%m-%d")

            if all(d.get(str(h), "yes") == "yes" for h in range(1, 25)):
                res[grp][d_str] = {"slots": None, "date": dt, "status": "pending"}
            else:
                res[grp][d_str] = {"slots": parse_github_day(d), "date": dt, "status": "normal"}
    return res


def extract_yasno(data: dict, cfg: dict) -> dict:
    res = {}
    if not data:
        return res
    
    for grp in cfg['settings']['groups']:
        key = grp.replace("GPV", "")
        if key not in data:
            continue
        
        res[grp] = {}
        for day in ["today", "tomorrow"]:
            d = data[key].get(day)
            if not d or "date" not in d:
                continue
            
            dt = datetime.fromisoformat(d["date"])
            d_str = dt.strftime("%Y-%m-%d")
            status = d.get("status", "")
            
            if status == "EmergencyShutdowns":
                res[grp][d_str] = {"slots": None, "date": dt, "status": "emergency"}
                continue
            
            if not d.get("slots"):
                res[grp][d_str] = {"slots": None, "date": dt, "status": "pending"}
                continue
            
            slots = [True] * 48
            for s in d["slots"]:
                start, end = s.get("start", 0) // 30, s.get("end", 0) // 30
                is_on = (s.get("type") == "NotPlanned")
                for i in range(start, min(end, 48)):
                    slots[i] = is_on
            
            res[grp][d_str] = {"slots": slots, "date": dt, "status": "normal"}
    return res


# === Processing ===

def slots_to_periods(slots: list[bool]) -> list[dict]:
    if not slots:
        return []
    periods = []
    curr, start = slots[0], 0
    for i in range(1, len(slots)):
        if slots[i] != curr:
            periods.append({
                "start": format_slot_time(start),
                "end": format_slot_time(i),
                "is_on": curr,
                "hours": (i - start) * 0.5
            })
            curr, start = slots[i], i
    periods.append({
        "start": format_slot_time(start),
        "end": format_slot_time(len(slots)),
        "is_on": curr,
        "hours": (len(slots) - start) * 0.5
    })
    return periods


def get_cache() -> dict:
    try:
        with open(CACHE_FILE, "r") as f:
            return json.load(f)
    except:
        return {"github": {}, "yasno": {}}


def save_cache(cache: dict):
    with open(CACHE_FILE, "w") as f:
        json.dump(cache, f, indent=2)


# === Formatting ===

def render_intervals_detail(periods: list[dict], is_on: bool, cfg: dict) -> str:
    """Render detailed intervals with monospace time and bold hours"""
    icons = cfg['ui']['icons']
    txt = cfg['ui']['text']
    indent = get_detail_indent(cfg)
    
    filtered = [p for p in periods if p['is_on'] == is_on]
    
    if not filtered:
        return ""
    
    total = sum(p['hours'] for p in filtered)
    
    if is_on:
        icon = icons.get('light_on', '☀')
        label = txt.get('on_detail', 'Світло буде')
    else:
        icon = icons.get('light_off', '⃠')
        label = txt.get('off_detail', 'Світла не буде')
    
    lines = [f"{icon} {label} {format_hours_full(total)}:"]
    
    for p in filtered:
        time_range = f"{p['start']}-{p['end']}"
        dur = format_hours_short_bold(p['hours'], cfg)
        # <code> for monospace time, <b> for bold hours in dur
        lines.append(f"{indent}<code>{time_range}</code>  |  {dur}")
    
    return "\n".join(lines)


def render_summary_simple(periods: list[dict], cfg: dict) -> str:
    """Render simple summary"""
    icons = cfg['ui']['icons']
    txt = cfg['ui']['text']
    
    total_on = sum(p['hours'] for p in periods if p['is_on'])
    total_off = sum(p['hours'] for p in periods if not p['is_on'])
    
    icon_on = icons.get('on_list', icons['on'])
    icon_off = icons.get('off_list', icons['off'])
    
    lines = [
        f"{icon_on} {txt.get('on_full', 'Світло є')}: {format_hours_full(total_on)}",
        f"{icon_off} {txt.get('off_full', 'Світла нема')}: {format_hours_full(total_off)}"
    ]
    return "\n".join(lines)


def render_summary(periods: list[dict], cfg: dict) -> str:
    """Render summary with spacing"""
    show_detail = cfg['settings'].get('show_intervals_detail', False)
    spacing = get_spacing(cfg, 'before_summary', 1)
    
    if show_detail:
        on_detail = render_intervals_detail(periods, True, cfg)
        off_detail = render_intervals_detail(periods, False, cfg)
        
        parts = []
        if on_detail:
            parts.append(on_detail)
        if off_detail:
            parts.append(off_detail)
        
        content = "\n\n".join(parts)
    else:
        content = render_summary_simple(periods, cfg)
    
    return f"{spacing}{content}"


def render_table(periods: list[dict], cfg: dict) -> str:
    """Render table wrapped in <pre>"""
    icons = cfg['ui']['icons']
    fmt = cfg['ui']['format']
    
    COL1, COL2, COL3 = 12, 12, 10
    total_width = COL1 + COL2 + COL3 + 2
    
    sep_char = fmt.get('table_separator', '-')
    sep_line = sep_char * total_width
    
    header = f"    {icons['off']}     |    {icons['on']}     |   {icons['clock']}"
    
    lines = [sep_line, header, sep_line]
    
    for p in periods:
        time_range = f"{p['start']}-{p['end']}"
        dur = format_hours_short(p['hours'], cfg)
        
        if p['is_on']:
            row = f"{'':{COL1}}|{time_range:^{COL2}}|{dur:^{COL3}}"
        else:
            row = f"{time_range:^{COL1}}|{'':{COL2}}|{dur:^{COL3}}"
        
        lines.append(row)
    
    lines.append(sep_line)
    
    # Wrap entire table in <pre>
    table_text = "\n".join(lines)
    
    summary = render_summary(periods, cfg)
    
    return f"<pre>{table_text}</pre>{summary}"


def render_list(periods: list[dict], cfg: dict) -> str:
    """Render list format"""
    icons = cfg['ui']['icons']
    
    icon_on = icons.get('on_list', icons['on'])
    icon_off = icons.get('off_list', icons['off'])
    
    lines = []
    
    for p in periods:
        ico = icon_on if p['is_on'] else icon_off
        lines.append(f"{ico} {p['start']} - {p['end']} … ({format_hours_full(p['hours'])})")
    
    content = "\n".join(lines)
    summary = render_summary(periods, cfg)
    
    return f"{content}{summary}"


def format_day(data: dict, date: datetime, src: str, cfg: dict) -> str:
    """Format single day message"""
    ui = cfg['ui']
    icons = ui['icons']
    txt = ui['text']
    
    d_str = date.strftime("%d.%m")
    day_name = DAYS_UA[date.weekday()]
    src_name = cfg['sources'].get(src, {}).get('name', src)
    
    lines = [f"{icons['calendar']}  {d_str} ({day_name}) [{src_name}]:", ""]
    
    st = data.get("status")
    if st == "emergency":
        lines.append(f"{icons['emergency']} {txt['emergency']}")
    elif st == "pending":
        lines.append(f"{icons['pending']} {txt['pending']}")
    elif data.get("slots"):
        periods = slots_to_periods(data["slots"])
        if cfg['settings']['style'] == "table":
            lines.append(render_table(periods, cfg))
        else:
            lines.append(render_list(periods, cfg))
    
    return "\n".join(lines)


def format_footer(cfg: dict) -> str:
    """Format footer with separator and update time"""
    icons = cfg['ui']['icons']
    txt = cfg['ui']['text']
    fmt = cfg['ui']['format']
    
    footer_sep = fmt.get('separator_footer', '────────────────────')
    space_before = get_spacing(cfg, 'before_footer', 2)
    space_after_sep = get_spacing(cfg, 'after_footer_separator', 1)
    
    sep = icons.get('separator', '⠅')
    now = get_kyiv_now()
    time_str = now.strftime(f"%d.%m.%Y {sep}%H:%M")
    
    return f"{space_before}{footer_sep}{space_after_sep}{icons['clock']} {txt['updated']}: {time_str} (Київ)"


def format_msg(gh: dict, ya: dict, cfg: dict) -> Optional[str]:
    """Format complete message"""
    groups = cfg['settings']['groups']
    fmt = cfg['ui']['format']
    
    sep_source = fmt['separator_source']
    sep_day = fmt['separator_day']
    
    space_source = get_spacing(cfg, 'before_separator_source', 1)
    space_day = get_spacing(cfg, 'before_separator_day', 2)
    
    blocks = []
    
    for grp in groups:
        grp_num = grp.replace("GPV", "")
        header = fmt['header_template'].format(group=grp_num)
        
        dates = set()
        if grp in gh:
            dates.update(gh[grp].keys())
        if grp in ya:
            dates.update(ya[grp].keys())
        
        if not dates:
            continue
        
        day_msgs = []
        for d_str in sorted(dates)[:2]:
            g_d = gh.get(grp, {}).get(d_str)
            y_d = ya.get(grp, {}).get(d_str)
            
            if not g_d and not y_d:
                continue
            
            dt = (g_d or y_d)["date"]
            src_msgs = []
            
            # Merge when both sources agree: same status, and same slots for 'normal'
            match = bool(g_d and y_d) and g_d['status'] == y_d['status'] and (
                g_d['status'] != 'normal' or g_d['slots'] == y_d['slots']
            )
            
            if match:
                gh_name = cfg['sources']['github']['name']
                ya_name = cfg['sources']['yasno']['name']
                base = format_day(g_d, dt, "github", cfg)
                base = base.replace(f"[{gh_name}]", f"[{gh_name}, {ya_name}]")
                src_msgs.append(base)
            else:
                if g_d:
                    src_msgs.append(format_day(g_d, dt, "github", cfg))
                if y_d:
                    src_msgs.append(format_day(y_d, dt, "yasno", cfg))
            
            if src_msgs:
                source_separator = f"{space_source}{sep_source}\n"
                day_msgs.append(source_separator.join(src_msgs))
        
        if day_msgs:
            day_separator = f"{space_day}{sep_day}\n"
            body = day_separator.join(day_msgs)
            blocks.append(f"{header}\n{body}")
    
    if not blocks:
        return None
    
    footer = format_footer(cfg)
    
    return "\n\n\n".join(blocks) + footer


# === Telegram ===

def send_tg(text: str) -> Optional[int]:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHANNEL_ID:
        print("Telegram credentials not configured")
        return None
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHANNEL_ID, "text": text, "parse_mode": "HTML"},
            timeout=30
        )
        r.raise_for_status()
        return r.json()["result"]["message_id"]
    except Exception as e:
        print(f"Send failed: {e}")
        return None


def manage_msgs(mid: int, cfg: dict):
    max_msgs = cfg['settings'].get('max_messages', 3)
    
    try:
        with open(MESSAGES_FILE, "r") as f:
            ids = json.load(f)
    except:
        ids = []
    
    requests.post(
        f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/pinChatMessage",
        json={"chat_id": TELEGRAM_CHANNEL_ID, "message_id": mid, "disable_notification": True}
    )
    
    ids.append(mid)
    
    while len(ids) > max_msgs:
        old = ids.pop(0)
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/deleteMessage",
            json={"chat_id": TELEGRAM_CHANNEL_ID, "message_id": old}
        )
    
    with open(MESSAGES_FILE, "w") as f:
        json.dump(ids, f)


# === Main ===

def main():
    cfg = load_config()
    
    print(f"Region: {cfg['settings']['region']}")
    print(f"Groups: {cfg['settings']['groups']}")
    print(f"Style: {cfg['settings']['style']}")
    print(f"Detail intervals: {cfg['settings'].get('show_intervals_detail', False)}")
    print(f"GitHub: {cfg['sources']['github'].get('enabled', False)}")
    print(f"Yasno: {cfg['sources']['yasno'].get('enabled', False)}")
    
    print("\nFetching data...")
    gh_data = fetch_github(cfg)
    ya_data = fetch_yasno(cfg)
    
    print(f"GitHub: {'OK' if gh_data else 'SKIP/FAIL'}")
    print(f"Yasno: {'OK' if ya_data else 'SKIP/FAIL'}")
    
    if not gh_data and not ya_data:
        print("No data from any source")
        return
    
    gh_sched = extract_github(gh_data, cfg)
    ya_sched = extract_yasno(ya_data, cfg)
    
    def serialize(s):
        r = {}
        for g, d in s.items():
            r[g] = {k: {"status": v["status"], "slots": v["slots"]} for k, v in d.items()}
        return r
    
    new_c = {"github": serialize(gh_sched), "yasno": serialize(ya_sched)}
    old_c = get_cache()
    
    if new_c == old_c:
        print("No changes.")
        return
    
    print("Updates detected!")
    msg = format_msg(gh_sched, ya_sched, cfg)
    
    if msg:
        print("\n" + "=" * 50)
        print(msg)
        print("=" * 50 + "\n")
        
        mid = send_tg(msg)
        if mid:
            manage_msgs(mid, cfg)
            save_cache(new_c)
            print("Done.")
        else:
            print("Failed to send message")
    else:
        print("No message generated")


if __name__ == "__main__":
    main()
