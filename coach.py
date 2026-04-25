import json
import os
import smtplib
import ssl
import sys
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import html

try:
    from zoneinfo import ZoneInfo  # py>=3.9
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore


STRAVA_TOKEN_URL = "https://www.strava.com/oauth/token"
STRAVA_ACTIVITIES_URL = "https://www.strava.com/api/v3/athlete/activities"
STRAVA_STREAMS_URL_TMPL = "https://www.strava.com/api/v3/activities/{activity_id}/streams"
OPENAI_CHAT_COMPLETIONS_URL = "https://api.openai.com/v1/chat/completions"


@dataclass(frozen=True)
class Activity:
    id: int
    name: str
    start_date: datetime
    distance_m: float
    moving_time_s: int
    elapsed_time_s: int
    average_speed_mps: float | None
    average_heartrate: float | None
    max_heartrate: float | None
    type: str


@dataclass(frozen=True)
class Streams:
    time_s: list[int]
    heartrate_bpm: list[int] | None
    velocity_mps: list[float] | None


@dataclass(frozen=True)
class Block:
    kind: str  # "run" | "walk" | "pause"
    start_idx: int
    end_idx_exclusive: int


def _env(name: str, default: str | None = None) -> str | None:
    v = os.environ.get(name)
    if v is None:
        return default
    v = v.strip()
    return v if v else default


def _http_post_form(url: str, form: dict[str, str]) -> dict:
    data = urllib.parse.urlencode(form).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _http_get_json(url: str, headers: dict[str, str], params: dict[str, str] | None = None) -> object:
    if params:
        url = f"{url}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers=headers, method="GET")
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def get_strava_access_token() -> str:
    client_id = _env("STRAVA_CLIENT_ID")
    client_secret = _env("STRAVA_CLIENT_SECRET")
    refresh_token = _env("STRAVA_REFRESH_TOKEN")
    if not client_id or not client_secret or not refresh_token:
        raise SystemExit("Missing STRAVA_CLIENT_ID / STRAVA_CLIENT_SECRET / STRAVA_REFRESH_TOKEN env vars.")

    payload = _http_post_form(
        STRAVA_TOKEN_URL,
        {
            "client_id": client_id,
            "client_secret": client_secret,
            "refresh_token": refresh_token,
            "grant_type": "refresh_token",
        },
    )
    token = payload.get("access_token")
    if not token:
        raise RuntimeError(f"Strava token refresh failed: {payload}")
    return str(token)


def _parse_dt(s: str) -> datetime:
    # Strava uses ISO8601 like "2026-04-23T19:34:12Z"
    if s.endswith("Z"):
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    return datetime.fromisoformat(s)


def list_activities(access_token: str, after: datetime, before: datetime) -> list[Activity]:
    headers = {"Authorization": f"Bearer {access_token}"}
    params = {
        "after": str(int(after.replace(tzinfo=timezone.utc).timestamp())),
        "before": str(int(before.replace(tzinfo=timezone.utc).timestamp())),
        "per_page": "200",
        "page": "1",
    }
    raw = _http_get_json(STRAVA_ACTIVITIES_URL, headers=headers, params=params)
    if not isinstance(raw, list):
        raise RuntimeError(f"Unexpected Strava activities response: {raw}")

    out: list[Activity] = []
    for a in raw:
        if not isinstance(a, dict):
            continue
        out.append(
            Activity(
                id=int(a.get("id")),
                name=str(a.get("name") or ""),
                start_date=_parse_dt(str(a.get("start_date"))),
                distance_m=float(a.get("distance") or 0.0),
                moving_time_s=int(a.get("moving_time") or 0),
                elapsed_time_s=int(a.get("elapsed_time") or 0),
                average_speed_mps=(float(a["average_speed"]) if a.get("average_speed") is not None else None),
                average_heartrate=(float(a["average_heartrate"]) if a.get("average_heartrate") is not None else None),
                max_heartrate=(float(a["max_heartrate"]) if a.get("max_heartrate") is not None else None),
                type=str(a.get("type") or ""),
            )
        )
    return out


def fetch_streams(access_token: str, activity_id: int) -> Streams:
    """
    Fetch per-sample streams for an activity.
    We request time + heartrate + velocity_smooth (pace).
    """
    headers = {"Authorization": f"Bearer {access_token}"}
    params = {
        "keys": "time,heartrate,velocity_smooth",
        "key_by_type": "true",
    }
    raw = _http_get_json(STRAVA_STREAMS_URL_TMPL.format(activity_id=activity_id), headers=headers, params=params)
    if not isinstance(raw, dict):
        raise RuntimeError(f"Unexpected Strava streams response: {raw}")

    def _get_list(key: str) -> list | None:
        v = raw.get(key)
        if not isinstance(v, dict):
            return None
        data = v.get("data")
        if not isinstance(data, list):
            return None
        return data

    time_data = _get_list("time")
    if not time_data:
        raise RuntimeError(f"Strava streams missing 'time' for activity {activity_id}: {raw}")

    # Ensure ints
    time_s: list[int] = []
    for x in time_data:
        try:
            time_s.append(int(x))
        except Exception:
            pass
    if not time_s:
        raise RuntimeError(f"Invalid 'time' stream for activity {activity_id}")

    hr_data = _get_list("heartrate")
    heartrate_bpm: list[int] | None = None
    if hr_data:
        hr_out: list[int] = []
        for x in hr_data:
            try:
                hr_out.append(int(x))
            except Exception:
                hr_out.append(0)
        heartrate_bpm = hr_out if hr_out else None

    vel_data = _get_list("velocity_smooth")
    velocity_mps: list[float] | None = None
    if vel_data:
        vel_out: list[float] = []
        for x in vel_data:
            try:
                vel_out.append(float(x))
            except Exception:
                vel_out.append(0.0)
        velocity_mps = vel_out if vel_out else None

    # Strava streams are usually aligned; if not, we truncate to min length.
    n = len(time_s)
    if heartrate_bpm is not None:
        n = min(n, len(heartrate_bpm))
    if velocity_mps is not None:
        n = min(n, len(velocity_mps))
    time_s = time_s[:n]
    if heartrate_bpm is not None:
        heartrate_bpm = heartrate_bpm[:n]
    if velocity_mps is not None:
        velocity_mps = velocity_mps[:n]

    return Streams(time_s=time_s, heartrate_bpm=heartrate_bpm, velocity_mps=velocity_mps)


def is_running_activity(a: Activity) -> bool:
    # Strava types vary; "Run" is typical. Keep flexible.
    return a.type.lower() in {"run", "trailrun", "virtualrun"}


def pace_min_per_km(avg_speed_mps: float | None) -> float | None:
    if not avg_speed_mps or avg_speed_mps <= 0:
        return None
    sec_per_km = 1000.0 / avg_speed_mps
    return sec_per_km / 60.0


def fmt_duration(seconds: int) -> str:
    m, s = divmod(max(0, seconds), 60)
    h, m = divmod(m, 60)
    if h:
        return f"{h}u {m:02d}m"
    return f"{m}m {s:02d}s"


def fmt_pace(p: float | None) -> str:
    if p is None:
        return "n.v.t."
    total_sec = int(round(p * 60))
    mm, ss = divmod(total_sec, 60)
    return f"{mm}:{ss:02d} /km"


def classify_run(a: Activity) -> str:
    # Lightweight heuristic: if elapsed >> moving, there were substantial pauses -> likely run/walk or intervals.
    if a.moving_time_s <= 0:
        return "onbekend"
    ratio = a.elapsed_time_s / a.moving_time_s if a.elapsed_time_s else 1.0
    if ratio >= 1.20:
        return "run/walk of interval (veel pauzes)"
    return "rustige duur (aaneengesloten)"


def _pace_from_velocity(velocity_mps: float | None) -> float | None:
    return pace_min_per_km(velocity_mps)


def segment_blocks(
    streams: Streams,
    *,
    walk_pace_threshold_min_per_km: float = 7.75,  # ~7:45/km
    pause_velocity_mps: float = 0.3,
    min_block_seconds: int = 25,
) -> list[Block]:
    """
    Segment a run into run/walk/pause blocks from streams.
    Uses velocity_smooth when available; otherwise falls back to pause detection only.
    Adds a minimum block duration to avoid rapid flipping due to noise.
    """
    n = len(streams.time_s)
    if n == 0:
        return []

    vel = streams.velocity_mps
    if vel is None:
        # Can't reliably segment run vs walk; return one unknown-ish run block.
        return [Block(kind="run", start_idx=0, end_idx_exclusive=n)]

    def kind_at(i: int) -> str:
        v = vel[i]
        if v <= pause_velocity_mps:
            return "pause"
        p = _pace_from_velocity(v)
        if p is None:
            return "run"
        return "walk" if p >= walk_pace_threshold_min_per_km else "run"

    blocks: list[Block] = []
    cur_kind = kind_at(0)
    cur_start = 0

    def duration_seconds(start_idx: int, end_idx_excl: int) -> int:
        if end_idx_excl <= start_idx + 1:
            return 0
        return int(streams.time_s[end_idx_excl - 1] - streams.time_s[start_idx])

    i = 1
    while i < n:
        k = kind_at(i)
        if k == cur_kind:
            i += 1
            continue

        # candidate boundary at i
        cur_dur = duration_seconds(cur_start, i)
        if cur_dur < min_block_seconds:
            # too short: treat as noise; keep current kind
            i += 1
            continue

        blocks.append(Block(kind=cur_kind, start_idx=cur_start, end_idx_exclusive=i))
        cur_kind = k
        cur_start = i
        i += 1

    # finalize
    if duration_seconds(cur_start, n) >= 1:
        blocks.append(Block(kind=cur_kind, start_idx=cur_start, end_idx_exclusive=n))

    # merge adjacent same-kind blocks (can happen due to noise suppression)
    merged: list[Block] = []
    for b in blocks:
        if merged and merged[-1].kind == b.kind and merged[-1].end_idx_exclusive == b.start_idx:
            prev = merged[-1]
            merged[-1] = Block(kind=prev.kind, start_idx=prev.start_idx, end_idx_exclusive=b.end_idx_exclusive)
        else:
            merged.append(b)
    return merged


def blocks_to_readable_pattern(block_summaries: list[dict]) -> str:
    """
    Turn blocks into a compact pattern string like:
    run 3:00 / walk 2:00 × 6 (approx)
    """
    # Filter only run/walk/pause with meaningful duration
    filtered = [b for b in block_summaries if (b.get("duration_s") or 0) >= 20 and b.get("kind") in {"run", "walk"}]
    if len(filtered) < 2:
        return "n.v.t."

    # Use the first run+walk pair as template
    first_run = next((b for b in filtered if b["kind"] == "run"), None)
    if not first_run:
        return "n.v.t."
    run_dur = int(first_run["duration_s"])
    # first walk after that
    idx = filtered.index(first_run)
    first_walk = next((b for b in filtered[idx + 1 :] if b["kind"] == "walk"), None)
    if not first_walk:
        return f"run {run_dur//60}:{run_dur%60:02d} (aaneengesloten/blokken)"
    walk_dur = int(first_walk["duration_s"])

    # Count pairs by scanning for run->walk transitions that roughly match durations (+/-40s)
    def close(a: int, b: int) -> bool:
        return abs(a - b) <= 40

    pairs = 0
    i = 0
    while i < len(filtered) - 1:
        if filtered[i]["kind"] == "run" and filtered[i + 1]["kind"] == "walk":
            if close(int(filtered[i]["duration_s"]), run_dur) and close(int(filtered[i + 1]["duration_s"]), walk_dur):
                pairs += 1
                i += 2
                continue
        i += 1

    return f"run {run_dur//60}:{run_dur%60:02d} / walk {walk_dur//60}:{walk_dur%60:02d} × {max(1, pairs)} (geschat)"


def _avg(values: list[float] | list[int] | None) -> float | None:
    if not values:
        return None
    if len(values) == 0:
        return None
    return float(sum(values) / len(values))


def summarize_blocks(streams: Streams, blocks: list[Block]) -> list[dict]:
    """
    Compute block-level metrics for email/LLM:
    - duration
    - avg pace (if velocity available)
    - avg HR (if HR available)
    - HR change within block (end-start)
    - For walk blocks: HR drop over first 60 seconds (if possible)
    """
    out: list[dict] = []
    hr = streams.heartrate_bpm
    vel = streams.velocity_mps

    def block_duration(b: Block) -> int:
        if b.end_idx_exclusive <= b.start_idx + 1:
            return 0
        return int(streams.time_s[b.end_idx_exclusive - 1] - streams.time_s[b.start_idx])

    for b in blocks:
        idxs = list(range(b.start_idx, b.end_idx_exclusive))
        hr_vals: list[int] | None = [hr[i] for i in idxs if hr and hr[i] > 0] if hr else None
        vel_vals: list[float] | None = [vel[i] for i in idxs if vel and vel[i] > 0] if vel else None

        avg_vel = _avg(vel_vals)
        avg_pace = _pace_from_velocity(avg_vel) if avg_vel is not None else None
        avg_hr = _avg(hr_vals) if hr_vals else None

        hr_start = None
        hr_end = None
        if hr_vals and hr:
            # first/last non-zero within block
            for i in idxs:
                if hr[i] > 0:
                    hr_start = hr[i]
                    break
            for i in reversed(idxs):
                if hr[i] > 0:
                    hr_end = hr[i]
                    break

        hr_change = (hr_end - hr_start) if (hr_start is not None and hr_end is not None) else None

        hr_drop_60s = None
        if b.kind == "walk" and hr and hr_start is not None:
            # find index ~60s after block start within block
            start_t = streams.time_s[b.start_idx]
            target_t = start_t + 60
            j = None
            for ii in idxs:
                if streams.time_s[ii] >= target_t and hr[ii] > 0:
                    j = ii
                    break
            if j is not None:
                hr_drop_60s = hr_start - hr[j]

        out.append(
            {
                "kind": b.kind,
                "duration_s": block_duration(b),
                "avg_pace_min_per_km": (round(avg_pace, 2) if avg_pace is not None else None),
                "avg_hr": (round(avg_hr, 1) if avg_hr is not None else None),
                "hr_change": hr_change,
                "hr_drop_60s": hr_drop_60s,
            }
        )
    return out


def week_range_amsterdam(now_utc: datetime) -> tuple[datetime, datetime, datetime, datetime]:
    """
    Returns (week_start_local, week_end_local_exclusive, after_utc, before_utc)
    Week is Mon 00:00 -> next Mon 00:00 in Europe/Amsterdam.
    """
    if ZoneInfo is None:
        raise SystemExit("Python zoneinfo not available. Need Python 3.9+.")

    tz = ZoneInfo("Europe/Amsterdam")
    now_local = now_utc.astimezone(tz)
    # find start of current week (Mon)
    week_start_local = (now_local - timedelta(days=now_local.weekday())).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    week_end_local = week_start_local + timedelta(days=7)

    after_utc = week_start_local.astimezone(timezone.utc)
    before_utc = week_end_local.astimezone(timezone.utc)
    return week_start_local, week_end_local, after_utc, before_utc


def should_send_now(now_utc: datetime) -> bool:
    """
    Gate sending so GitHub Actions can run multiple UTC times,
    but we only send at Sunday 22:00 in Europe/Amsterdam.
    """
    if (_env("FORCE_SEND") or "").lower() in {"1", "true", "yes", "y"}:
        return True
    if ZoneInfo is None:
        return True
    tz = ZoneInfo("Europe/Amsterdam")
    local = now_utc.astimezone(tz)
    return local.weekday() == 6 and local.hour == 22  # Sunday=6


def build_week_summary(runs: list[Activity]) -> dict:
    runs_sorted = sorted(runs, key=lambda a: a.start_date)
    total_distance_km = sum(a.distance_m for a in runs_sorted) / 1000.0
    total_moving_s = sum(a.moving_time_s for a in runs_sorted)
    avg_hr_values = [a.average_heartrate for a in runs_sorted if a.average_heartrate is not None]
    avg_hr = sum(avg_hr_values) / len(avg_hr_values) if avg_hr_values else None

    details = []
    for a in runs_sorted:
        details.append(
            {
                "date": a.start_date.date().isoformat(),
                "name": a.name,
                "distance_km": round(a.distance_m / 1000.0, 2),
                "moving_time": fmt_duration(a.moving_time_s),
                "elapsed_time": fmt_duration(a.elapsed_time_s),
                "avg_pace": fmt_pace(pace_min_per_km(a.average_speed_mps)),
                "avg_hr": a.average_heartrate,
                "max_hr": a.max_heartrate,
                "classification": classify_run(a),
            }
        )

    return {
        "run_count": len(runs_sorted),
        "total_distance_km": round(total_distance_km, 2),
        "total_moving_time": fmt_duration(total_moving_s),
        "avg_heart_rate": round(avg_hr, 1) if avg_hr is not None else None,
        "runs": details,
    }


def build_week_detailed(access_token: str, runs: list[Activity]) -> list[dict]:
    detailed: list[dict] = []
    for a in sorted(runs, key=lambda x: x.start_date):
        try:
            streams = fetch_streams(access_token, a.id)
            blocks = segment_blocks(streams)
            block_summaries = summarize_blocks(streams, blocks)
            pattern = blocks_to_readable_pattern(block_summaries)
        except Exception as e:
            block_summaries = []
            pattern = "n.v.t."
            err = str(e)
        else:
            err = None

        detailed.append(
            {
                "id": a.id,
                "date": a.start_date.date().isoformat(),
                "name": a.name,
                "distance_km": round(a.distance_m / 1000.0, 2),
                "moving_time_s": a.moving_time_s,
                "elapsed_time_s": a.elapsed_time_s,
                "avg_pace_min_per_km": pace_min_per_km(a.average_speed_mps),
                "avg_hr": a.average_heartrate,
                "pattern_estimate": pattern,
                "blocks": block_summaries,
                "streams_error": err,
            }
        )
    return detailed


def openai_generate_coach_email(week_summary: dict) -> str | None:
    api_key = _env("OPENAI_API_KEY")
    if not api_key:
        return None

    model = _env("OPENAI_MODEL", "gpt-4o-mini")

    system = (
        "Je bent een Nederlandse AI hardloopcoach. Je schrijft concreet en praktisch, als een persoonlijke coach. "
        "Je maakt altijd een schema voor 1 week vooruit met 2 trainingen. "
        "De loper is beginner, traint 2x per week, komt van run/walk intervallen en traint daarnaast 4x per week kracht. "
        "Doel: 10 km Singelloop Utrecht op 4 okt 2026 < 60 min (6:00/km), maar eerst veilig 10 km aaneengesloten kunnen lopen. "
        "Je baseert je analyse vooral op de intervalblokken uit runs_detailed (tempo + HR per blok), niet op de totale gemiddelden van de run."
    )

    user = {
        "week_summary": week_summary,
        "runs_detailed": week_summary.get("runs_detailed"),
        "current_baseline": "3 min hardlopen / 2 min wandelen × 6 voelde haalbaar (laatste bekende training).",
        "output_structure": [
            "Korte analyse van mijn laatste training(en)",
            "Belangrijkste inzichten (bullet points)",
            "Eventuele risico’s",
            "Concreet schema voor de volgende week (2 trainingen, volledig uitgeschreven)",
        ],
        "constraints": [
            "Bouw geleidelijk op (beginner, blessurepreventie).",
            "Geen algemene tips, maar specifiek advies op basis van de weekdata.",
            "Gebruik vooral de blokken (run vs walk) uit runs_detailed om advies te geven over harder/zachter lopen en interval-opbouw.",
            "Tempo-indicaties in RPE/praattempo en (als mogelijk) min/km richting 6:00, maar niet forceren.",
            "Als HR in loopblokken snel oploopt of herstel in wandelblokken slecht is (HR zakt weinig), adviseer rustiger lopen of dezelfde interval nog een week herhalen.",
            "Als pace in loopblokken stabiel is en herstel goed is (HR daalt duidelijk binnen 60–90 sec), adviseer een kleine progressie: +30–60 sec lopen per blok of -15–30 sec wandelen.",
        ],
    }

    body = json.dumps(user, ensure_ascii=False)

    payload = json.dumps(
        {
            "model": model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": body},
            ],
            "temperature": 0.4,
        }
    ).encode("utf-8")

    req = urllib.request.Request(
        OPENAI_CHAT_COMPLETIONS_URL,
        data=payload,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        # e.g. 429 Too Many Requests (rate limit / quota). Fall back to deterministic email.
        try:
            detail = e.read().decode("utf-8")
        except Exception:
            detail = ""
        print(f"OpenAI HTTPError {e.code}. Falling back. {detail[:500]}")
        return None
    except Exception as e:  # pragma: no cover
        print(f"OpenAI request failed. Falling back. {e}")
        return None

    try:
        return str(data["choices"][0]["message"]["content"]).strip()
    except Exception:  # pragma: no cover
        raise RuntimeError(f"Unexpected OpenAI response: {data}")


def fallback_email(week_summary: dict) -> str:
    runs = week_summary.get("runs", [])
    runs_detailed = week_summary.get("runs_detailed") or []
    lines = []
    lines.append("Korte analyse van je laatste training(en)")
    if not runs:
        lines.append("- Deze week zie ik geen hardloopactiviteiten in Strava.")
    else:
        lines.append(f"- Aantal runs: {week_summary.get('run_count')}")
        lines.append(f"- Totaal afstand: {week_summary.get('total_distance_km')} km")
        lines.append(f"- Totale beweegtijd: {week_summary.get('total_moving_time')}")
        if week_summary.get("avg_heart_rate") is not None:
            lines.append(f"- Gem. hartslag (waar beschikbaar): {week_summary.get('avg_heart_rate')} bpm")
        lines.append("")
        lines.append("Details")
        for r in runs:
            lines.append(
                f"- {r['date']}: {r['distance_km']} km in {r['moving_time']} (pace {r['avg_pace']}, type: {r['classification']})"
            )
        if runs_detailed:
            lines.append("")
            lines.append("Intervalblokken (op basis van tempo/HR over tijd)")
            for rd in runs_detailed:
                lines.append(f"- {rd['date']}: patroon {rd.get('pattern_estimate')}")
                if rd.get("streams_error"):
                    lines.append(f"  - streams niet beschikbaar: {rd['streams_error']}")
                    continue
                # Show first few blocks only
                blocks = rd.get("blocks") or []
                for b in blocks[:10]:
                    dur = int(b.get('duration_s') or 0)
                    lines.append(
                        f"  - {b.get('kind')}: {dur//60}:{dur%60:02d}, pace {fmt_pace(b.get('avg_pace_min_per_km'))}, HR {b.get('avg_hr')}, HRΔ {b.get('hr_change')}, HR↓60s {b.get('hr_drop_60s')}"
                    )

    lines.append("")
    lines.append("Belangrijkste inzichten")
    lines.append("- Hou het comfortabel (praattempo) zodat je naast krachttraining goed herstelt.")
    lines.append("- Progressie: per week één stapje (iets langer lopen óf iets minder wandelen).")
    lines.append("")
    lines.append("Eventuele risico’s")
    lines.append("- Als je pijn krijgt die 48 uur blijft (scheen/knie/achilles): stap terug en laat het weten.")
    lines.append("")
    lines.append("Concreet schema voor de volgende week (2 trainingen)")
    lines.append("Training 1: 5 min wandelen, dan 4 min lopen / 2 min wandelen × 6, 5–8 min uitwandelen.")
    lines.append("Training 2: 5 min wandelen, dan 3 min lopen / 1.5 min wandelen × 8, 5–8 min uitwandelen.")
    return "\n".join(lines)


def _render_html_email(subject: str, plain_text: str, week_summary: dict) -> str:
    runs = week_summary.get("runs") or []
    runs_detailed = week_summary.get("runs_detailed") or []

    def esc(s: object) -> str:
        return html.escape("" if s is None else str(s))

    # Basic inline CSS (email-safe)
    css = """
    body { font-family: -apple-system, BlinkMacSystemFont, Segoe UI, Roboto, Arial, sans-serif; color:#111; }
    h2 { margin: 18px 0 8px; }
    .meta { color:#555; font-size: 13px; margin-bottom: 12px; }
    .card { border:1px solid #e5e5e5; border-radius:10px; padding:12px 14px; margin:12px 0; }
    table { width:100%; border-collapse: collapse; }
    th, td { border-bottom:1px solid #eee; padding:8px 6px; text-align:left; font-size: 13px; vertical-align: top; }
    th { background:#fafafa; font-weight:600; }
    .pill { display:inline-block; padding:2px 8px; border-radius: 999px; background:#f2f4f7; font-size:12px; color:#333; }
    .muted { color:#666; }
    .mono { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; font-size: 12px; white-space: pre-wrap; }
    """

    # Summary table
    summary_rows = []
    for r in runs:
        summary_rows.append(
            f"<tr>"
            f"<td>{esc(r.get('date'))}</td>"
            f"<td>{esc(r.get('distance_km'))} km</td>"
            f"<td>{esc(r.get('moving_time'))}</td>"
            f"<td>{esc(r.get('avg_pace'))}</td>"
            f"<td>{esc(r.get('avg_hr') or '—')}</td>"
            f"<td><span class='pill'>{esc(r.get('classification'))}</span></td>"
            f"</tr>"
        )
    summary_table = (
        "<div class='card'>"
        "<h2>Overzicht (deze week)</h2>"
        "<table>"
        "<thead><tr><th>Datum</th><th>Afstand</th><th>Tijd</th><th>Tempo</th><th>HR</th><th>Type</th></tr></thead>"
        f"<tbody>{''.join(summary_rows) if summary_rows else '<tr><td colspan=6>Geen runs gevonden.</td></tr>'}</tbody>"
        "</table>"
        "</div>"
    )

    # Block tables per run (first ~2 runs to keep email readable)
    blocks_html = []
    for rd in runs_detailed[:3]:
        blocks = rd.get("blocks") or []
        header = (
            f"<div class='card'>"
            f"<h2>Intervalblokken {esc(rd.get('date'))}</h2>"
            f"<div class='meta'>{esc(rd.get('name'))} — {esc(rd.get('distance_km'))} km — patroon: "
            f"<span class='pill'>{esc(rd.get('pattern_estimate'))}</span></div>"
        )
        if rd.get("streams_error"):
            blocks_html.append(header + f"<div class='muted'>Streams niet beschikbaar: {esc(rd.get('streams_error'))}</div></div>")
            continue

        rows = []
        for b in blocks[:18]:
            dur = int(b.get("duration_s") or 0)
            rows.append(
                "<tr>"
                f"<td><span class='pill'>{esc(b.get('kind'))}</span></td>"
                f"<td class='mono'>{dur//60}:{dur%60:02d}</td>"
                f"<td>{esc(fmt_pace(b.get('avg_pace_min_per_km')))}</td>"
                f"<td>{esc(b.get('avg_hr') or '—')}</td>"
                f"<td>{esc(b.get('hr_change') if b.get('hr_change') is not None else '—')}</td>"
                f"<td>{esc(b.get('hr_drop_60s') if b.get('hr_drop_60s') is not None else '—')}</td>"
                "</tr>"
            )

        blocks_html.append(
            header
            + "<table>"
            + "<thead><tr><th>Blok</th><th>Duur</th><th>Gem. tempo</th><th>Gem. HR</th><th>HRΔ</th><th>HR↓60s</th></tr></thead>"
            + f"<tbody>{''.join(rows) if rows else '<tr><td colspan=6>Geen blokken gevonden.</td></tr>'}</tbody>"
            + "</table>"
            + "</div>"
        )

    coach_text_html = (
        "<div class='card'>"
        "<h2>Coachbericht</h2>"
        f"<div class='mono'>{esc(plain_text)}</div>"
        "</div>"
    )

    return (
        "<!doctype html><html><head><meta charset='utf-8'>"
        f"<title>{esc(subject)}</title>"
        f"<style>{css}</style></head><body>"
        f"<div class='meta'>{esc(subject)}</div>"
        f"{summary_table}"
        f"{''.join(blocks_html)}"
        f"{coach_text_html}"
        "</body></html>"
    )

def send_email(subject: str, body: str, week_summary: dict) -> None:
    gmail_user = _env("GMAIL_USER")
    gmail_app_password = _env("GMAIL_APP_PASSWORD")
    mail_to = _env("MAIL_TO")
    if not gmail_user or not gmail_app_password or not mail_to:
        raise SystemExit("Missing GMAIL_USER / GMAIL_APP_PASSWORD / MAIL_TO env vars.")

    # Always send multipart (plain + html) so it looks nice but stays compatible.
    msg = MIMEMultipart("alternative")
    msg["From"] = gmail_user
    msg["To"] = mail_to
    msg["Subject"] = subject

    plain_part = MIMEText(body, "plain", "utf-8")
    html_body = _render_html_email(subject, body, week_summary)
    html_part = MIMEText(html_body, "html", "utf-8")
    msg.attach(plain_part)
    msg.attach(html_part)

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
        server.login(gmail_user, gmail_app_password)
        server.sendmail(gmail_user, [mail_to], msg.as_string())


def main() -> int:
    now_utc = datetime.now(timezone.utc)
    if not should_send_now(now_utc):
        print("Not Sunday 22:00 Europe/Amsterdam. Exiting without sending.")
        return 0

    week_start_local, week_end_local, after_utc, before_utc = week_range_amsterdam(now_utc)
    access_token = get_strava_access_token()
    acts = list_activities(access_token, after=after_utc, before=before_utc)
    runs = [a for a in acts if is_running_activity(a)]
    week_summary = build_week_summary(runs)
    week_summary["runs_detailed"] = build_week_detailed(access_token, runs)

    body = openai_generate_coach_email(week_summary) or fallback_email(week_summary)
    subject = f"Hardloopcoach weekplan ({week_start_local.date().isoformat()}–{(week_end_local.date() - timedelta(days=1)).isoformat()})"
    send_email(subject, body, week_summary)
    print("Email sent.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

