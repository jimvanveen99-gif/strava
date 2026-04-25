import json
import os
import smtplib
import ssl
import sys
import argparse
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import html
import io
import re
from email.mime.image import MIMEImage
from typing import Optional, Union, Tuple, List, Dict, Any

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
    average_speed_mps: Optional[float]
    average_heartrate: Optional[float]
    max_heartrate: Optional[float]
    type: str


@dataclass(frozen=True)
class Streams:
    time_s: list[int]
    heartrate_bpm: Optional[list[int]]
    velocity_mps: Optional[list[float]]


@dataclass(frozen=True)
class Block:
    kind: str  # "run" | "walk" | "pause"
    start_idx: int
    end_idx_exclusive: int


def _env(name: str, default: Optional[str] = None) -> Optional[str]:
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


def _http_get_json(url: str, headers: dict[str, str], params: Optional[dict[str, str]] = None) -> object:
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

    def _get_list(key: str) -> Optional[list]:
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
    heartrate_bpm: Optional[list[int]] = None
    if hr_data:
        hr_out: list[int] = []
        for x in hr_data:
            try:
                hr_out.append(int(x))
            except Exception:
                hr_out.append(0)
        heartrate_bpm = hr_out if hr_out else None

    vel_data = _get_list("velocity_smooth")
    velocity_mps: Optional[list[float]] = None
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


def pace_min_per_km(avg_speed_mps: Optional[float]) -> Optional[float]:
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


def fmt_pace(p: Optional[float]) -> str:
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


def _pace_from_velocity(velocity_mps: Optional[float]) -> Optional[float]:
    return pace_min_per_km(velocity_mps)


def segment_blocks(
    streams: Streams,
    *,
    walk_pace_threshold_min_per_km: Optional[float] = None,  # auto if None
    pause_velocity_mps: float = 0.3,
    min_block_seconds: int = 45,
    smooth_window_seconds: int = 15,
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

    # Build a smoothed pace series to avoid noisy flips.
    # We use a simple rolling median over ~smooth_window_seconds.
    paces: list[Optional[float]] = []
    for v in vel:
        if v <= pause_velocity_mps:
            paces.append(None)
        else:
            paces.append(_pace_from_velocity(v))

    def _rolling_median(values: list[Optional[float]]) -> list[Optional[float]]:
        out: list[Optional[float]] = [None] * len(values)
        w = max(5, smooth_window_seconds)
        for i in range(len(values)):
            # window by TIME, not by index (streams can be irregular)
            t0 = streams.time_s[i] - w
            t1 = streams.time_s[i] + w
            start = i
            while start > 0 and streams.time_s[start - 1] >= t0:
                start -= 1
            end = i + 1
            while end < len(values) and streams.time_s[end] <= t1:
                end += 1
            window = [x for x in values[start:end] if x is not None]
            if not window:
                out[i] = None
                continue
            window.sort()
            out[i] = window[len(window) // 2]
        return out

    smooth_paces = _rolling_median(paces)

    # Auto threshold: find two clusters (run vs walk) in observed paces.
    # Simple 1D k-means with 2 centroids.
    if walk_pace_threshold_min_per_km is None:
        observed = [p for p in smooth_paces if p is not None and 3.0 <= p <= 20.0]
        if len(observed) >= 30:
            observed_sorted = sorted(observed)
            c1 = observed_sorted[int(len(observed_sorted) * 0.25)]
            c2 = observed_sorted[int(len(observed_sorted) * 0.75)]
            if c1 > c2:
                c1, c2 = c2, c1
            for _ in range(10):
                g1: list[float] = []
                g2: list[float] = []
                for p in observed:
                    if abs(p - c1) <= abs(p - c2):
                        g1.append(p)
                    else:
                        g2.append(p)
                if g1:
                    c1 = sum(g1) / len(g1)
                if g2:
                    c2 = sum(g2) / len(g2)
                if c1 > c2:
                    c1, c2 = c2, c1
            walk_pace_threshold_min_per_km = (c1 + c2) / 2.0
        else:
            # Fallback default if too little data
            walk_pace_threshold_min_per_km = 8.0  # ~8:00/km

    threshold = float(walk_pace_threshold_min_per_km)

    def kind_at(i: int) -> str:
        v = vel[i]
        if v <= pause_velocity_mps:
            return "pause"
        p = smooth_paces[i]
        if p is None:
            return "run"
        return "walk" if p >= threshold else "run"

    blocks: list[Block] = []
    cur_kind = kind_at(0)
    cur_start = 0

    def duration_seconds(start_idx: int, end_idx_excl: int) -> int:
        if end_idx_excl <= start_idx + 1:
            return 0
        return int(streams.time_s[end_idx_excl - 1] - streams.time_s[start_idx] + 1)

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

    # absorb very short pauses (e.g. a stoplight) into neighboring block
    def dur(b: Block) -> int:
        if b.end_idx_exclusive <= b.start_idx + 1:
            return 0
        return int(streams.time_s[b.end_idx_exclusive - 1] - streams.time_s[b.start_idx] + 1)

    cleaned: list[Block] = []
    i = 0
    while i < len(merged):
        b = merged[i]
        # Stoplights in city running often show up as short "pause" blocks.
        # Absorb these so the readable pattern becomes run/walk only.
        if b.kind == "pause" and dur(b) <= 75:
            if cleaned:
                prev = cleaned[-1]
                cleaned[-1] = Block(kind=prev.kind, start_idx=prev.start_idx, end_idx_exclusive=b.end_idx_exclusive)
            elif i + 1 < len(merged):
                nxt = merged[i + 1]
                merged[i + 1] = Block(kind=nxt.kind, start_idx=b.start_idx, end_idx_exclusive=nxt.end_idx_exclusive)
            i += 1
            continue
        cleaned.append(b)
        i += 1

    final: list[Block] = []
    for b in cleaned:
        if final and final[-1].kind == b.kind and final[-1].end_idx_exclusive == b.start_idx:
            prev = final[-1]
            final[-1] = Block(kind=prev.kind, start_idx=prev.start_idx, end_idx_exclusive=b.end_idx_exclusive)
        else:
            final.append(b)
    return final


def blocks_to_readable_pattern(block_summaries: list[dict]) -> str:
    """
    Turn blocks into a compact pattern string like:
    run 3:00 / walk 2:00 × 6 (approx)
    """
    # Ignore pauses; we want a clear run/walk pattern even in city runs.
    filtered = [b for b in block_summaries if (b.get("duration_s") or 0) >= 20 and b.get("kind") in {"run", "walk"}]
    if len(filtered) < 2:
        return "n.v.t."

    run_durs = [int(b["duration_s"]) for b in filtered if b.get("kind") == "run"]
    walk_durs = [int(b["duration_s"]) for b in filtered if b.get("kind") == "walk"]
    if not run_durs:
        return "n.v.t."

    def _median_int(xs: list[int]) -> int:
        ys = sorted(xs)
        return int(ys[len(ys) // 2])

    def _round_to_nearest_30s(seconds: int) -> int:
        # Round to nearest 30 seconds (whole/half minutes).
        if seconds <= 0:
            return 0
        return int(round(seconds / 30.0) * 30)

    run_dur = _median_int(run_durs)
    walk_dur = _median_int(walk_durs) if walk_durs else 0
    run_dur = _round_to_nearest_30s(run_dur)
    walk_dur = _round_to_nearest_30s(walk_dur)
    # Estimate repetitions by counting run blocks (more robust than exact matching).
    reps = min(len(run_durs), len(walk_durs)) if walk_durs else len(run_durs)

    if not walk_durs:
        return f"run {run_dur//60}:{run_dur%60:02d} (aaneengesloten/blokken)"
    return f"run {run_dur//60}:{run_dur%60:02d} / walk {walk_dur//60}:{walk_dur%60:02d} × {max(1, reps)} (geschat)"


def _parse_pattern_durations_seconds(pattern: str) -> Optional[tuple[int, int]]:
    """
    Parse pattern strings like:
      "run 3:00 / walk 2:00 × 6 (geschat)"
    Returns (run_seconds, walk_seconds).
    """
    m = re.search(r"run\s+(\d+):(\d{2})\s*/\s*walk\s+(\d+):(\d{2})", pattern, re.IGNORECASE)
    if not m:
        return None
    rmin, rsec, wmin, wsec = (int(m.group(1)), int(m.group(2)), int(m.group(3)), int(m.group(4)))
    return (rmin * 60 + rsec, wmin * 60 + wsec)


def planned_timer_blocks_from_streams(
    streams: Streams,
    *,
    run_seconds: int,
    walk_seconds: int,
    reps: int = 6,
    pause_velocity_mps: float = 0.3,
) -> list[Block]:
    """
    Create blocks aligned to an intended run/walk timer (e.g. 3:00/2:00),
    independent of stoplights and Strava's block segmentation.

    We anchor at the first moment where velocity is above the pause threshold
    (i.e. the run actually starts), then slice time into alternating windows.
    """
    if not streams.time_s or not streams.velocity_mps:
        return []
    if run_seconds <= 0 or walk_seconds <= 0 or reps <= 0:
        return []

    # Anchor at first non-pause sample.
    start_idx = None
    for i, v in enumerate(streams.velocity_mps):
        if v is not None and v > pause_velocity_mps:
            start_idx = i
            break
    if start_idx is None:
        return []

    start_t = streams.time_s[start_idx]

    def idx_at_or_after(target_t: int) -> int:
        # Find first index with time >= target_t (linear scan from start_idx; streams are small enough).
        for j in range(start_idx, len(streams.time_s)):
            if streams.time_s[j] >= target_t:
                return j
        return len(streams.time_s) - 1

    out: list[Block] = []
    cur_t = start_t
    cur_idx = start_idx
    for _ in range(reps):
        # run window
        run_end_t = cur_t + run_seconds
        run_end_idx = idx_at_or_after(run_end_t)
        out.append(Block(kind="run", start_idx=cur_idx, end_idx_exclusive=min(len(streams.time_s), run_end_idx + 1)))
        cur_t = run_end_t
        cur_idx = run_end_idx

        # walk window
        walk_end_t = cur_t + walk_seconds
        walk_end_idx = idx_at_or_after(walk_end_t)
        out.append(Block(kind="walk", start_idx=cur_idx, end_idx_exclusive=min(len(streams.time_s), walk_end_idx + 1)))
        cur_t = walk_end_t
        cur_idx = walk_end_idx

        if cur_idx >= len(streams.time_s) - 2:
            break

    # Drop degenerate/empty blocks.
    cleaned: list[Block] = []
    for b in out:
        if b.end_idx_exclusive > b.start_idx + 1:
            cleaned.append(b)
    return cleaned


def _latest_run_with_timer(week_summary: dict) -> Optional[dict]:
    rds = [x for x in (week_summary.get("runs_detailed") or []) if isinstance(x, dict)]
    if not rds:
        return None
    rds_sorted = sorted(rds, key=lambda x: str(x.get("date") or ""))
    return rds_sorted[-1]


def coaching_from_timer_blocks(week_summary: dict) -> dict:
    """
    Produce a deterministic coaching verdict + next step based on timer-normalized blocks.
    """
    rd = _latest_run_with_timer(week_summary)
    if not rd:
        return {"verdict": "geen data", "details": None, "next_step": "Geen runs gevonden deze week. Doe 3:00/2:00 × 6 op praattempo."}

    blocks = rd.get("timer_blocks") or []
    run_blocks = [b for b in blocks if b.get("kind") == "run"]
    walk_blocks = [b for b in blocks if b.get("kind") == "walk"]
    if not run_blocks:
        return {"verdict": "onvoldoende data", "details": rd.get("streams_error"), "next_step": "Doe 3:00/2:00 × 6 op praattempo."}

    run_hr = [float(b["avg_hr"]) for b in run_blocks if b.get("avg_hr") is not None]
    walk_drop = [float(b["hr_drop_60s"]) for b in walk_blocks if b.get("hr_drop_60s") is not None]
    run_drift = [float(b["hr_change"]) for b in run_blocks if b.get("hr_change") is not None]

    def avg(xs: list[float]) -> Optional[float]:
        return (sum(xs) / len(xs)) if xs else None

    a_run_hr = avg(run_hr)
    a_drop = avg(walk_drop)
    a_drift = avg(run_drift)

    details = []
    if a_run_hr is not None:
        details.append(f"gem. HR loopblokken: {int(round(a_run_hr))} bpm")
    if a_drop is not None:
        details.append(f"HR↓60s (wandelblokken): {int(round(a_drop))} bpm")
    if a_drift is not None:
        details.append(f"HRΔ (loopblokken): {int(round(a_drift))} bpm")

    # conservative heuristics for a beginner with city noise + strength training
    too_hard = (a_run_hr is not None and a_run_hr >= 170) or (a_drift is not None and a_drift >= 55)
    poor_recovery = (a_drop is not None and a_drop < 15)
    good_recovery = (a_drop is not None and a_drop >= 25) and (a_run_hr is None or a_run_hr < 168)

    if too_hard or poor_recovery:
        verdict = "rustiger / consolideren"
        next_step = "Herhaal 3:00 lopen / 2:00 wandelen × 6, maar loop de loopblokken rustiger (praattempo)."
        suggestion = {"run_s": 180, "walk_s": 120, "reps": 6}
        delta = "zelfde als vorige week (focus: rustiger)"
    elif good_recovery:
        verdict = "stabiel → kleine progressie"
        # default progressive step: +30s run
        next_step = "Kleine progressie: 3:30 lopen / 2:00 wandelen × 6 (of alternatief: 3:00/1:45 × 6)."
        suggestion = {"run_s": 210, "walk_s": 120, "reps": 6}
        delta = "+30s lopen per blok"
    else:
        verdict = "oké → nog 1 week vasthouden"
        next_step = "Nog één week 3:00/2:00 × 6 consolideren; als dit stabiel blijft, daarna +30s lopen per blok."
        suggestion = {"run_s": 180, "walk_s": 120, "reps": 6}
        delta = "zelfde als vorige week"

    return {
        "verdict": verdict,
        "details": ", ".join(details) if details else None,
        "next_step": next_step,
        "suggestion": suggestion,
        "delta_label": delta,
    }


def build_plan(week_summary: dict) -> dict:
    """
    Build (1) next 2 weeks schedule and (2) a compact roadmap until race day.
    """
    coach = coaching_from_timer_blocks(week_summary)
    sug = coach.get("suggestion") or {"run_s": 180, "walk_s": 120, "reps": 6}
    run_s = int(sug.get("run_s") or 180)
    walk_s = int(sug.get("walk_s") or 120)
    reps = int(sug.get("reps") or 6)

    def fmt_interval(rs: int, ws: int, n: int) -> str:
        return f"5–8 min warm-up wandelen/joggen, dan {rs//60}:{rs%60:02d} lopen / {ws//60}:{ws%60:02d} wandelen × {n}, 5–8 min uitwandelen."

    next_2_weeks = [
        {
            "week": "Week 1",
            "session": "Training A",
            "when": "begin week (ma/di/wo)",
            "workout": fmt_interval(run_s, walk_s, reps),
            "delta": coach.get("delta_label") or "—",
            "why": "Opbouw op basis van herstel/HR in je laatste run; doel is stabiele, comfortabele loopblokken.",
        },
        {
            "week": "Week 1",
            "session": "Training B",
            "when": "weekend",
            "workout": "Easy duur op praattempo: 35–45 min totaal. Gebruik run/walk als nodig (bijv. 3:00/2:00) en vermijd ‘duwen’.",
            "delta": "volume / soepelheid",
            "why": "Extra aerobe prikkel zonder je krachttraining te slopen; bouwt conditie veilig op.",
        },
        {
            "week": "Week 2",
            "session": "Training A",
            "when": "begin week (ma/di/wo)",
            "workout": fmt_interval(min(run_s + 30, 300), walk_s, reps) if coach.get("verdict") == "stabiel → kleine progressie" else fmt_interval(run_s, walk_s, reps),
            "delta": "kleine stap (alleen als Week 1 goed voelde)",
            "why": "Progressie alleen als HR en gevoel stabiel blijven; anders consolideren.",
        },
        {
            "week": "Week 2",
            "session": "Training B",
            "when": "weekend",
            "workout": "Easy duur op praattempo: 40–50 min totaal. Eindig met 4× 20s ‘vlotte pas’ (niet sprinten) als je je fris voelt.",
            "delta": "+5 min (als herstel goed is)",
            "why": "Langzaam duurvolume omhoog; kleine prikkel voor loopeconomie zonder hoge hartslag.",
        },
    ]

    # Roadmap: very compact, week blocks towards race date.
    race = week_summary.get("race_countdown") or {}
    weeks_left = int(round(float(race.get("weeks") or 0))) if race.get("weeks") is not None else 0
    weeks_left = max(0, weeks_left)
    roadmap = []
    for i in range(min(weeks_left, 16)):  # keep short
        wk = i + 1
        if wk <= 6:
            focus = "Opbouw aaneengesloten lopen"
            target = "meer looptijd per blok, HR stabiel"
        elif wk <= 12:
            focus = "Duur + lichte tempo-prikkels"
            target = "richting 6:30–6:00/km stukken, zonder forceren"
        else:
            focus = "Specifieker richting 10 km"
            target = "tempo-blokken + taper"
        roadmap.append({"week": f"Week {wk}", "focus": focus, "target": target})

    return {"coach": coach, "next_2_weeks": next_2_weeks, "roadmap": roadmap}


def _avg(values: Optional[Union[list[float], list[int]]]) -> Optional[float]:
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
        return int(streams.time_s[b.end_idx_exclusive - 1] - streams.time_s[b.start_idx] + 1)

    for b in blocks:
        idxs = list(range(b.start_idx, b.end_idx_exclusive))
        hr_vals: Optional[list[int]] = [hr[i] for i in idxs if hr and hr[i] > 0] if hr else None
        vel_vals: Optional[list[float]] = [vel[i] for i in idxs if vel and vel[i] > 0] if vel else None

        avg_vel = _avg(vel_vals)
        avg_pace = _pace_from_velocity(avg_vel) if avg_vel is not None else None
        avg_hr = _avg(hr_vals) if hr_vals else None

        # Guardrail: in city runs and around manual pauses, a "pause" block can still
        # contain non-zero velocity samples (GPS/stream noise). Don't report a "pause"
        # as if it had a meaningful running pace.
        if b.kind == "pause":
            avg_pace = None

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

            # Also compute "timer-aligned" blocks for common run/walk training,
            # so stoplights/manual pauses don't distort block durations.
            timer_blocks = None
            timer_block_summaries: Optional[list[dict]] = None
            timer_pattern = None
            parsed = _parse_pattern_durations_seconds(pattern)
            # If we can't parse, fall back to baseline 3:00/2:00 (common for this project).
            run_s, walk_s = parsed if parsed else (180, 120)
            timer_blocks = planned_timer_blocks_from_streams(streams, run_seconds=run_s, walk_seconds=walk_s, reps=8)
            if timer_blocks:
                timer_block_summaries = summarize_blocks(streams, timer_blocks)
                timer_pattern = f"run {run_s//60}:{run_s%60:02d} / walk {walk_s//60}:{walk_s%60:02d} (timer)"

            # Store a downsampled series for plotting (keep small).
            series = _downsample_series_for_chart(streams)
        except Exception as e:
            block_summaries = []
            pattern = "n.v.t."
            err = str(e)
            series = None
            timer_block_summaries = None
            timer_pattern = None
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
                "timer_pattern": timer_pattern,
                "timer_blocks": timer_block_summaries,
                "series": series,
                "streams_error": err,
            }
        )
    return detailed


def _downsample_series_for_chart(streams: Streams, max_points: int = 240) -> Optional[dict]:
    """
    Prepare a small (time, pace, hr) series for charting in email.
    Returns dict with minutes + values (some may be None).
    """
    if not streams.time_s:
        return None

    n = len(streams.time_s)
    step = max(1, n // max_points)

    minutes: list[float] = []
    pace: list[Optional[float]] = []
    hr: list[Optional[int]] = []

    vel = streams.velocity_mps
    hrv = streams.heartrate_bpm

    for i in range(0, n, step):
        t = streams.time_s[i] / 60.0
        minutes.append(round(t, 3))
        if vel is not None:
            p = _pace_from_velocity(vel[i])
            pace.append(p if (p is None or 2.5 <= p <= 20.0) else None)
        else:
            pace.append(None)
        if hrv is not None:
            v = hrv[i]
            hr.append(v if v > 0 else None)
        else:
            hr.append(None)

    # If both are empty, don't bother.
    if all(x is None for x in pace) and all(x is None for x in hr):
        return None
    return {"minutes": minutes, "pace_min_per_km": pace, "hr_bpm": hr}


def openai_generate_coach_email(week_summary: dict) -> Optional[str]:
    api_key = _env("OPENAI_API_KEY")
    if not api_key:
        return None

    model = _env("OPENAI_MODEL", "gpt-4o-mini")

    athlete_profile = {
        "age_years": 26,
        "weight_kg": 90,
        "training_frequency_runs_per_week": 2,
        "strength_training_per_week": 4,
        "experience": "beginner (run/walk opbouw)",
        "environment": "stad (Hilversum centrum) met stoplichten/verkeer/hobbels; tempo (pace) is daardoor minder betrouwbaar",
        "preferences": {
            "preferred_days": ["ma/di/wo (begin week)", "weekend"],
        },
        "recent_notes_from_athlete": [
            "20 min aaneengesloten op ~6:17/km voelde moeizaam/te hard",
            "15 min aaneengesloten op 6:15–6:30/km komt vaak in HR ~170+ bpm",
            "3 min lopen / 2 min wandelen × 6 rond HR ~160 bpm voelde stabiel en ging (erg) goed",
        ],
    }

    system = (
        "Je bent een Nederlandse AI hardloopcoach. Je schrijft concreet en praktisch, als een persoonlijke coach. "
        "Je maakt altijd een schema voor 2 weken vooruit met 2 trainingen per week (4 trainingen totaal), volledig uitgeschreven. "
        "Je coacht conservatief (blessurepreventie) maar doelgericht. "
        "De loper is beginner en bouwt op via run/walk intervallen, traint daarnaast 4x per week kracht en wil primair conditie opbouwen. "
        "Context: de loper loopt vaak in de stad met stoplichten; daarom is pace minder betrouwbaar. Stuur primair op RPE/praattempo en hartslag, "
        "en gebruik pace alleen als indicatie. "
        "Doel: 10 km Singelloop Utrecht op 4 okt 2026 < 60 min (6:00/km), maar eerst veilig 10 km aaneengesloten kunnen lopen. "
        "Je baseert je analyse vooral op de intervalblokken uit runs_detailed (tempo + HR per blok) en herstel-indicatoren (HRΔ en HR↓60s), "
        "niet op de totale gemiddelden van de run. "
        "Je houdt rekening met ruis (stoplichten/pauzes): overweeg dan trends per blok i.p.v. 1 uitschieter."
    )

    # Try to infer the most recent interval pattern from detailed runs.
    inferred_baseline = None
    try:
        rd = list(week_summary.get("runs_detailed") or [])
        rd_sorted = sorted([x for x in rd if isinstance(x, dict)], key=lambda x: str(x.get("date") or ""))
        last_with_pattern = next((x for x in reversed(rd_sorted) if (x.get("pattern_estimate") or "n.v.t.") != "n.v.t."), None)
        if last_with_pattern:
            inferred_baseline = str(last_with_pattern.get("pattern_estimate") or "").strip() or None
    except Exception:
        inferred_baseline = None

    user = {
        "week_summary": week_summary,
        "runs_detailed": week_summary.get("runs_detailed"),
        "athlete_profile": athlete_profile,
        "current_baseline": "3 min lopen / 2 min wandelen × 6 rond ~160 bpm voelt stabiel en goed (laatste bekende training).",
        "inferred_recent_pattern_from_strava": inferred_baseline,
        "race": {
            "name": "Singelloop Utrecht 10 km",
            "date": "2026-10-04",
        },
        "output_structure": [
            "Korte analyse van mijn laatste training(en)",
            "Belangrijkste inzichten (bullet points)",
            "Eventuele risico’s",
            "Concreet schema voor de komende 2 weken (4 trainingen, volledig uitgeschreven)",
            "Op schema? (korte indicatie richting 10 km < 60 min)",
        ],
        "constraints": [
            "Bouw geleidelijk op (beginner, blessurepreventie).",
            "Geen algemene tips, maar specifiek advies op basis van de weekdata.",
            "Gebruik vooral de blokken (run vs walk) uit runs_detailed om advies te geven over harder/zachter lopen en interval-opbouw.",
            "Tempo-indicaties primair in RPE/praattempo; HR als guardrail; pace (min/km) alleen als ruwe indicatie (stad/stoplichten).",
            "Gebruik voor beslissingen vooral: HRΔ per loopblok, HR↓60s in wandelblokken, en stabiliteit van pace/HR over meerdere blokken.",
            "Als HR in loopblokken oploopt richting ~170+ of herstel in wandelblokken slecht is (HR↓60s klein), adviseer rustiger lopen of dezelfde interval nog 1–2 weken herhalen.",
            "Als loopblokken stabiel aanvoelen en herstel goed is (HR daalt duidelijk binnen 60–90 sec), adviseer een kleine progressie: +30–60 sec lopen per blok of -15–30 sec wandelen.",
            "Houd rekening met 4x per week sportschool: liever iets te rustig dan te agressief opbouwen.",
            "Plan bij voorkeur 1 training begin van de week (ma/di/wo) en 1 in het weekend, met minimaal 48 uur tussen zware beentraining en een run.",
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
    # The actual plan is rendered in the HTML email from week_summary["plan"].
    return "\n".join(lines)


def _render_html_email(subject: str, plain_text: str, week_summary: dict, inline_cids: dict[str, str]) -> str:
    runs = week_summary.get("runs") or []
    runs_detailed = week_summary.get("runs_detailed") or []
    build = week_summary.get("build_info") or {}
    build_label = build.get("label") or None

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

    # Block tables + charts per run (first few runs to keep email readable)
    blocks_html = []
    for rd in runs_detailed[:3]:
        blocks = rd.get("blocks") or []
        timer_blocks = rd.get("timer_blocks") or []
        chart_html = ""
        cid_pace = inline_cids.get(f"{rd.get('id')}:pace")
        cid_hr = inline_cids.get(f"{rd.get('id')}:hr")
        cid_intervals = inline_cids.get(f"{rd.get('id')}:intervals")
        if cid_pace or cid_hr or cid_intervals:
            imgs = []
            if cid_intervals:
                imgs.append(f"<div><div class='muted'>Run/Walk</div><img alt='Run/Walk' style='max-width:100%; height:auto;' src='cid:{esc(cid_intervals)}' /></div>")
            if cid_pace:
                imgs.append(f"<div><div class='muted'>Tempo</div><img alt='Tempo' style='max-width:100%; height:auto;' src='cid:{esc(cid_pace)}' /></div>")
            if cid_hr:
                imgs.append(f"<div><div class='muted'>Hartslag</div><img alt='Hartslag' style='max-width:100%; height:auto;' src='cid:{esc(cid_hr)}' /></div>")
            chart_html = "<div style='display:grid; gap:12px; grid-template-columns:1fr;'>" + "".join(imgs) + "</div>"
        else:
            chart_html = "<div class='muted' style='margin:8px 0'>Geen grafieken gegenereerd (ontbrekende streams of plotting dependency).</div>"

        header = (
            f"<div class='card'>"
            f"<h2>Intervalblokken {esc(rd.get('date'))}</h2>"
            f"<div class='meta'>{esc(rd.get('name'))} — {esc(rd.get('distance_km'))} km — patroon: "
            f"<span class='pill'>{esc(rd.get('pattern_estimate'))}</span></div>"
            f"{chart_html}"
        )
        if rd.get("streams_error"):
            blocks_html.append(header + f"<div class='muted'>Streams niet beschikbaar: {esc(rd.get('streams_error'))}</div></div>")
            continue

        timer_table = ""
        if timer_blocks:
            trows = []
            for b in timer_blocks[:18]:
                dur = int(b.get("duration_s") or 0)
                trows.append(
                    "<tr>"
                    f"<td><span class='pill'>{esc(b.get('kind'))}</span></td>"
                    f"<td class='mono'>{dur//60}:{dur%60:02d}</td>"
                    f"<td>{esc(fmt_pace(b.get('avg_pace_min_per_km')))}</td>"
                    f"<td>{esc(b.get('avg_hr') or '—')}</td>"
                    f"<td>{esc(b.get('hr_change') if b.get('hr_change') is not None else '—')}</td>"
                    f"<td>{esc(b.get('hr_drop_60s') if b.get('hr_drop_60s') is not None else '—')}</td>"
                    "</tr>"
                )
            timer_table = (
                "<div style='margin-top:10px'>"
                "<div class='muted' style='margin:6px 0'>Genormaliseerd volgens timer (minder gevoelig voor stoplichten)</div>"
                "<table>"
                "<thead><tr><th>Blok</th><th>Duur</th><th>Gem. tempo</th><th>Gem. HR</th><th>HRΔ</th><th>HR↓60s</th></tr></thead>"
                f"<tbody>{''.join(trows)}</tbody>"
                "</table>"
                "</div>"
            )

        # Hide the raw (Strava-segmented) blocks: in city running they are too noisy.
        blocks_html.append(
            header
            + timer_table
            + "</div>"
        )

    # Split out the schedule section so it stands out.
    schema_html = ""
    coach_text_only = plain_text

    plan = week_summary.get("plan") or {}
    next_2w = plan.get("next_2_weeks") or []
    roadmap = plan.get("roadmap") or []
    if next_2w:
        tr = []
        for row in next_2w:
            tr.append(
                "<tr>"
                f"<td>{esc(row.get('week'))}</td>"
                f"<td><b>{esc(row.get('session'))}</b><div class='muted'>{esc(row.get('when'))}</div></td>"
                f"<td class='mono'>{esc(row.get('workout'))}</td>"
                f"<td class='mono'>{esc(row.get('delta'))}</td>"
                f"<td class='mono'>{esc(row.get('why'))}</td>"
                "</tr>"
            )
        schema_html = (
            "<div class='card'>"
            "<h2>Schema (komende 2 weken)</h2>"
            "<table>"
            "<thead><tr><th>Week</th><th>Training</th><th>Workout</th><th>Verandering</th><th>Waarom</th></tr></thead>"
            f"<tbody>{''.join(tr)}</tbody>"
            "</table>"
            "</div>"
        )

    roadmap_html = ""
    if roadmap:
        items = []
        for it in roadmap[:12]:
            items.append(f"<li><b>{esc(it.get('week'))}</b>: {esc(it.get('focus'))} <span class='muted'>({esc(it.get('target'))})</span></li>")
        roadmap_html = (
            "<div class='card'>"
            "<h2>Roadmap t/m Singelloop</h2>"
            "<div class='muted'>Korte routekaart; we sturen elke week bij op basis van je Strava-data.</div>"
            f"<ul>{''.join(items)}</ul>"
            "</div>"
        )

    coach_text_html = (
        "<div class='card'>"
        "<h2>Coachbericht</h2>"
        f"<div class='mono'>{esc(coach_text_only)}</div>"
        "</div>"
    )

    return (
        "<!doctype html><html><head><meta charset='utf-8'>"
        f"<title>{esc(subject)}</title>"
        f"<style>{css}</style></head><body>"
        f"<div class='meta'>{esc(subject)}"
        + (f" — <span class='muted'>build {esc(build_label)}</span>" if build_label else "")
        + "</div>"
        f"{summary_table}"
        f"{''.join(blocks_html)}"
        f"{schema_html}"
        f"{roadmap_html}"
        f"{coach_text_html}"
        "</body></html>"
    )

def send_email(subject: str, body: str, week_summary: dict) -> None:
    gmail_user = _env("GMAIL_USER")
    gmail_app_password = _env("GMAIL_APP_PASSWORD")
    mail_to = _env("MAIL_TO")
    if not gmail_user or not gmail_app_password or not mail_to:
        raise SystemExit("Missing GMAIL_USER / GMAIL_APP_PASSWORD / MAIL_TO env vars.")

    # Outer container is multipart/mixed so attachments show up reliably (Apple Mail).
    # Inside it we include a multipart/related with multipart/alternative (plain + html) plus inline images.
    msg = MIMEMultipart("mixed")
    msg["From"] = gmail_user
    msg["To"] = mail_to
    msg["Subject"] = subject

    related = MIMEMultipart("related")

    alt = MIMEMultipart("alternative")
    alt.attach(MIMEText(body, "plain", "utf-8"))

    # Build inline charts (PNG) for first few runs.
    inline_cids: dict[str, str] = {}
    images: list[tuple[str, bytes]] = []
    for rd in (week_summary.get("runs_detailed") or [])[:3]:
        rid = rd.get("id")
        series = rd.get("series")
        if not rid or not series:
            continue
        pace_png = _plot_series_png(series, kind="pace")
        hr_png = _plot_series_png(series, kind="hr")
        interval_png = _plot_intervals_png(rd.get("timer_blocks") or rd.get("blocks") or [], title="Run/Walk timeline")
        if pace_png:
            cid = f"run{rid}-pace"
            inline_cids[f"{rid}:pace"] = cid
            images.append((cid, pace_png))
        if hr_png:
            cid = f"run{rid}-hr"
            inline_cids[f"{rid}:hr"] = cid
            images.append((cid, hr_png))
        if interval_png:
            cid = f"run{rid}-intervals"
            inline_cids[f"{rid}:intervals"] = cid
            images.append((cid, interval_png))

    html_body = _render_html_email(subject, body, week_summary, inline_cids)
    alt.attach(MIMEText(html_body, "html", "utf-8"))
    related.attach(alt)

    # Inline images (referenced via cid:)
    for cid, data in images:
        img = MIMEImage(data, _subtype="png")
        img.add_header("Content-ID", f"<{cid}>")
        img.add_header("Content-Disposition", "inline", filename=f"{cid}.png")
        related.attach(img)

    # Attach the related container first (so the HTML can render inline images).
    msg.attach(related)

    # Also attach the images as regular attachments (so clients show a paperclip).
    for cid, data in images:
        att = MIMEImage(data, _subtype="png")
        att.add_header("Content-Disposition", "attachment", filename=f"{cid}.png")
        msg.attach(att)

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
        server.login(gmail_user, gmail_app_password)
        server.sendmail(gmail_user, [mail_to], msg.as_string())


def write_preview(subject: str, body: str, week_summary: dict, *, out_dir: str = "preview") -> str:
    """
    Write a local HTML preview + PNG charts to disk.
    Returns the path to the HTML file.
    """
    os.makedirs(out_dir, exist_ok=True)

    runs_detailed = week_summary.get("runs_detailed") or []
    images: list[tuple[str, bytes]] = []
    for rd in runs_detailed[:3]:
        rid = rd.get("id")
        series = rd.get("series")
        if not rid or not series:
            continue
        pace_png = _plot_series_png(series, kind="pace")
        hr_png = _plot_series_png(series, kind="hr")
        interval_png = _plot_intervals_png(rd.get("timer_blocks") or rd.get("blocks") or [], title="Run/Walk timeline")
        if pace_png:
            images.append((f"run{rid}-pace.png", pace_png))
        if hr_png:
            images.append((f"run{rid}-hr.png", hr_png))
        if interval_png:
            images.append((f"run{rid}-intervals.png", interval_png))

    for name, data in images:
        with open(os.path.join(out_dir, name), "wb") as f:
            f.write(data)

    # Build a simple HTML that references local PNGs (not cid:).
    blocks = []
    for rd in runs_detailed[:3]:
        rid = rd.get("id")
        if not rid:
            continue
        imgs = []
        p = os.path.join(out_dir, f"run{rid}-pace.png")
        h = os.path.join(out_dir, f"run{rid}-hr.png")
        if os.path.exists(p):
            imgs.append(f"<div><div style='color:#666;font-size:12px'>Tempo</div><img style='max-width:100%;height:auto' src='{html.escape(os.path.basename(p))}' /></div>")
        if os.path.exists(h):
            imgs.append(f"<div><div style='color:#666;font-size:12px'>Hartslag</div><img style='max-width:100%;height:auto' src='{html.escape(os.path.basename(h))}' /></div>")
        if imgs:
            blocks.append(
                "<div style='border:1px solid #e5e5e5;border-radius:10px;padding:12px 14px;margin:12px 0'>"
                f"<div style='font-weight:600;margin-bottom:6px'>Preview grafieken {html.escape(str(rd.get('date') or ''))}</div>"
                + "<div style='display:grid;gap:12px;grid-template-columns:1fr'>"
                + "".join(imgs)
                + "</div></div>"
            )

    preview_html = (
        "<!doctype html><html><head><meta charset='utf-8'>"
        f"<title>{html.escape(subject)}</title>"
        "</head><body style='font-family:-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,Arial,sans-serif;color:#111'>"
        f"<div style='color:#555;font-size:13px;margin-bottom:12px'>{html.escape(subject)}</div>"
        + "".join(blocks)
        + "<div style='border:1px solid #e5e5e5;border-radius:10px;padding:12px 14px;margin:12px 0'>"
        "<div style='font-weight:600;margin-bottom:6px'>Coachbericht (plain text)</div>"
        f"<pre style='white-space:pre-wrap;font-family:ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,monospace;font-size:12px'>{html.escape(body)}</pre>"
        "</div>"
        "</body></html>"
    )

    html_path = os.path.join(out_dir, "preview.html")
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(preview_html)
    return html_path


def _plot_series_png(series: dict, *, kind: str) -> Optional[bytes]:
    """
    Render a lightweight PNG chart for email (pace or hr).
    Uses matplotlib if available; returns PNG bytes.
    """
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except Exception:
        plt = None  # type: ignore

    x = series.get("minutes") or []
    if not x:
        return None

    if kind == "pace":
        y = series.get("pace_min_per_km") or []
        title = "Tempo (min/km)"
        color = "#2563eb"
    elif kind == "hr":
        y = series.get("hr_bpm") or []
        title = "Hartslag (bpm)"
        color = "#dc2626"
    else:
        return None

    # Convert None to gaps
    xs: list[float] = []
    ys: list[float] = []
    for xi, yi in zip(x, y):
        if yi is None:
            xs.append(float("nan"))
            ys.append(float("nan"))
        else:
            xs.append(float(xi))
            ys.append(float(yi))

    if plt is not None:
        fig, ax = plt.subplots(figsize=(7.2, 2.2), dpi=140)
        ax.plot(xs, ys, linewidth=1.6, color=color)
        ax.set_title(title, fontsize=10, loc="left")
        ax.set_xlabel("min", fontsize=8)
        ax.grid(True, alpha=0.18)
        ax.tick_params(axis="both", labelsize=8)

        # Pace chart: invert y so faster (lower) is higher up
        if kind == "pace":
            ax.invert_yaxis()

        buf = io.BytesIO()
        fig.tight_layout()
        fig.savefig(buf, format="png")
        plt.close(fig)
        return buf.getvalue()

    # Fallback: simple plot via Pillow (keeps charts working even if matplotlib fails).
    try:
        from PIL import Image, ImageDraw
    except Exception:
        return None

    w, h = 1000, 320
    pad_l, pad_r, pad_t, pad_b = 44, 18, 30, 34
    img = Image.new("RGB", (w, h), (255, 255, 255))
    draw = ImageDraw.Draw(img)
    draw.text((pad_l, 8), title, fill=(20, 20, 20))

    vals: list[float] = [v for v in ys if v == v]  # NaN check
    if len(vals) < 2:
        return None
    y_min = min(vals)
    y_max = max(vals)
    if y_max - y_min < 1e-6:
        y_max = y_min + 1.0

    invert = kind == "pace"

    def x_to_px(xv: float) -> int:
        x0 = float(xs[0]) if xs else 0.0
        x1 = float(xs[-1]) if xs else 1.0
        if x1 - x0 < 1e-6:
            x1 = x0 + 1.0
        return int(pad_l + (w - pad_l - pad_r) * ((xv - x0) / (x1 - x0)))

    def y_to_px(yv: float) -> int:
        t = (yv - y_min) / (y_max - y_min)
        if invert:
            t = 1.0 - t
        return int(pad_t + (h - pad_t - pad_b) * t)

    for k in range(5):
        yy = pad_t + int((h - pad_t - pad_b) * (k / 4))
        draw.line([(pad_l, yy), (w - pad_r, yy)], fill=(235, 235, 235), width=1)
    draw.line([(pad_l, pad_t), (pad_l, h - pad_b)], fill=(210, 210, 210), width=1)
    draw.line([(pad_l, h - pad_b), (w - pad_r, h - pad_b)], fill=(210, 210, 210), width=1)

    last = None
    rgb = (37, 99, 235) if kind == "pace" else (220, 38, 38)
    for xv, yv in zip(xs, ys):
        if yv != yv:
            last = None
            continue
        p = (x_to_px(float(xv)), y_to_px(float(yv)))
        if last is not None:
            draw.line([last, p], fill=rgb, width=3)
        last = p

    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


def _plot_intervals_png(blocks: list[dict], *, title: str) -> Optional[bytes]:
    """
    Render a compact timeline chart (run vs walk) from block summaries.
    Uses Pillow (always available if pillow is installed).
    """
    try:
        from PIL import Image, ImageDraw
    except Exception:
        return None

    # Keep only run/walk and meaningful durations.
    parts: list[tuple[str, int]] = []
    for b in blocks:
        k = b.get("kind")
        if k not in {"run", "walk"}:
            continue
        d = int(b.get("duration_s") or 0)
        if d <= 10:
            continue
        parts.append((str(k), d))
    if not parts:
        return None

    total = sum(d for _, d in parts)
    if total <= 0:
        return None

    w, h = 1000, 170
    pad = 18
    bar_y0, bar_y1 = 62, 118
    img = Image.new("RGB", (w, h), (255, 255, 255))
    draw = ImageDraw.Draw(img)
    draw.text((pad, 10), title, fill=(20, 20, 20))

    x0 = pad
    x1 = w - pad
    cur = x0
    run_c = (37, 99, 235)
    walk_c = (16, 185, 129)

    for kind, d in parts:
        seg_w = max(1, int((x1 - x0) * (d / total)))
        color = run_c if kind == "run" else walk_c
        draw.rectangle([cur, bar_y0, min(x1, cur + seg_w), bar_y1], fill=color)
        cur += seg_w
        if cur >= x1:
            break

    # Border
    draw.rectangle([x0, bar_y0, x1, bar_y1], outline=(200, 200, 200), width=2)

    # Legend
    lx, ly = pad, 132
    draw.rectangle([lx, ly, lx + 18, ly + 10], fill=run_c)
    draw.text((lx + 24, ly - 4), "run", fill=(60, 60, 60))
    lx2 = lx + 90
    draw.rectangle([lx2, ly, lx2 + 18, ly + 10], fill=walk_c)
    draw.text((lx2 + 24, ly - 4), "walk", fill=(60, 60, 60))

    # Total duration label
    mm, ss = divmod(total, 60)
    draw.text((w - pad - 140, 10), f"totale interval: {mm}:{ss:02d}", fill=(90, 90, 90))

    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


def main() -> int:
    parser = argparse.ArgumentParser(description="Weekly Singelloop Coach")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Generate coach output and print to stdout (no email).",
    )
    parser.add_argument(
        "--preview",
        action="store_true",
        help="Write preview HTML + charts to ./preview (no email).",
    )
    parser.add_argument(
        "--force-send",
        action="store_true",
        help="Ignore Sunday 22:00 Europe/Amsterdam gate (useful for testing).",
    )
    args = parser.parse_args()

    if args.force_send:
        os.environ["FORCE_SEND"] = "true"

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
    # Add a build/version label so it's obvious whether GitHub is running the latest code.
    sha = _env("GITHUB_SHA")
    build_label = (sha[:7] if sha else None) or now_utc.strftime("%Y%m%d-%H%M")
    week_summary["build_info"] = {"label": build_label}
    week_summary["plan"] = build_plan(week_summary)
    # Add race countdown for the LLM / email.
    try:
        if ZoneInfo is not None:
            tz = ZoneInfo("Europe/Amsterdam")
            now_local = now_utc.astimezone(tz)
            race_date_local = datetime(2026, 10, 4, 0, 0, 0, tzinfo=tz)
            days = max(0, (race_date_local.date() - now_local.date()).days)
            week_summary["race_countdown"] = {"days": days, "weeks": round(days / 7.0, 1)}
    except Exception:
        pass

    # Use OpenAI for narrative coaching if available; otherwise fall back.
    body = openai_generate_coach_email(week_summary) or fallback_email(week_summary)
    subject = f"Hardloopcoach weekplan ({week_start_local.date().isoformat()}–{(week_end_local.date() - timedelta(days=1)).isoformat()}) [{build_label}]"
    if args.dry_run:
        print(subject)
        print("")
        print(body)
        return 0
    if args.preview:
        path = write_preview(subject, body, week_summary)
        print(f"Preview written: {path}")
        return 0

    send_email(subject, body, week_summary)
    print("Email sent.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

