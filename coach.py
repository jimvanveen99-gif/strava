import json
import os
import smtplib
import ssl
import sys
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

try:
    from zoneinfo import ZoneInfo  # py>=3.9
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore


STRAVA_TOKEN_URL = "https://www.strava.com/oauth/token"
STRAVA_ACTIVITIES_URL = "https://www.strava.com/api/v3/athlete/activities"
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


def openai_generate_coach_email(week_summary: dict) -> str | None:
    api_key = _env("OPENAI_API_KEY")
    if not api_key:
        return None

    model = _env("OPENAI_MODEL", "gpt-4o-mini")

    system = (
        "Je bent een Nederlandse AI hardloopcoach. Je schrijft concreet en praktisch, als een persoonlijke coach. "
        "Je maakt altijd een schema voor 1 week vooruit met 2 trainingen. "
        "De loper is beginner, traint 2x per week, komt van run/walk intervallen en traint daarnaast 4x per week kracht. "
        "Doel: 10 km Singelloop Utrecht op 4 okt 2026 < 60 min (6:00/km), maar eerst veilig 10 km aaneengesloten kunnen lopen."
    )

    user = {
        "week_summary": week_summary,
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
            "Tempo-indicaties in RPE/praattempo en (als mogelijk) min/km richting 6:00, maar niet forceren.",
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
    except Exception as e:  # pragma: no cover
        raise RuntimeError(f"OpenAI request failed: {e}")

    try:
        return str(data["choices"][0]["message"]["content"]).strip()
    except Exception:  # pragma: no cover
        raise RuntimeError(f"Unexpected OpenAI response: {data}")


def fallback_email(week_summary: dict) -> str:
    runs = week_summary.get("runs", [])
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


def send_email(subject: str, body: str) -> None:
    gmail_user = _env("GMAIL_USER")
    gmail_app_password = _env("GMAIL_APP_PASSWORD")
    mail_to = _env("MAIL_TO")
    if not gmail_user or not gmail_app_password or not mail_to:
        raise SystemExit("Missing GMAIL_USER / GMAIL_APP_PASSWORD / MAIL_TO env vars.")

    msg = (
        f"From: {gmail_user}\r\n"
        f"To: {mail_to}\r\n"
        f"Subject: {subject}\r\n"
        "MIME-Version: 1.0\r\n"
        "Content-Type: text/plain; charset=utf-8\r\n"
        "\r\n"
        f"{body}\r\n"
    )

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
        server.login(gmail_user, gmail_app_password)
        server.sendmail(gmail_user, [mail_to], msg.encode("utf-8"))


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

    body = openai_generate_coach_email(week_summary) or fallback_email(week_summary)
    subject = f"Hardloopcoach weekplan ({week_start_local.date().isoformat()}–{(week_end_local.date() - timedelta(days=1)).isoformat()})"
    send_email(subject, body)
    print("Email sent.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

