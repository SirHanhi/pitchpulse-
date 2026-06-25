#!/usr/bin/env python3
"""
PitchPulse Elite — automated daily intelligence pipeline. (Finnish schema)

Pipeline:
  1. Odds + fixtures   : TheOddsAPI (h2h, decimal, multi-bookmaker range)
  2. Stats + injuries  : API-Football (api-sports.io direct)
  3. Noise / OSINT     : PRAW search of r/soccer + r/SoccerBetting
                         -> tagged PUBLIC_HYPE per protocol (fade unless
                            corroborated by line movement)
  4. AI synthesis      : Anthropic API, Finnish-language analyst directive,
                         strict JSON output: analyysi / voittaja /
                         tulosveikkaus / luottamus
  5. PWA generation    : writes index.html (+ sw.js), Finnish-language
                         'Intelligence Terminal' design

Install:
  pip install requests praw anthropic

Set keys as environment variables — NEVER hardcode them in this file:

  Linux/macOS:
    export ODDS_API_KEY="..."
    export APIFOOTBALL_KEY="..."
    export ANTHROPIC_API_KEY="..."
    export REDDIT_CLIENT_ID="..."          # optional
    export REDDIT_CLIENT_SECRET="..."      # optional
    export REDDIT_USER_AGENT="pitchpulse/1.0 by u/yourname"   # optional

  Windows (PowerShell):
    $env:ODDS_API_KEY="..."
    $env:APIFOOTBALL_KEY="..."
    $env:ANTHROPIC_API_KEY="..."

Optional config (env, with defaults):
  ANTHROPIC_MODEL         default: claude-sonnet-4-6
  PP_SPORT_KEY            default: soccer_fifa_world_cup
  PP_LEAGUE_ID            API-Football league id, default: 1 (World Cup)
  PP_SEASON               default: 2026
  PP_REGIONS              default: eu,uk
  PP_HOURS_AHEAD          fixture window, default: 24
  PP_OUTPUT_DIR           default: current directory
  PP_EXTRA_KEYWORDS       comma-separated additions to the noise filter

Run:
  python pitchpulse_pipeline.py            # full pipeline
  python pitchpulse_pipeline.py --sample   # render PWA with canned data,
                                           # no keys / network needed

Line movement note: opening odds are a paid add-on at TheOddsAPI, so this
script keeps its own snapshot file (odds_history.json). The first time an
event is seen, the current median is recorded as "opening"; later runs
compare against it. Run the script early and often for real movement data.
"""

from __future__ import annotations

import json
import logging
import os
import re
import sys
import unicodedata
import urllib.parse
from datetime import datetime, timedelta, timezone
from difflib import SequenceMatcher
from pathlib import Path
from statistics import median
from typing import Any

try:
    import requests
except ImportError:  # pragma: no cover
    requests = None  # sample mode works without it

try:
    import praw
except ImportError:
    praw = None

try:
    import anthropic
except ImportError:
    anthropic = None

log = logging.getLogger("pitchpulse")

# --------------------------------------------------------------------------
# Configuration — keys come from the environment ONLY.
# --------------------------------------------------------------------------

ODDS_API_KEY = os.getenv("ODDS_API_KEY", "")
APIFOOTBALL_KEY = os.getenv("APIFOOTBALL_KEY", "")
ANTHROPIC_MODEL = os.getenv("ANTHROPIC_MODEL", "claude-sonnet-4-6")

SPORT_KEY = os.getenv("PP_SPORT_KEY", "soccer_fifa_world_cup")
LEAGUE_ID = int(os.getenv("PP_LEAGUE_ID", "1"))     # API-Football: 1 = World Cup
SEASON = int(os.getenv("PP_SEASON", "2026"))
REGIONS = os.getenv("PP_REGIONS", "eu,uk")
HOURS_AHEAD = int(os.getenv("PP_HOURS_AHEAD", "24"))   # 24h window per spec
OUTPUT_DIR = Path(os.getenv("PP_OUTPUT_DIR", "."))
SNAPSHOT_FILE = OUTPUT_DIR / "odds_history.json"

ODDS_BASE = "https://api.the-odds-api.com/v4"
APIFOOTBALL_BASE = "https://v3.football.api-sports.io"

SUBREDDITS = "soccer+SoccerBetting"
NOISE_KEYWORDS = [
    "injury", "wife", "girlfriend", "party", "drama", "rumor", "leak",
]
NOISE_KEYWORDS += [
    k.strip().lower()
    for k in os.getenv("PP_EXTRA_KEYWORDS", "").split(",")
    if k.strip()
]
MAX_NOISE_PER_MATCH = 10
SNIPPET_CHARS = 280

SYSTEM_PROMPT = """You are the Lead Intelligence Officer and senior football analyst for "PitchPulse Elite". You produce sharp, authoritative, data-driven match analysis grounded strictly in the data you are given.

CRITICAL LANGUAGE RULE: Every string VALUE you output (analyysi and any other text) MUST be written in fluent, professional Finnish. No English in the output values. The field KEYS stay exactly as specified below.

INPUT: You receive a JSON array of fixtures kicking off within the next 24 hours. Each fixture may contain: multi-bookmaker h2h odds (min/median/max), opening-vs-current median line movement, bookmaker count, injuries, and a "public_hype_reddit" array.

ANALYSIS RULES (non-negotiable):
1. Ground every judgment in the payload — odds, line movement, injuries, venue. NEVER invent odds, injuries, scores, or any fact not present in the payload.
2. Items in "public_hype_reddit" are public chatter. Treat them as low-grade noise; weight them only when line movement corroborates them in the same direction.
3. "luottamus" is your CALIBRATED confidence in the "voittaja" (match-outcome / 1X2) call — NOT in the exact scoreline. Calibrate honestly: a clear favourite with corroborating data sits around 60-75; an even matchup sits near 34-42; reserve 80+ only for genuinely lopsided mismatches with strong supporting data. Do not inflate to express a certainty football does not allow.
4. "tulosveikkaus" is the single most probable exact scoreline and MUST be consistent with "voittaja" (Tasapeli -> level score e.g. "1-1"; Kotijoukkue -> home goals greater; Vierasjoukkue -> away goals greater). It is your best single estimate, not a guarantee.

OUTPUT FORMAT: Return ONLY a valid JSON array — no prose, no commentary, no markdown fences, nothing before or after the array. One object per fixture, with EXACTLY these keys:
{
  "fixture": "<copy verbatim from payload, do not translate — e.g. 'Spain vs Italy'>",
  "kickoff_utc": "<copy verbatim from payload>",
  "analyysi": "<2-4 sentences of professional Finnish reasoning citing the relevant data>",
  "voittaja": "Kotijoukkue" | "Vierasjoukkue" | "Tasapeli",
  "tulosveikkaus": "<exact score 'H-A', e.g. '2-1'>",
  "luottamus": <integer 0-100>
}"""

# --------------------------------------------------------------------------
# Small utilities
# --------------------------------------------------------------------------


def http_get(url: str, *, headers: dict | None = None, params: dict | None = None,
             retries: int = 3, timeout: int = 20) -> requests.Response:
    """GET with basic retry/backoff. Raises on final failure."""
    if requests is None:
        raise RuntimeError("The 'requests' package is required: pip install requests")
    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=timeout)
            if resp.status_code in (429, 500, 502, 503, 504):
                raise requests.HTTPError(f"HTTP {resp.status_code}", response=resp)
            resp.raise_for_status()
            return resp
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            wait = 2 ** attempt
            log.warning("GET %s failed (attempt %d/%d): %s — retrying in %ss",
                        url, attempt, retries, exc, wait)
            import time as _t
            _t.sleep(wait)
    raise RuntimeError(f"GET {url} failed after {retries} attempts: {last_exc}")


def norm_team(name: str) -> str:
    """Normalize a team name for cross-API matching."""
    s = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode()
    s = re.sub(r"[^a-z0-9 ]", "", s.lower())
    s = re.sub(r"\b(republic|fc|cf|national|team)\b", "", s).strip()
    return re.sub(r"\s+", " ", s)


def teams_match(a: str, b: str, threshold: float = 0.78) -> bool:
    na, nb = norm_team(a), norm_team(b)
    if not na or not nb:
        return False
    if na in nb or nb in na:
        return True
    return SequenceMatcher(None, na, nb).ratio() >= threshold


def pct_move(opening: float | None, current: float | None) -> float | None:
    if not opening or not current:
        return None
    return round((current - opening) / opening * 100, 1)


# --------------------------------------------------------------------------
# 1. Odds ingestion (TheOddsAPI) + line-movement snapshots
# --------------------------------------------------------------------------


def discover_sport_keys() -> None:
    """Log soccer sport keys so a bad PP_SPORT_KEY is easy to fix."""
    try:
        resp = http_get(f"{ODDS_BASE}/sports", params={"apiKey": ODDS_API_KEY})
        keys = [s["key"] for s in resp.json() if "soccer" in s.get("key", "")]
        log.info("Available soccer sport keys: %s", ", ".join(keys) or "none")
    except Exception as exc:  # noqa: BLE001
        log.warning("Could not list sports: %s", exc)


def load_snapshots() -> dict[str, Any]:
    if SNAPSHOT_FILE.exists():
        try:
            return json.loads(SNAPSHOT_FILE.read_text())
        except json.JSONDecodeError:
            log.warning("Corrupt snapshot file — starting fresh.")
    return {}


def save_snapshots(snaps: dict[str, Any]) -> None:
    SNAPSHOT_FILE.write_text(json.dumps(snaps, indent=1))


def fetch_odds() -> list[dict[str, Any]]:
    """Return matches in the window with aggregated h2h odds + movement."""
    params = {
        "apiKey": ODDS_API_KEY,
        "regions": REGIONS,
        "markets": "h2h",
        "oddsFormat": "decimal",
        "dateFormat": "iso",
    }
    try:
        resp = http_get(f"{ODDS_BASE}/sports/{SPORT_KEY}/odds", params=params)
    except RuntimeError:
        log.error("Odds fetch failed for sport key '%s'. Candidates below:", SPORT_KEY)
        discover_sport_keys()
        raise
    remaining = resp.headers.get("x-requests-remaining")
    if remaining:
        log.info("TheOddsAPI quota remaining: %s", remaining)

    now = datetime.now(timezone.utc)
    horizon = now + timedelta(hours=HOURS_AHEAD)
    snaps = load_snapshots()
    matches: list[dict[str, Any]] = []

    for ev in resp.json():
        kickoff = datetime.fromisoformat(ev["commence_time"].replace("Z", "+00:00"))
        if not (now - timedelta(hours=2) <= kickoff <= horizon):
            continue

        # Collect every bookmaker's price per outcome
        prices: dict[str, list[float]] = {}
        for book in ev.get("bookmakers", []):
            for market in book.get("markets", []):
                if market.get("key") != "h2h":
                    continue
                for outcome in market.get("outcomes", []):
                    prices.setdefault(outcome["name"], []).append(float(outcome["price"]))

        if not prices:
            continue

        snap = snaps.setdefault(ev["id"], {
            "first_seen": now.isoformat(),
            "opening": {},
        })
        odds: dict[str, dict[str, float | None]] = {}
        for name, plist in prices.items():
            cur_median = round(median(plist), 3)
            opening = snap["opening"].setdefault(name, cur_median)
            odds[name] = {
                "min": round(min(plist), 3),
                "median": cur_median,
                "max": round(max(plist), 3),
                "opening_median": opening,
                "move_pct": pct_move(opening, cur_median),
            }

        matches.append({
            "event_id": ev["id"],
            "home": ev["home_team"],
            "away": ev["away_team"],
            "kickoff_utc": kickoff.isoformat(),
            "bookmaker_count": len(ev.get("bookmakers", [])),
            "odds_h2h": odds,
        })

    save_snapshots(snaps)
    log.info("Odds: %d fixture(s) in the next %dh window.", len(matches), HOURS_AHEAD)
    return matches


# --------------------------------------------------------------------------
# 2. Fixtures + injuries (API-Football)
# --------------------------------------------------------------------------


def _af_headers() -> dict[str, str]:
    # For RapidAPI instead, swap to:
    #   {"x-rapidapi-key": KEY, "x-rapidapi-host": "api-football-v1.p.rapidapi.com"}
    return {"x-apisports-key": APIFOOTBALL_KEY}


def fetch_apifootball_context() -> tuple[list[dict], dict[str, list[dict]]]:
    """Return (fixtures, injuries_by_team) for today, league + season scoped."""
    today = datetime.now(timezone.utc).date().isoformat()
    fixtures: list[dict] = []
    injuries: dict[str, list[dict]] = {}

    try:
        resp = http_get(f"{APIFOOTBALL_BASE}/fixtures", headers=_af_headers(),
                        params={"league": LEAGUE_ID, "season": SEASON, "date": today})
        for fx in resp.json().get("response", []):
            fixtures.append({
                "home": fx["teams"]["home"]["name"],
                "away": fx["teams"]["away"]["name"],
                "round": fx.get("league", {}).get("round"),
                "venue": (fx.get("fixture", {}).get("venue") or {}).get("name"),
            })
    except Exception as exc:  # noqa: BLE001
        log.warning("API-Football fixtures unavailable (%s) — continuing without.", exc)

    try:
        resp = http_get(f"{APIFOOTBALL_BASE}/injuries", headers=_af_headers(),
                        params={"league": LEAGUE_ID, "season": SEASON, "date": today})
        for item in resp.json().get("response", []):
            team = item.get("team", {}).get("name", "Unknown")
            injuries.setdefault(team, []).append({
                "player": item.get("player", {}).get("name"),
                "reason": item.get("player", {}).get("reason"),
                "type": item.get("player", {}).get("type"),
            })
    except Exception as exc:  # noqa: BLE001
        log.warning("API-Football injuries unavailable (%s) — continuing without.", exc)

    log.info("API-Football: %d fixture(s), injuries for %d team(s).",
             len(fixtures), len(injuries))
    return fixtures, injuries


def injuries_for(team: str, injuries: dict[str, list[dict]]) -> list[dict]:
    for name, items in injuries.items():
        if teams_match(team, name):
            return items
    return []


def context_for(home: str, away: str, fixtures: list[dict]) -> dict:
    for fx in fixtures:
        if teams_match(home, fx["home"]) and teams_match(away, fx["away"]):
            return {"round": fx.get("round"), "venue": fx.get("venue")}
    return {}


# --------------------------------------------------------------------------
# 3. Reddit noise (PRAW) — tagged PUBLIC_HYPE by protocol
# --------------------------------------------------------------------------


def reddit_client() -> "praw.Reddit | None":
    cid = os.getenv("REDDIT_CLIENT_ID")
    secret = os.getenv("REDDIT_CLIENT_SECRET")
    agent = os.getenv("REDDIT_USER_AGENT", "pitchpulse-elite/1.0")
    if praw is None:
        log.warning("praw not installed — skipping noise stream.")
        return None
    if not (cid and secret):
        log.warning("Reddit credentials missing — skipping noise stream.")
        return None
    return praw.Reddit(client_id=cid, client_secret=secret, user_agent=agent)


def fetch_reddit_noise(matches: list[dict]) -> dict[str, list[dict]]:
    """Per-fixture public chatter mentioning a team + a noise keyword.

    Search runs over public threads in r/soccer and r/SoccerBetting only.
    Everything returned here is treated downstream as PUBLIC HYPE: the
    synthesis prompt fades it unless line movement corroborates.
    """
    client = reddit_client()
    if client is None:
        return {}

    now = datetime.now(timezone.utc).timestamp()
    noise: dict[str, list[dict]] = {}

    for m in matches:
        key = f"{m['home']} vs {m['away']}"
        query = f'"{m["home"]}" OR "{m["away"]}"'
        items: list[dict] = []
        try:
            results = client.subreddit(SUBREDDITS).search(
                query, sort="new", time_filter="day", limit=40)
            for post in results:
                text = f"{post.title}\n{getattr(post, 'selftext', '') or ''}"
                lowered = text.lower()
                hits = [kw for kw in NOISE_KEYWORDS if kw in lowered]
                if not hits:
                    continue
                items.append({
                    "tag": "PUBLIC_HYPE",
                    "subreddit": str(post.subreddit),
                    "title": post.title[:200],
                    "snippet": re.sub(r"\s+", " ",
                                      (post.selftext or "")[:SNIPPET_CHARS]).strip(),
                    "matched_keywords": hits,
                    "score": int(post.score),
                    "age_hours": round((now - post.created_utc) / 3600, 1),
                })
                if len(items) >= MAX_NOISE_PER_MATCH:
                    break
        except Exception as exc:  # noqa: BLE001
            log.warning("Reddit search failed for %s: %s", key, exc)
        noise[key] = items
        log.info("Noise: %s -> %d item(s).", key, len(items))
    return noise


# --------------------------------------------------------------------------
# 4. AI synthesis (Anthropic)
# --------------------------------------------------------------------------


def build_payload(matches: list[dict], fixtures: list[dict],
                  injuries: dict[str, list[dict]],
                  noise: dict[str, list[dict]]) -> list[dict]:
    payload = []
    for m in matches:
        key = f"{m['home']} vs {m['away']}"
        payload.append({
            "fixture": key,
            "kickoff_utc": m["kickoff_utc"],
            **context_for(m["home"], m["away"], fixtures),
            "bookmaker_count": m["bookmaker_count"],
            "odds_h2h": m["odds_h2h"],
            "injuries": {
                m["home"]: injuries_for(m["home"], injuries),
                m["away"]: injuries_for(m["away"], injuries),
            },
            "public_hype_reddit": noise.get(key, []),
        })
    return payload


def extract_json_array(text: str) -> list[dict]:
    cleaned = re.sub(r"```(?:json)?", "", text).strip()
    start, end = cleaned.find("["), cleaned.rfind("]")
    if start == -1 or end == -1:
        # Tolerate a single object
        obj_start, obj_end = cleaned.find("{"), cleaned.rfind("}")
        if obj_start != -1 and obj_end != -1:
            return [json.loads(cleaned[obj_start:obj_end + 1])]
        raise ValueError("No JSON array found in model output.")
    parsed = json.loads(cleaned[start:end + 1])
    if isinstance(parsed, dict):
        parsed = [parsed]
    return parsed


def synthesize(payload: list[dict]) -> list[dict]:
    if anthropic is None:
        raise RuntimeError("The 'anthropic' package is required: pip install anthropic")
    if not os.getenv("ANTHROPIC_API_KEY"):
        raise RuntimeError("ANTHROPIC_API_KEY is not set.")

    client = anthropic.Anthropic()  # reads ANTHROPIC_API_KEY from env
    user_msg = (
        "Today's slate payload follows. Apply the analysis rules strictly. "
        "Return ONLY the JSON array.\n\n" + json.dumps(payload, indent=1)
    )
    log.info("Synthesizing %d fixture(s) with %s …", len(payload), ANTHROPIC_MODEL)
    resp = client.messages.create(
        model=ANTHROPIC_MODEL,
        max_tokens=4096,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_msg}],
    )
    text = "\n".join(b.text for b in resp.content if getattr(b, "type", "") == "text")
    previews = extract_json_array(text)

    # Validate / normalize the Finnish-schema output (runs BEFORE return).
    allowed = {"Kotijoukkue", "Vierasjoukkue", "Tasapeli"}
    for p in previews:
        try:
            conf = int(p.get("luottamus", 0) or 0)
        except (TypeError, ValueError):
            conf = 0
        p["luottamus"] = max(0, min(100, conf))
        if p.get("voittaja") not in allowed:
            p["voittaja"] = "Tasapeli"
        p.setdefault("analyysi", "")
        p.setdefault("tulosveikkaus", "")
    log.info("Synthesis complete: %d preview(s).", len(previews))
    return previews


def attach_odds_display(previews: list[dict], matches: list[dict]) -> None:
    """Join Claude's previews back to our raw odds for the card footer strip."""
    for p in previews:
        fixture = p.get("fixture", "")
        for m in matches:
            key = f"{m['home']} vs {m['away']}"
            if teams_match(fixture, key) or fixture == key:
                strip = []
                for name, o in m["odds_h2h"].items():
                    label = ("1" if teams_match(name, m["home"])
                             else "2" if teams_match(name, m["away"]) else "X")
                    move = o.get("move_pct")
                    arrow = "" if move in (None, 0) else (" ↑" if move > 0 else " ↓")
                    strip.append(f"{label} {o['median']}{arrow}")
                p["odds_strip"] = " · ".join(sorted(strip))
                break


def attach_confidence_signals(previews: list[dict], matches: list[dict]) -> None:
    """Compute the honest, deterministic drivers behind each confidence call.

    Everything here is derived purely from odds data we already have — no model
    guessing, no invented "attack strength" factors. Identical inputs always
    produce identical signals. Each signal is a small dict the PWA renders as a
    bar or a directional tag; the list is easy to extend later (upset warning,
    volatility, etc.) without touching the renderer.
    """
    for p in previews:
        fixture = p.get("fixture", "")
        voittaja = p.get("voittaja")
        for m in matches:
            key = f"{m['home']} vs {m['away']}"
            if not (teams_match(fixture, key) or fixture == key):
                continue
            odds = m["odds_h2h"]

            # Identify the odds dict for the picked outcome, and gather all
            # three medians so we can strip the bookmaker margin (vig).
            picked = None
            implied = {}
            for name, o in odds.items():
                med = o.get("median")
                if med:
                    implied[name] = 1.0 / med
                is_home = teams_match(name, m["home"])
                is_away = teams_match(name, m["away"])
                is_draw = not is_home and not is_away
                if ((voittaja == "Kotijoukkue" and is_home)
                        or (voittaja == "Vierasjoukkue" and is_away)
                        or (voittaja == "Tasapeli" and is_draw)):
                    picked = (name, o)

            if not picked:
                break
            pick_name, pick_odds = picked
            signals: list[dict] = []

            # 1. Market backing — vig-adjusted implied probability of the pick.
            overround = sum(implied.values()) or 1.0
            if pick_name in implied:
                fair = implied[pick_name] / overround
                pct = round(fair * 100)
                signals.append({
                    "label": "Markkinaetu",
                    "type": "bar",
                    "value": max(0, min(100, pct)),
                    "tone": "pulse" if pct >= 55 else "ash",
                    "caption": f"Markkinan arvio: {pct} % todennäköisyys",
                })

            # 2. Line movement — directional. Shortening = money toward the
            #    pick (supportive); drifting = money against it (caution).
            move = pick_odds.get("move_pct")
            if move is None or move == 0:
                signals.append({
                    "label": "Linjaliike",
                    "type": "tag", "tone": "ash",
                    "text": "– ei liikettä",
                    "caption": "Ei avauskertoimen muutosta vielä",
                })
            elif move < 0:
                signals.append({
                    "label": "Linjaliike",
                    "type": "tag", "tone": "pulse",
                    "text": f"↓ {abs(move)} %",
                    "caption": "Kerroin kaventunut — raha valinnan suuntaan",
                })
            else:
                signals.append({
                    "label": "Linjaliike",
                    "type": "tag", "tone": "amber",
                    "text": f"↑ {move} %",
                    "caption": "Kerroin levinnyt — raha valintaa vastaan",
                })

            # 3. Bookmaker consensus — tight spread across books = agreement.
            mn, mx, md = pick_odds.get("min"), pick_odds.get("max"), pick_odds.get("median")
            if mn and mx and md:
                rel = (mx - mn) / md
                agree = round(max(0, min(100, 100 * (1 - rel / 0.30))))
                signals.append({
                    "label": "Yksimielisyys",
                    "type": "bar",
                    "value": agree,
                    "tone": "pulse" if agree >= 60 else "amber" if agree >= 35 else "ash",
                    "caption": (f"{m['bookmaker_count']} kirjaa, "
                                f"hajonta {'pieni' if agree >= 60 else 'kohtalainen' if agree >= 35 else 'suuri'}"),
                })

            p["signals"] = signals
            break


# --------------------------------------------------------------------------
# 5. PWA generation
# --------------------------------------------------------------------------

ICON_SVG = (
    "<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 64 64'>"
    "<rect width='64' height='64' rx='14' fill='%230A0E12'/>"
    "<circle cx='32' cy='32' r='10' fill='%2300E599'/></svg>"
)

SW_JS = """const CACHE = 'pitchpulse-v2';
self.addEventListener('install', e => {
  e.waitUntil(caches.open(CACHE).then(c => c.addAll(['./', './index.html'])));
  self.skipWaiting();
});
self.addEventListener('activate', e => {
  e.waitUntil(caches.keys().then(keys =>
    Promise.all(keys.filter(k => k !== CACHE).map(k => caches.delete(k)))));
  self.clients.claim();
});
self.addEventListener('fetch', e => {
  e.respondWith(
    fetch(e.request)
      .then(r => { const cp = r.clone();
        caches.open(CACHE).then(c => c.put(e.request, cp)); return r; })
      .catch(() => caches.match(e.request))
  );
});
"""

HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="fi" class="dark">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0, viewport-fit=cover">
<title>PitchPulse Elite — Päivän analyysit</title>

<!-- PWA -->
<meta name="theme-color" content="#0A0E12">
<meta name="mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">
<meta name="apple-mobile-web-app-title" content="PitchPulse">
<link rel="manifest" href="__PP_MANIFEST__">
<link rel="icon" href="data:image/svg+xml,__PP_ICON__">

<script src="https://cdn.tailwindcss.com"></script>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Archivo:wght@500;600;700;800&family=JetBrains+Mono:wght@400;500;700&display=swap" rel="stylesheet">
<script>
tailwind.config = { theme: { extend: {
  fontFamily: { display: ['Archivo','sans-serif'], mono: ['JetBrains Mono','monospace'] },
  colors: { ink:'#0A0E12', panel:'#11161C', line:'#1E2730', ash:'#5A6672',
            chalk:'#C7D0D8', pulse:'#00E599', flag:'#FF4D5E', amber:'#FFB020' },
}}};
</script>
<style>
  body { background:#0A0E12; -webkit-tap-highlight-color:transparent; }
  .grid-bg { background-image:
      linear-gradient(rgba(30,39,48,.5) 1px, transparent 1px),
      linear-gradient(90deg, rgba(30,39,48,.5) 1px, transparent 1px);
    background-size:22px 22px; }
  .blip { animation:blip 1.6s ease-in-out infinite; }
  @keyframes blip { 0%,100%{transform:scale(1);opacity:1} 50%{transform:scale(1.8);opacity:.4} }
  @media (prefers-reduced-motion: reduce){ .blip{animation:none} }
  .edge { box-shadow: inset 0 0 0 1px #1E2730; }
</style>
</head>
<body class="font-mono text-chalk min-h-screen antialiased">

<div class="max-w-md mx-auto min-h-screen flex flex-col">
  <header class="grid-bg border-b border-line px-5 pt-6 pb-5 sticky top-0 z-20 bg-ink/95 backdrop-blur">
    <div class="flex items-center gap-2.5">
      <span class="relative flex h-2.5 w-2.5">
        <span class="blip absolute inline-flex h-full w-full rounded-full bg-pulse"></span>
        <span class="relative inline-flex rounded-full h-2.5 w-2.5 bg-pulse"></span>
      </span>
      <span class="text-pulse text-[10px] tracking-[0.3em] font-bold">SEURAAVAT 24 H</span>
    </div>
    <h1 class="font-display font-extrabold text-2xl tracking-tight mt-2 leading-none">
      PITCHPULSE <span class="text-pulse">ELITE</span>
    </h1>
    <p class="text-ash text-[11px] mt-1.5 leading-snug" id="meta-line">Luotu __PP_GENERATED__</p>
    __PP_BANNER__
  </header>

  <main id="cards" class="flex-1 px-4 py-5 space-y-3"></main>

  <footer class="px-5 py-4 border-t border-line">
    <p class="text-ash text-[10px] leading-relaxed">
      Analyyttinen näkemys, ei vedonlyönti- tai sijoitusneuvontaa. Tulosveikkaus on
      todennäköisin yksittäinen lopputulos — ei takuu. Vedonlyöntiin liittyy aina riski;
      tarkista kertoimet vedonvälittäjältä ennen päätöksiä.
    </p>
  </footer>
</div>

<script id="pp-data" type="application/json">__PP_DATA__</script>
<script>
const data = JSON.parse(document.getElementById('pp-data').textContent);
const cards = document.getElementById('cards');

function esc(s){return String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));}

// Map voittaja -> badge style + the actual team name from the fixture string.
function winnerInfo(p){
  const parts = String(p.fixture||'').split(' vs ');
  if (p.voittaja === 'Kotijoukkue')
    return { label: parts[0] || 'Kotijoukkue', sub: 'KOTIVOITTO (1)', cls: 'bg-pulse/15 text-pulse' };
  if (p.voittaja === 'Vierasjoukkue')
    return { label: parts[1] || 'Vierasjoukkue', sub: 'VIERASVOITTO (2)', cls: 'bg-pulse/15 text-pulse' };
  return { label: 'Tasapeli', sub: 'TASAPELI (X)', cls: 'bg-amber/15 text-amber' };
}

// Continuous 0-100 confidence bar.
function confBar(n){
  n = Math.max(0, Math.min(100, parseInt(n)||0));
  const color = n >= 60 ? 'bg-pulse' : n >= 40 ? 'bg-amber' : 'bg-ash';
  return `<div class="w-24 h-1.5 rounded-full bg-line overflow-hidden">
            <div class="h-full ${color}" style="width:${n}%"></div>
          </div>`;
}

// Reusable confidence-signal row. Renders type:"bar" or type:"tag".
// Extend later (upset warning, volatility…) by adding signal objects — no
// renderer changes needed.
function signalRow(s){
  const toneText = { pulse:'text-pulse', amber:'text-amber', ash:'text-ash' }[s.tone] || 'text-ash';
  const toneBg   = { pulse:'bg-pulse', amber:'bg-amber', ash:'bg-ash' }[s.tone] || 'bg-ash';
  const right = s.type === 'tag'
    ? `<span class="text-[11px] font-bold ${toneText}">${esc(s.text)}</span>`
    : `<div class="w-20 h-1.5 rounded-full bg-line overflow-hidden">
         <div class="h-full ${toneBg}" style="width:${Math.max(0,Math.min(100,parseInt(s.value)||0))}%"></div>
       </div>`;
  return `<div class="flex items-center justify-between gap-3">
            <div class="min-w-0">
              <div class="text-[11px] text-chalk leading-none">${esc(s.label)}</div>
              <div class="text-[9px] text-ash mt-1 leading-snug truncate">${esc(s.caption||'')}</div>
            </div>
            <div class="shrink-0">${right}</div>
          </div>`;
}

function kickoffLocal(iso){
  try { return new Date(iso).toLocaleString('fi-FI',
    {weekday:'short', hour:'2-digit', minute:'2-digit'}); }
  catch(e){ return iso || ''; }
}

if (!data.previews || !data.previews.length){
  cards.innerHTML = `<div class="bg-panel rounded-lg edge px-5 py-10 text-center text-ash text-[11px]">
    Ei otteluita seuraavan 24 tunnin ikkunassa.</div>`;
} else {
  cards.innerHTML = data.previews.map(p => {
    const w = winnerInfo(p);
    return `
    <article class="bg-panel rounded-lg edge overflow-hidden">
      <div class="px-4 pt-3.5 pb-3 border-b border-line">
        <div class="flex items-start justify-between gap-3">
          <div>
            <div class="text-[9px] tracking-[0.2em] text-ash mb-1">${esc(kickoffLocal(p.kickoff_utc))}</div>
            <h3 class="font-display font-bold text-[15px] leading-tight">${esc(p.fixture)}</h3>
          </div>
          <div class="shrink-0 text-right">
            <span class="inline-block text-[10px] font-bold px-2 py-1 rounded ${w.cls}">${esc(w.label)}</span>
            <div class="text-[8px] tracking-[0.15em] text-ash mt-1">${esc(w.sub)}</div>
          </div>
        </div>
        ${p.odds_strip ? `<div class="text-[10px] text-ash mt-2">${esc(p.odds_strip)}</div>` : ''}
      </div>
      <div class="px-4 py-3 space-y-3">
        <div class="flex items-center justify-between bg-line/40 rounded-lg px-3 py-2.5">
          <div>
            <div class="text-[9px] tracking-[0.2em] text-ash mb-0.5">TULOSVEIKKAUS</div>
            <div class="font-display font-extrabold text-xl text-chalk leading-none">${esc(p.tulosveikkaus || '—')}</div>
          </div>
          <div class="text-right">
            <div class="text-[9px] tracking-[0.2em] text-ash mb-1">LUOTTAMUS ${esc(p.luottamus)} %</div>
            <div class="flex justify-end">${confBar(p.luottamus)}</div>
          </div>
        </div>
        ${Array.isArray(p.signals) && p.signals.length ? `
        <div>
          <div class="text-[9px] tracking-[0.2em] text-ash mb-2">MIHIN LUOTTAMUS PERUSTUU</div>
          <div class="space-y-2.5">${p.signals.map(signalRow).join('')}</div>
        </div>` : ''}
        <div>
          <div class="text-[9px] tracking-[0.2em] text-ash mb-1">ANALYYSI</div>
          <p class="text-[12px] leading-snug">${esc(p.analyysi)}</p>
        </div>
      </div>
    </article>`;
  }).join('');
}

document.getElementById('meta-line').textContent =
  `Luotu ${data.generated_at} · ${data.previews.length} ottelua`;

if ('serviceWorker' in navigator &&
    (location.protocol === 'https:' || location.hostname === 'localhost')) {
  navigator.serviceWorker.register('./sw.js').catch(() => {});
}
</script>
</body>
</html>
"""


def build_manifest_uri() -> str:
    manifest = {
        "name": "PitchPulse Elite",
        "short_name": "PitchPulse",
        "start_url": ".",
        "display": "standalone",
        "background_color": "#0A0E12",
        "theme_color": "#0A0E12",
        "icons": [{
            "src": "data:image/svg+xml," + ICON_SVG,
            "sizes": "any",
            "type": "image/svg+xml",
            "purpose": "any",
        }],
    }
    return "data:application/manifest+json," + urllib.parse.quote(json.dumps(manifest))


def render_pwa(previews: list[dict], *, sample: bool = False) -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    generated = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    data = {"generated_at": generated, "sample": sample, "previews": previews}
    data_json = json.dumps(data, ensure_ascii=False).replace("</", "<\\/")

    banner = ""
    if sample:
        banner = ('<div class="mt-2 text-[10px] text-amber tracking-[0.2em] font-bold">'
                  "ESIMERKKITILA — TESTIDATAA</div>")

    html = (HTML_TEMPLATE
            .replace("__PP_DATA__", data_json)
            .replace("__PP_GENERATED__", generated)
            .replace("__PP_MANIFEST__", build_manifest_uri())
            .replace("__PP_ICON__", urllib.parse.quote(ICON_SVG))
            .replace("__PP_BANNER__", banner))

    index = OUTPUT_DIR / "index.html"
    index.write_text(html, encoding="utf-8")
    (OUTPUT_DIR / "sw.js").write_text(SW_JS, encoding="utf-8")
    log.info("Wrote %s and sw.js", index)
    return index


# --------------------------------------------------------------------------
# Sample mode (no keys, no network — verifies the PWA end of the pipeline)
# --------------------------------------------------------------------------

SAMPLE_PREVIEWS = [
    {
        "fixture": "Sample United vs Placeholder FC",
        "kickoff_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "analyysi": "Kotijoukkueen mediaanikerroin on laskenut avauksesta, mikä viittaa "
                    "markkinan vahvistuneeseen näkemykseen. Vierasjoukkueen kokoonpanosta "
                    "puuttuu avainpelaajia. Esimerkkidataa — aja täysi putki saadaksesi "
                    "todellisen analyysin.",
        "voittaja": "Kotijoukkue",
        "tulosveikkaus": "2-1",
        "luottamus": 64,
        "odds_strip": "1 2.10 ↓ · 2 3.60 · X 3.40 ↑",
        "signals": [
            {"label": "Markkinaetu", "type": "bar", "value": 47, "tone": "ash",
             "caption": "Markkinan arvio: 47 % todennäköisyys"},
            {"label": "Linjaliike", "type": "tag", "tone": "pulse", "text": "↓ 5 %",
             "caption": "Kerroin kaventunut — raha valinnan suuntaan"},
            {"label": "Yksimielisyys", "type": "bar", "value": 72, "tone": "pulse",
             "caption": "8 kirjaa, hajonta pieni"},
        ],
    },
    {
        "fixture": "Demo City vs Testers SC",
        "kickoff_utc": (datetime.now(timezone.utc)
                        + timedelta(hours=5)).replace(microsecond=0).isoformat(),
        "analyysi": "Tasaväkinen ottelu, jossa kertoimet eivät erottele joukkueita ja "
                    "linjaliike on alle prosentin. Data ei tue selvää suosikkia "
                    "kumpaankaan suuntaan.",
        "voittaja": "Tasapeli",
        "tulosveikkaus": "1-1",
        "luottamus": 38,
        "odds_strip": "1 2.50 · 2 2.95 · X 3.10",
        "signals": [
            {"label": "Markkinaetu", "type": "bar", "value": 32, "tone": "ash",
             "caption": "Markkinan arvio: 32 % todennäköisyys"},
            {"label": "Linjaliike", "type": "tag", "tone": "ash", "text": "– ei liikettä",
             "caption": "Ei avauskertoimen muutosta vielä"},
            {"label": "Yksimielisyys", "type": "bar", "value": 41, "tone": "amber",
             "caption": "8 kirjaa, hajonta kohtalainen"},
        ],
    },
]


# --------------------------------------------------------------------------
# Orchestration
# --------------------------------------------------------------------------


def require_keys() -> None:
    missing = [name for name, val in [
        ("ODDS_API_KEY", ODDS_API_KEY),
        ("APIFOOTBALL_KEY", APIFOOTBALL_KEY),
        ("ANTHROPIC_API_KEY", os.getenv("ANTHROPIC_API_KEY", "")),
    ] if not val]
    if missing:
        raise SystemExit(f"Missing required env vars: {', '.join(missing)} "
                         "(or run with --sample).")


def main() -> int:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)-7s %(message)s")
    sample = "--sample" in sys.argv or os.getenv("PP_SAMPLE") == "1"

    if sample:
        log.info("Sample mode: rendering PWA from canned previews.")
        render_pwa(SAMPLE_PREVIEWS, sample=True)
        return 0

    require_keys()

    matches = fetch_odds()
    if not matches:
        log.info("No fixtures in window — rendering empty-state page.")
        render_pwa([])
        return 0

    fixtures, injuries = fetch_apifootball_context()
    noise = fetch_reddit_noise(matches)
    payload = build_payload(matches, fixtures, injuries, noise)

    previews = synthesize(payload)
    attach_odds_display(previews, matches)
    attach_confidence_signals(previews, matches)
    render_pwa(previews)

    log.info("Done. %d preview(s) rendered.", len(previews))
    return 0


if __name__ == "__main__":
    sys.exit(main())
