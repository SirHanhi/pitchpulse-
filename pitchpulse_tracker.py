#!/usr/bin/env python3
"""
PitchPulse Elite — results tracker & calibration grader.

What it does:
  1. LOG    : after each pipeline run, record that run's predictions to
              track_record.json (idempotent — re-logging the same fixture
              for the same kickoff just updates the stored prediction).
  2. GRADE  : look up finished matches via TheOddsAPI /scores, compare the
              final result to what was predicted, and mark each as graded.
  3. REPORT : print/write calibration stats — voittaja (1X2) hit rate,
              exact-score hit rate, and a calibration table that buckets
              predictions by stated luottamus and shows ACTUAL hit rate per
              bucket. This is the honest test of whether "65%" means 65%.

Ground truth is the scoreboard, never a self-grade. The only subjective
input is an optional human "note" per fixture (see annotate()), for logging
things the data could not see — a red card, a keeper error, a late lineup
change — so a miss becomes a labeled lesson instead of just a tally.

Usage (standalone):
  python pitchpulse_tracker.py grade      # fetch scores, grade, then report
  python pitchpulse_tracker.py report     # just print current stats
  python pitchpulse_tracker.py note "Spain vs Italy" "red card 30'"

Usage (from the pipeline, after render_pwa):
  from pitchpulse_tracker import log_predictions
  log_predictions(previews)

Env:
  ODDS_API_KEY     required for `grade` (reads TheOddsAPI /scores)
  PP_SPORT_KEY     default: soccer_fifa_world_cup
  PP_OUTPUT_DIR    default: current directory (where track_record.json lives)
"""

from __future__ import annotations

import json
import logging
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# Reuse the pipeline's own helpers so matching logic stays identical.
try:
    from pitchpulse_pipeline_fi import (
        http_get, teams_match, ODDS_BASE, SPORT_KEY, ODDS_API_KEY,
    )
except ImportError:  # allow standalone use if the module name differs
    import re
    import unicodedata
    from difflib import SequenceMatcher
    import requests

    ODDS_BASE = "https://api.the-odds-api.com/v4"
    SPORT_KEY = os.getenv("PP_SPORT_KEY", "soccer_fifa_world_cup")
    ODDS_API_KEY = os.getenv("ODDS_API_KEY", "")

    def http_get(url, *, headers=None, params=None, retries=3, timeout=20):
        resp = requests.get(url, headers=headers, params=params, timeout=timeout)
        resp.raise_for_status()
        return resp

    def norm_team(name: str) -> str:
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

log = logging.getLogger("pitchpulse.tracker")

OUTPUT_DIR = Path(os.getenv("PP_OUTPUT_DIR", "."))
RECORD_FILE = OUTPUT_DIR / "track_record.json"

# Map our Finnish 1X2 label to a canonical home/away/draw token.
VOITTAJA_TO_RESULT = {
    "Kotijoukkue": "home",
    "Vierasjoukkue": "away",
    "Tasapeli": "draw",
}


# --------------------------------------------------------------------------
# Storage
# --------------------------------------------------------------------------


def _load() -> dict[str, Any]:
    if RECORD_FILE.exists():
        try:
            return json.loads(RECORD_FILE.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            log.warning("track_record.json corrupt — starting fresh.")
    return {"entries": {}}


def _save(data: dict[str, Any]) -> None:
    RECORD_FILE.write_text(
        json.dumps(data, indent=1, ensure_ascii=False), encoding="utf-8"
    )


def _entry_key(fixture: str, kickoff_utc: str) -> str:
    """Stable id for a prediction: fixture + kickoff date."""
    day = (kickoff_utc or "")[:10]
    return f"{fixture}__{day}"


# --------------------------------------------------------------------------
# 1. LOG — called by the pipeline after synthesis
# --------------------------------------------------------------------------


def log_predictions(previews: list[dict]) -> None:
    """Record predictions. Safe to call every run; updates in place."""
    data = _load()
    entries = data["entries"]
    now = datetime.now(timezone.utc).isoformat()

    for p in previews:
        fixture = p.get("fixture", "")
        kickoff = p.get("kickoff_utc", "")
        if not fixture:
            continue
        key = _entry_key(fixture, kickoff)
        existing = entries.get(key, {})
        # Preserve grading + human note if this fixture was seen before.
        entries[key] = {
            "fixture": fixture,
            "kickoff_utc": kickoff,
            "voittaja": p.get("voittaja"),
            "tulosveikkaus": p.get("tulosveikkaus"),
            "luottamus": p.get("luottamus"),
            "logged_at": existing.get("logged_at", now),
            "updated_at": now,
            "graded": existing.get("graded", False),
            "actual_score": existing.get("actual_score"),
            "actual_result": existing.get("actual_result"),
            "voittaja_hit": existing.get("voittaja_hit"),
            "score_hit": existing.get("score_hit"),
            "note": existing.get("note", ""),
        }
    _save(data)
    log.info("Logged %d prediction(s) to %s", len(previews), RECORD_FILE.name)


# --------------------------------------------------------------------------
# 2. GRADE — fetch final scores and compare
# --------------------------------------------------------------------------


def _parse_score(scores_field: Any, home: str, away: str) -> tuple[int, int] | None:
    """TheOddsAPI /scores returns a list of {name, score} dicts."""
    if not scores_field:
        return None
    h = a = None
    for s in scores_field:
        try:
            val = int(s["score"])
        except (KeyError, ValueError, TypeError):
            return None
        if teams_match(s.get("name", ""), home):
            h = val
        elif teams_match(s.get("name", ""), away):
            a = val
    if h is None or a is None:
        return None
    return h, a


def _result_token(h: int, a: int) -> str:
    return "home" if h > a else "away" if a > h else "draw"


def grade(days_from: int = 3) -> int:
    """Grade any logged-but-ungraded fixtures that have finished.

    Returns the number of fixtures newly graded.
    """
    if not ODDS_API_KEY:
        raise SystemExit("ODDS_API_KEY not set — needed to fetch final scores.")

    data = _load()
    entries = data["entries"]
    pending = [e for e in entries.values() if not e.get("graded")]
    if not pending:
        log.info("Nothing pending to grade.")
        return 0

    resp = http_get(
        f"{ODDS_BASE}/sports/{SPORT_KEY}/scores",
        params={"apiKey": ODDS_API_KEY, "daysFrom": days_from, "dateFormat": "iso"},
    )
    score_events = resp.json()
    newly_graded = 0

    for entry in pending:
        fixture = entry["fixture"]
        parts = fixture.split(" vs ")
        if len(parts) != 2:
            continue
        home, away = parts[0].strip(), parts[1].strip()

        for ev in score_events:
            if not ev.get("completed"):
                continue
            if not (teams_match(ev.get("home_team", ""), home)
                    and teams_match(ev.get("away_team", ""), away)):
                continue
            parsed = _parse_score(ev.get("scores"), home, away)
            if parsed is None:
                continue
            h, a = parsed
            actual = _result_token(h, a)
            pred_result = VOITTAJA_TO_RESULT.get(entry.get("voittaja"))

            entry["graded"] = True
            entry["actual_score"] = f"{h}-{a}"
            entry["actual_result"] = actual
            entry["voittaja_hit"] = (pred_result == actual)
            entry["score_hit"] = (entry.get("tulosveikkaus") == f"{h}-{a}")
            newly_graded += 1
            log.info("Graded %s -> %d-%d (voittaja %s, score %s)",
                     fixture, h, a,
                     "HIT" if entry["voittaja_hit"] else "miss",
                     "HIT" if entry["score_hit"] else "miss")
            break

    _save(data)
    log.info("Newly graded: %d", newly_graded)
    return newly_graded


# --------------------------------------------------------------------------
# 3. REPORT — calibration is the real verdict
# --------------------------------------------------------------------------

# Confidence buckets for calibration. A well-calibrated model has actual
# hit rate inside each band roughly equal to the band's midpoint.
BUCKETS = [(0, 40), (40, 50), (50, 60), (60, 70), (70, 80), (80, 101)]


def build_report() -> dict[str, Any]:
    data = _load()
    graded = [e for e in data["entries"].values() if e.get("graded")]
    total_logged = len(data["entries"])

    if not graded:
        return {
            "total_logged": total_logged,
            "total_graded": 0,
            "message": "No graded fixtures yet — run `grade` after matches finish.",
        }

    n = len(graded)
    voittaja_hits = sum(1 for e in graded if e.get("voittaja_hit"))
    score_hits = sum(1 for e in graded if e.get("score_hit"))

    buckets = []
    for lo, hi in BUCKETS:
        in_band = [e for e in graded if lo <= (e.get("luottamus") or 0) < hi]
        if not in_band:
            continue
        hits = sum(1 for e in in_band if e.get("voittaja_hit"))
        buckets.append({
            "band": f"{lo}-{hi - 1 if hi <= 100 else 100}%",
            "count": len(in_band),
            "stated_mid": (lo + min(hi, 100)) / 2,
            "actual_hit_rate": round(100 * hits / len(in_band), 1),
        })

    return {
        "total_logged": total_logged,
        "total_graded": n,
        "voittaja_accuracy_pct": round(100 * voittaja_hits / n, 1),
        "exact_score_accuracy_pct": round(100 * score_hits / n, 1),
        "calibration": buckets,
        "notes": [
            {"fixture": e["fixture"], "predicted": e.get("voittaja"),
             "actual": e.get("actual_result"), "note": e["note"]}
            for e in graded if e.get("note")
        ],
    }


def print_report() -> None:
    r = build_report()
    print("\n" + "=" * 52)
    print("  PITCHPULSE ELITE — TRACK RECORD")
    print("=" * 52)
    print(f"  Logged: {r['total_logged']}   Graded: {r['total_graded']}")
    if r.get("message"):
        print(f"  {r['message']}")
        print("=" * 52 + "\n")
        return
    print(f"  Voittaja (1X2) accuracy : {r['voittaja_accuracy_pct']}%")
    print(f"  Exact-score accuracy    : {r['exact_score_accuracy_pct']}%")
    print("-" * 52)
    print("  CALIBRATION  (stated confidence vs actual hit rate)")
    print(f"  {'band':>9} {'n':>4} {'stated':>8} {'actual':>8}")
    for b in r["calibration"]:
        flag = ""
        gap = b["actual_hit_rate"] - b["stated_mid"]
        if abs(gap) >= 12:
            flag = "  <- off" if gap < 0 else "  <- under-confident"
        print(f"  {b['band']:>9} {b['count']:>4} "
              f"{b['stated_mid']:>7}% {b['actual_hit_rate']:>7}%{flag}")
    if r.get("notes"):
        print("-" * 52)
        print("  HUMAN NOTES")
        for note in r["notes"]:
            print(f"   {note['fixture']}: predicted {note['predicted']}, "
                  f"actual {note['actual']} — {note['note']}")
    print("=" * 52)
    print("  Calibration > accuracy. A high hit rate on a tiny sample is")
    print("  noise. Watch whether each band's actual matches its stated.")
    print("=" * 52 + "\n")


# --------------------------------------------------------------------------
# Optional human note
# --------------------------------------------------------------------------


def annotate(fixture_query: str, note: str) -> bool:
    """Attach a human note to the most recent matching fixture."""
    data = _load()
    matches = [
        (k, e) for k, e in data["entries"].items()
        if teams_match(fixture_query, e["fixture"]) or fixture_query in e["fixture"]
    ]
    if not matches:
        log.warning("No logged fixture matches %r", fixture_query)
        return False
    key, entry = sorted(matches, key=lambda kv: kv[1].get("kickoff_utc", ""))[-1]
    entry["note"] = note
    _save(data)
    log.info("Noted %s: %s", entry["fixture"], note)
    return True


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------


def main() -> int:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)-7s %(message)s")
    args = sys.argv[1:]
    cmd = args[0] if args else "report"

    if cmd == "grade":
        grade()
        print_report()
    elif cmd == "report":
        print_report()
    elif cmd == "note":
        if len(args) < 3:
            print('Usage: python pitchpulse_tracker.py note "Home vs Away" "your note"')
            return 1
        annotate(args[1], args[2])
    else:
        print(f"Unknown command: {cmd}")
        print("Commands: grade | report | note")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
