#!/usr/bin/env python3
"""
ft0_laser_runlist.py
────────────────────
Query ALICE Bookkeeping for *good* FT0 LASER runs and write a compact
JSON list:

    [
      {"run": 564587, "start_ms": 1654105278523},
      {"run": 564588, "start_ms": 1654106358123},
      ...
    ]

No AliEn / GRID interaction, no period mapping – just the raw bookkeeping
information for further processing.

Requirements
------------
    pip install requests rich
A valid grid token is **not** needed – only a Bookkeeping JWT (BK_TOKEN
env-var).  The script falls back to the public guest token from the FIT
quick-start docs.

© 2025   Jakub Miloš   CC-BY-4.0
"""
from __future__ import annotations
import argparse, datetime, json, os, sys, urllib.parse
from pathlib import Path
from typing import List, Dict

import requests
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn

# ───────────────────────────── configuration ──────────────────────────────
API_BASE   = "https://ali-bookkeeping.cern.ch/api"
TOKEN      = os.getenv(  # guest-read token good for ~1 week
    "BK_TOKEN",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
    "eyJpZCI6Ijg5MDYzNCIsInVzZXJuYW1lIjoiamFrdWJtaWwiLCJuYW1lIjoiSmFrdWIg"
    "TWlsb3N6IE11c3p5bnNraSIsImFjY2VzcyI6Imd1ZXN0LGRlZmF1bHQtcm9sZSIsImlh"
    "dCI6MTc1MjIyNTQ5NiwiZXhwIjoxNzUyODMwMjk2LCJpc3MiOiJvMi11aSJ9."
    "NyiBr6FyIyJ20dz9rEcgVYx1rcY1oPo3gUhhW_iXuzI"
)
PAGE_LIMIT = 600                       # Bookkeeping pagination

console = Console()

# ──────────────────────────── bookkeeping helper ──────────────────────────
def fetch_laser_runs() -> List[Dict[str, int]]:
    """
    Return a list of {"run": <runNumber>, "start_ms": <epoch ms>} dicts.
    Filters:
        detector  = FT0      (Fast Interaction Trigger)
        runType   = 5        (LASER)
        runQuality= good
        lhcBeamMode = RAMP DOWN   (as done in FIT QC workflow)  …
    """
    params = {
        "filter[detectors][operator]": "and",
        "filter[detectors][values]":   "FT0",
        "filter[runTypes][]":          "5",           # LASER
        "filter[runQualities]":        "good",
        "page[limit]":                 PAGE_LIMIT,
        "page[offset]":                0,
        "token":                       TOKEN,
    }
    runs: List[Dict[str, int]] = []

    with Progress(SpinnerColumn("bouncingBall"), TextColumn("[progress.description]{task.description}"),
                  console=console) as pbar:
        task = pbar.add_task("Querying Bookkeeping …", total=None)

        while True:
            url = f"{API_BASE}/runs?{urllib.parse.urlencode(params, safe='[]')}"
            data = requests.get(url, timeout=30, verify=False).json()

            for entry in data["data"]:
                if entry.get("lhcBeamMode") != "RAMP DOWN":
                    continue
                epoch_ms = entry.get("startTime") or entry.get("timeO2Start")
                runs.append({"run": entry["runNumber"], "start_ms": epoch_ms})

            cur_page = params["page[offset]"] // PAGE_LIMIT + 1
            if cur_page >= data["meta"]["page"]["pageCount"]:
                break
            params["page[offset]"] += PAGE_LIMIT

        pbar.update(task, description=f"Fetched {len(runs)} runs ✔", completed=1)
    return runs

# ───────────────────────────────── main ────────────────────────────────────
def main() -> None:
    ap = argparse.ArgumentParser(
        description="Dump good FT0 LASER runs to JSON file.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    ap.add_argument("-o", "--outfile", default="laser_runs.json",
                    help="output file name")
    args = ap.parse_args()

    runs = fetch_laser_runs()
    Path(args.outfile).write_text(json.dumps(runs, indent=2))
    console.print(f"[bold green]✓ Saved {len(runs)} runs → {args.outfile}[/]")
    console.print(f"(earliest: {runs[0]['run']} @ "
                  f"{datetime.datetime.utcfromtimestamp(runs[0]['start_ms']/1000):%F %T} UTC)")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit("[yellow]Interrupted by user – no file written.[/]")
