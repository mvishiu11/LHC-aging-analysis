#!/usr/bin/env python3
"""
laser_path_mapper_cached.py
---------------------------
Collects *good* FT0 **LASER** runs from ALICE Bookkeeping, maps each run to its
AliEn directory `alien://alice/data/<year>/<LHCperiod>_FT0/<run>`, and stores
all paths in **laser_paths.lst**.

Key features
============
* **Per-year caches**  
  • `period_list_<year>.json` – list of FT0 periods in `/alice/data/<year>`  
  • `period_cache_<year>.json` – run → period mapping (grows on demand)

* **Whole-run cache**  
  • Already-mapped run numbers are read from *laser_paths.lst* and skipped.

* **Immediate append**  
  • New paths are appended to the list as soon as they are found – safe to
    interrupt and resume.

* **Colourful progress** via *rich* (spinner, green ✓ / red ✗ lines).

Requirements
============
`pip install rich tqdm requests` and a valid Grid proxy (`alien_find`
available in $PATH).

Set **BK_TOKEN** in the environment if you need a different Bookkeeping token.

---------------------------------------------------------------------------
"""

from __future__ import annotations

import datetime
import json
import os
import pathlib
import re
import subprocess
import sys
import urllib.parse
from typing import Dict, List, Set, Tuple

import requests
import urllib3
from rich.console import Console
from rich.spinner import Spinner
from tqdm import tqdm

# ----------------------------------------------------------------------------
# Configuration constants
# ----------------------------------------------------------------------------
TOKEN = os.getenv(
    "ALICE_BK_TOKEN",
    "",
)
OUTDIR = pathlib.Path(".").absolute()
LISTFILE = OUTDIR / "laser_paths.lst"
PAGE_LIMIT = 600

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

PERIOD_RE_FT0 = re.compile(r"LHC\d{2}[a-z]{1,2}_FT0")
PERIOD_RE = re.compile(r"LHC\d{2}[a-z]{1,2}.*")
RUN_RE = re.compile(r"run(\d{6,9})")

console = Console(highlight=False)


# ----------------------------------------------------------------------------
# Helpers – Bookkeeping query
# ----------------------------------------------------------------------------
def bk_runs() -> List[Tuple[int, int]]:
    """Return [(runNumber, year), …] for good FT0 LASER runs."""
    url = "https://ali-bookkeeping.cern.ch/api/runs"
    params = {
        "filter[detectors][operator]": "and",
        "filter[detectors][values]": "FT0",
        "filter[runTypes][]": "1",  # 5 LASER, 1 PHYSICS
        "filter[runQualities]": "good",
        "page[limit]": PAGE_LIMIT,
        "page[offset]": 0,
        "token": TOKEN,
    }

    out: List[Tuple[int, int]] = []
    while True:
        q = urllib.parse.urlencode(params, safe="[]")
        data = requests.get(f"{url}?{q}", verify=False, timeout=30).json()

        for entry in data["data"]:
            # if entry.get("lhcBeamMode") != "RAMP DOWN":
            #     continue
            if entry.get("pdpBeamType") not in ["OO", "pO"]:
                continue
            epoch_ms = entry.get("startTime") or entry.get("timeO2Start")
            year = datetime.datetime.utcfromtimestamp(epoch_ms / 1000).year
            out.append((entry["runNumber"], year))

        if params["page[offset]"] // PAGE_LIMIT + 1 >= data["meta"]["page"]["pageCount"]:
            break
        params["page[offset]"] += PAGE_LIMIT
    return out


# ----------------------------------------------------------------------------
# Helpers – per-year AliEn period caches
# ----------------------------------------------------------------------------
def period_list(year: int) -> List[str]:
    cache = OUTDIR / f"period_list_{year}.json"
    if cache.exists():
        return json.loads(cache.read_text())

    res = subprocess.run(
        ["alien.py", "ls", f"/alice/data/{year}"],
        capture_output=True,
        text=True,
        check=True,
    )
    periods = [p.strip("/") for p in res.stdout.splitlines() if PERIOD_RE.fullmatch(p.strip("/"))]
    cache.write_text(json.dumps(periods))
    return periods


def _mapping_path(year: int) -> pathlib.Path:
    return OUTDIR / f"period_cache_{year}.json"


def load_mapping(year: int) -> Dict[str, str]:
    path = _mapping_path(year)
    return json.loads(path.read_text()) if path.exists() else {}


def save_mapping(year: int, mp: Dict[str, str]):
    _mapping_path(year).write_text(json.dumps(mp, indent=2))


# ----------------------------------------------------------------------------
# Whole-run cache (laser_paths.lst)
# ----------------------------------------------------------------------------
def load_done_runs() -> Set[int]:
    done: Set[int] = set()
    if LISTFILE.exists():
        for line in LISTFILE.read_text().splitlines():
            m = RUN_RE.search(line)
            if m:
                done.add(int(m.group(1)))
    return done


def append_path(path: str):
    LISTFILE.parent.mkdir(parents=True, exist_ok=True)
    with LISTFILE.open("a", encoding="utf-8") as f:
        f.write(path + "\n")


# ----------------------------------------------------------------------------
# Core mapping routine
# ----------------------------------------------------------------------------
def find_period(year: int, run: int) -> str | None:
    """Return period name for given run or None (updates cache)."""
    mapping = load_mapping(year)
    if str(run) in mapping:
        return mapping[str(run)]

    for period in period_list(year):
        # NOTE: *no* zero-padding here – AliEn dirs use plain run numbers
        test_dir = f"/alice/data/{year}/{period}/{run}"
        cp = subprocess.run(["alien_find", test_dir, ""], capture_output=True)
        if cp.returncode == 0:
            mapping[str(run)] = period
            save_mapping(year, mapping)
            return period
    return None


# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------
def main():
    runs = bk_runs()
    console.print(f"[bold]Bookkeeping returned {len(runs)} candidate runs.[/]\n")

    done = load_done_runs()
    if done:
        console.print(f"[green]→ {len(done)} runs already in {LISTFILE.name} – skipped.[/]\n")

    mapped = missing = 0
    for run, year in tqdm(runs, desc="mapping", unit="run"):
        if run in done:
            continue

        spinner = Spinner("dots", text=f"[cyan]Searching {run} in {year}…")
        with console.status(spinner):
            period = find_period(year, run)

        if period:
            path = f"alien://alice/data/{year}/{period}/{run}"
            append_path(path)
            mapped += 1
            console.print(f"  [green]✓ {path}")
        else:
            missing += 1
            console.print(f"  [red]✗ run {run} not found in /alice/data/{year}")

    console.print(
        f"\n[bold green]✓ Added {mapped} new paths.[/] "
        f"[bold]Total lines:[/] {len(done) + mapped}"
    )
    if missing:
        console.print(f"[bold red]✗ {missing} runs still missing.[/]")


# ----------------------------------------------------------------------------
if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit("[bold yellow]Interrupted by user.[/]")
