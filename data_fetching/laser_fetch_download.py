#!/usr/bin/env python3
"""
laser_fetch_download.py

Download ALICE **FT0 LASER** CTF ROOT files and store them locally.

Changes in this revision
------------------------
* We now obtain **both the run number *and* its calendar year directly from Bookkeeping** (derive the year from `startTime` ‑ a UNIX epoch in ms).
* `lfn_list(year, run)` therefore gets the correct year without guessing.
* Wild‑card handling simplified: `alien.py find /alice/data/<YEAR> "*runXXXXXXXXX*.root"` recurses through every period directory.
* Fixed the call site in the main loop (`lfn_list(year, run)`).

Prerequisites
-------------
* Valid grid proxy (`voms-proxy-init -voms alice`).
* xjalienfs client (`alien.py`) in $PATH.
* Python deps: `pip install requests psutil tqdm humanfriendly`.
* Paste your JWT token into `TOKEN` below or export it as `BK_TOKEN`.
"""

from __future__ import annotations

import os, sys, json, time, pathlib, subprocess, datetime, urllib.parse, urllib3, requests, psutil, re
from typing import Iterator, Tuple, List, Set
from tqdm import tqdm

# ------------------------------------------------- CONFIG -------------------------------------------------
TOKEN         = os.getenv("BK_TOKEN", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6Ijg5MDYzNCIsInVzZXJuYW1lIjoiamFrdWJtaWwiLCJuYW1lIjoiSmFrdWIgTWlsb3N6IE11c3p5bnNraSIsImFjY2VzcyI6Imd1ZXN0LGRlZmF1bHQtcm9sZSIsImlhdCI6MTc1MjIyNTQ5NiwiZXhwIjoxNzUyODMwMjk2LCJpc3MiOiJvMi11aSJ9.NyiBr6FyIyJ20dz9rEcgVYx1rcY1oPo3gUhhW_iXuzI")
OUTDIR        = pathlib.Path("~/Desktop/CERN/LHC-aging-analysis/data/laserCTF").expanduser()
PAGE_LIMIT    = 600          # Bookkeeping pagination size
RSS_CAP_GB    = 40           # Pause when RSS exceeds this
ALIEN_STREAMS = 8            # Parallel TCP streams per file
LEDGER_FILE   = OUTDIR / "laser_fetch_ledger.json"
# ----------------------------------------------------------------------------------------------------------

# Disable SSL warnings – CERN internal CA chain not in aliBuild Python
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

if not TOKEN or TOKEN.startswith("<PASTE"):
    sys.exit("❌  Please set your Bookkeeping token via BK_TOKEN or edit TOKEN in the script.")


# ----- helper --------------------------------------------------------------------------------------------

_RUN_RE = re.compile(r"run(\d{8,9})")

def already_downloaded_runs() -> Set[int]:
    """Return a set of runNumbers that already have ≥1 CTF file on disk."""
    runs: set[int] = set()
    for f in OUTDIR.glob("*.root"):
        m = _RUN_RE.search(f.name)
        if m:
            runs.add(int(m.group(1)))
    return runs

# ----------------------------------------------------------------------------------------------------------
# 1.  BOOKKEEPING  →  (run, year) generator
# ----------------------------------------------------------------------------------------------------------

def bk_runs() -> Iterator[Tuple[int, int]]:
    """Yield tuples **(runNumber, calendarYear)** for LASER runs with good quality."""
    base_url = "https://ali-bookkeeping.cern.ch/api/runs"
    params = {
        "filter[detectors][operator]": "and",
        "filter[detectors][values]"  : "FT0",
        "filter[runTypes][]"         : "5",      # LASER
        "filter[runQualities]"       : "good",
        "page[limit]"                : PAGE_LIMIT,
        "page[offset]"               : 0,
        "token"                      : TOKEN,
    }

    while True:
        url = f"{base_url}?{urllib.parse.urlencode(params, safe='[]')}"
        resp = requests.get(url, verify=False, timeout=60)
        resp.raise_for_status()
        payload = resp.json()

        for entry in payload["data"]:
            if entry.get("lhcBeamMode") != "RAMP DOWN":
                continue 
            
            run   = entry["runNumber"]
            # Prefer startTime (epoch ms); fall back to timeO2Start
            epoch_ms = entry.get("startTime") or entry.get("timeO2Start")
            year     = datetime.datetime.utcfromtimestamp(epoch_ms / 1000).year
            yield run, year

        cur_page = params["page[offset]"] // PAGE_LIMIT + 1
        if cur_page >= payload["meta"]["page"]["pageCount"]:
            break
        params["page[offset]"] += PAGE_LIMIT

# ----------------------------------------------------------------------------------------------------------
# 2.  GRID helper — list files for a run/year
# ----------------------------------------------------------------------------------------------------------

def lfn_list(year: int, run: int) -> List[str]:
    """Return every ROOT LFN that contains the zero‑padded run number inside the given year."""
    year_dir = f"/alice/data/{year}"
    pattern  = f"*run{run:08d}*.root"   # recursive search key

    try:
        res = subprocess.run(
            ["alien.py", "find", year_dir, pattern],
            capture_output=True, text=True, check=True
        )
        return [lfn for lfn in res.stdout.splitlines() if lfn.strip()]
    except subprocess.CalledProcessError as e:
        tqdm.write(f"⚠️  alien.py find failed for run {run}: {e.stderr.strip()}")
        return []

# ----------------------------------------------------------------------------------------------------------
# 3.  Low‑level copy & ledger helpers
# ----------------------------------------------------------------------------------------------------------

def alien_cp(lfn: str, dest: pathlib.Path):
    dest.parent.mkdir(parents=True, exist_ok=True)
    subprocess.check_call(["alien.py", "cp", f"-T{ALIEN_STREAMS}", lfn, f"file:{dest}"])

def append_ledger(entry: dict):
    existing = json.loads(LEDGER_FILE.read_text()) if LEDGER_FILE.exists() else []
    existing.append(entry)
    LEDGER_FILE.write_text(json.dumps(existing, indent=2))

# ----------------------------------------------------------------------------------------------------------
# 4.  Main orchestrator
# ----------------------------------------------------------------------------------------------------------

def main():
    print("➡  Querying Bookkeeping for LASER runs …", flush=True)
    runs = list(bk_runs())               # list of (run, year)
    print(f"   Retrieved {len(runs)} runs.\n")
    already = already_downloaded_runs()
    runs = [ry for ry in runs if ry[0] not in already]  # skip whole run if any file already present
    print(f"   After pruning, {len(runs)} runs remain to download.\n")

    proc = psutil.Process()
    bar  = tqdm(runs, unit="run")

    for run, year in bar:
        # Memory guard
        while proc.memory_info().rss / 1e9 > RSS_CAP_GB:
            bar.write("🚦  RSS > cap, pausing …")
            time.sleep(5)

        lfns = lfn_list(year, run)
        if not lfns:
            bar.write(f"❌  Run {run}: no files in {year}")
            continue

        bar.write(f"📂  Run {run} ({year}) → {len(lfns)} files")

        for lfn in lfns:
            dest = OUTDIR / os.path.basename(lfn)
            if dest.exists():
                continue  # already fetched
            t0 = time.time()
            try:
                alien_cp(lfn, dest)
                append_ledger({
                    "run": run,
                    "year": year,
                    "lfn": lfn,
                    "bytes": dest.stat().st_size,
                    "seconds": round(time.time() - t0, 2),
                    "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                })
            except subprocess.CalledProcessError as exc:
                append_ledger({
                    "run": run,
                    "year": year,
                    "lfn": lfn,
                    "error": exc.returncode,
                    "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                })
        bar.set_postfix(rss=f"{proc.memory_info().rss / 1e9:4.1f} GB")

    print("\n✅  Finished. Ledger at", LEDGER_FILE)

# ----------------------------------------------------------------------------------------------------------

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted by user.")
