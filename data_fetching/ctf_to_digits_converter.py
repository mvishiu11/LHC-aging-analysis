#!/usr/bin/env python3
"""
ctf_to_digits_convert.py
-----------------------
Convert all **FT0 CTF ROOT** files in a given directory (downloaded by
`laser_fetch_download.py`) into decoded FT0 **digits ROOT** files using the O2
entropy decoder workflows.

Usage
-----
```bash
python3 ctf_to_digits_convert.py \
        --ctf-dir   ~/Desktop/CERN/LHC-aging-analysis/data/laserCTF \
        --out-dir   ~/Desktop/CERN/LHC-aging-analysis/data/laserDigits \
        --workers   4
```

•   Only CTF files that **have no matching decoded file** in `--out-dir` are
    processed.
•   The converter calls the official O2 chain:
    * `o2-ctf-reader-workflow --inFile <ctf> --detector FT0`
    * `o2-ft0-entropy-decoder-workflow`
    * `o2-ft0-digi-writer --output <out>/digits_<run>.root`
•   Progress is shown with `tqdm`.
•   A JSON ledger `digits_convert_ledger.json` is written next to `--out-dir`.

Prerequisites
-------------
* An **O2 runtime environment** (`alienv enter O2/latest`, or a matching CVMFS
  stack) so the three workflows are on $PATH.
* Python deps: `pip install tqdm psutil humanfriendly` (psutil used for RSS
  guard similar to the downloader).
"""

from __future__ import annotations

import argparse, concurrent.futures as cf, json, os, re, subprocess, time, pathlib, psutil
from typing import List
from tqdm import tqdm
import humanfriendly as hf

RSS_CAP_GB = 40          # pause if converter RAM > 40 GB

RUN_RE = re.compile(r"run(\d{6,9})")          # extract run from file name

# ----------------------------------------------------------------------------------
# helpers
# ----------------------------------------------------------------------------------

def find_ctf_files(ctf_dir: pathlib.Path) -> List[pathlib.Path]:
    return sorted(ctf_dir.glob("*.root"))


def expected_out(ctf: pathlib.Path, out_dir: pathlib.Path) -> pathlib.Path:
    run_m = RUN_RE.search(ctf.name)
    run_id = run_m.group(1) if run_m else "unknown"
    return out_dir / f"digits_{run_id}.root"


def already_done(ctf_dir: pathlib.Path, out_dir: pathlib.Path):
    done_runs = {RUN_RE.search(p.name).group(1)
                 for p in out_dir.glob("digits_*.root")
                 if RUN_RE.search(p.name)}
    for ctf in find_ctf_files(ctf_dir):
        run_m = RUN_RE.search(ctf.name)
        if not run_m:
            continue
        if run_m.group(1) not in done_runs:
            yield ctf


def run_decoder(ctf: pathlib.Path, out_root: pathlib.Path):
    """Call the O2 chain and produce digits_*.root."""
    cmd = (
        f"o2-ctf-reader-workflow --ctf-input {ctf} --copy-cmd no-copy --ctf-dict ccdb --onlyDet FT0 --severity=error -b | "
        f"o2-ft0-digits-writer-workflow --disable-mc -b"
    )
    t0 = time.time()
    proc = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    dt  = time.time() - t0
    return dict(ctf=str(ctf), out=str(out_root), rc=proc.returncode,
                seconds=round(dt, 1), stdout=proc.stdout[-200:],
                stderr=proc.stderr[-200:])

# ----------------------------------------------------------------------------------
# main
# ----------------------------------------------------------------------------------

def main(argv: List[str] | None = None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--ctf-dir", required=True, type=pathlib.Path)
    ap.add_argument("--out-dir", required=True, type=pathlib.Path)
    ap.add_argument("--workers", type=int, default=os.cpu_count()//2,
                    help="parallel decoder processes (default: half cores)")
    args = ap.parse_args(argv)

    args.out_dir.mkdir(parents=True, exist_ok=True)
    ledger_path = args.out_dir / "digits_convert_ledger.json"
    ledger = json.loads(ledger_path.read_text()) if ledger_path.exists() else []

    to_do = list(already_done(args.ctf_dir, args.out_dir))
    if not to_do:
        print("All CTFs already converted ✨")
        return

    rss_proc = psutil.Process()
    bar = tqdm(total=len(to_do), unit="file")

    def task(ctf_path: pathlib.Path):
        """Wrapper that enforces RSS limit."""
        while rss_proc.memory_info().rss / 1e9 > RSS_CAP_GB:
            time.sleep(5)
        out_root = expected_out(ctf_path, args.out_dir)
        res = run_decoder(ctf_path, out_root)
        ledger.append(res)
        ledger_path.write_text(json.dumps(ledger, indent=2))
        bar.update()
        bar.set_postfix(rss=f"{rss_proc.memory_info().rss/1e9:4.1f} GB")
        return res

    with cf.ThreadPoolExecutor(max_workers=args.workers) as ex:
        list(ex.map(task, to_do))

    bar.close()
    total_bytes = sum(pathlib.Path(e['out']).stat().st_size for e in ledger if e.get('rc')==0 and pathlib.Path(e['out']).exists())
    print(f"\nDone. {len(to_do)} CTFs converted. Output size {hf.format_size(total_bytes)}.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Interrupted by user.")
