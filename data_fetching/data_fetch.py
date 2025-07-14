#!/usr/bin/env python3
"""
data_fetch.py  –  bulk digit production from CTFs  (fixed v3)

See bottom of file for usage & prerequisites.
"""
from __future__ import annotations
import argparse, concurrent.futures, datetime, pathlib, shutil, subprocess, sys
from typing import Tuple

from rich.console import Console
from rich.progress import (
    Progress, TextColumn, BarColumn, TimeElapsedColumn, TimeRemainingColumn)
from rich.table import Table

# ───────────────────────── low-level helpers ──────────────────────────
def collect_ctfs(alien_dir: str, out_lst: pathlib.Path) -> Tuple[bool, str]:
    """Run `alien_find DIR .root` and store absolute grid paths in *out_lst*."""
    res = subprocess.run(["alien_find", alien_dir, ".root"],
                         capture_output=True, text=True)
    if res.returncode:
        return False, res.stderr.strip()

    lines = [f"alien://{p}" for p in res.stdout.splitlines() if p]
    out_lst.write_text("\n".join(lines) + ("\n" if lines else ""))
    return True, ""

def run_workflow(det: str, lst: pathlib.Path,
                 cwd: pathlib.Path, log: pathlib.Path) -> int:
    """
    Launch `o2-ctf-reader-workflow | o2-<det>-digits-writer-workflow`
    inside *cwd*.  Returns the OS exit-code.
    """
    # inside cwd we can reference the list just by name
    lst_for_cli = lst.name if cwd.samefile(lst.parent) else lst.resolve()

    reader = (f"o2-ctf-reader-workflow --ctf-input {lst_for_cli} "
              f"--onlyDet {det} --copy-cmd no-copy --ctf-dict ccdb -b")
    writer = f"o2-{det.lower()}-digits-writer-workflow --disable-mc -b"
    cmd    = f"{reader} | {writer}"

    with log.open("w") as lf:
        lf.write(f"# {datetime.datetime.utcnow():%F %T}  DET={det}\n")
        lf.write(f"# CWD: {cwd}\n# CMD: {cmd}\n\n")
        proc = subprocess.Popen(cmd, shell=True, executable="/bin/bash",
                                cwd=cwd, stdout=lf, stderr=subprocess.STDOUT)
        proc.communicate()
        lf.write(f"\n# exit code {proc.returncode}\n")
        return proc.returncode

# ───────────────────────── one-run pipeline ──────────────────────────
def process_run(alien_dir: str, det: str,
                workdir: pathlib.Path, digits_out: pathlib.Path
                ) -> Tuple[str, bool, str]:
    """
    Full pipeline for a single AliEn directory.
    Returns (runNumber, success?, path_to_log).
    """
    if alien_dir.startswith("alien://"):
        alien_dir = "/" + alien_dir[8:]      # drop scheme, keep leading slash

    run_id   = pathlib.Path(alien_dir).name
    run_tag  = f"run_{run_id}"
    run_tmp  = workdir / run_tag
    run_tmp.mkdir(parents=True, exist_ok=True)

    lst_file = run_tmp / f"{run_tag}_ctf_full.lst"
    log_file = run_tmp / f"{run_tag}.log"

    ok, err = collect_ctfs(alien_dir, lst_file)
    if not ok:
        log_file.write_text(f"alien_find failed for {alien_dir}\n{err}\n")
        return run_id, False, str(log_file)

    rc = run_workflow(det, lst_file, run_tmp, log_file)

    # move / rename digits file (if produced) immediately
    digits_src = run_tmp / f"o2_{det.lower()}digits.root"
    if rc == 0 and digits_src.exists():
        digits_out.mkdir(exist_ok=True)
        digits_dst = digits_out / f"{run_tag}_{det.lower()}digits.root"
        shutil.move(digits_src, digits_dst)

    return run_id, (rc == 0), str(log_file)

# ───────────────────────── CLI & driver ──────────────────────────
def parse_cli() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Bulk FT0 / FV0 digit production from CTFs",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    ap.add_argument("-l", "--list", default="laser_paths.lst",
                    help="File with AliEn directories")
    ap.add_argument("-d", "--det", required=True, choices=["FT0", "FV0"],
                    help="Detector to digitise")
    ap.add_argument("-j", "--jobs", type=int, default=1,
                    help="Parallel jobs")
    ap.add_argument("--workdir", default=None,
                    help="Parent work dir (auto-timestamp if omitted)")
    return ap.parse_args()

def main() -> None:
    args = parse_cli()
    console = Console()

    dirs = [p.strip() for p in pathlib.Path(args.list).read_text().splitlines()
            if p.strip() and not p.lstrip().startswith("#")]
    if not dirs:
        console.print("[red]⨯ list file is empty – nothing to do.[/]")
        sys.exit(1)

    workdir     = pathlib.Path(args.workdir or f"work/{datetime.datetime.utcnow():%y%m%d-%H%M%S}")
    digits_out  = workdir / "digits"
    workdir.mkdir(parents=True, exist_ok=True)

    console.rule(f"[bold yellow]{args.det} digit production[/]  "
                 f"({len(dirs)} runs, {args.jobs} job{'s' if args.jobs>1 else ''})")

    progress = Progress(
        TextColumn("[bold]{task.description}"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total}"),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        console=console, transient=False)
    t_id = progress.add_task("processing", total=len(dirs))

    results = Table(title="Results", box=None)
    results.add_column("Run", justify="right")
    results.add_column("Status", justify="center")
    results.add_column("Log")

    ok = err = 0
    with progress, concurrent.futures.ThreadPoolExecutor(max_workers=args.jobs) as pool:

        def _done_cb(fut: concurrent.futures.Future):
            nonlocal ok, err
            run, success, log = fut.result()
            results.add_row(run, "[green]✓" if success else "[red]✗", log)
            ok += success
            err += (not success)
            progress.update(t_id, advance=1)

        for d in dirs:
            fut = pool.submit(process_run, d, args.det, workdir, digits_out)
            fut.add_done_callback(_done_cb)
        pool.shutdown(wait=True)

    console.rule("[green]Finished")
    console.print(results)
    console.print(f"[bold green]Success:[/] {ok}   •   [bold red]Failed:[/] {err}")
    console.print(f"[dim]All artefacts: {workdir}[/]")

# ────────────────────────────────
if __name__ == "__main__":
    main()

"""
✨  WHAT’S NEW
• absolute/relative *.lst* handling → fixes “cannot get file size” (issue #1)
• automatic rename/move of digits file to   work/<ts>/digits/
• reliable progress bar with many threads
• unchanged CLI (see `-h`)
"""
