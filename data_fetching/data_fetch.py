#!/usr/bin/env python3
"""
data_fetch.py – bulk digit production from CTFs (with dashboard integration)

Modified version that sends real-time updates to the system dashboard
"""
from __future__ import annotations
import argparse, concurrent.futures, datetime, pathlib, shutil, subprocess, sys
from typing import Tuple, Optional
import random

from rich.console import Console
from rich.progress import (
    Progress, TextColumn, BarColumn, TimeElapsedColumn, TimeRemainingColumn)
from rich.table import Table

# Import the dashboard client
from monitor_client import DashboardClient

# ───────────────────────── low-level helpers ──────────────────────────
def collect_ctfs(alien_dir: str, out_lst: pathlib.Path, n_files: Optional[int] = None) -> Tuple[bool, str]:
    """Run `alien_find DIR .root` and store absolute grid paths in *out_lst*."""
    res = subprocess.run(["alien_find", alien_dir, ".root"],
                         capture_output=True, text=True)
    if res.returncode:
        return False, res.stderr.strip()

    lines = [f"alien://{p}" for p in res.stdout.splitlines() if p]

    # Randomly select subset if n_files is specified
    if n_files is not None and lines:
        lines = random.sample(lines, min(n_files, len(lines)))

    out_lst.write_text("\n".join(lines) + ("\n" if lines else ""))
    return True, ""

def run_workflow(det: str, lst: pathlib.Path,
                 cwd: pathlib.Path, log: pathlib.Path) -> int:
    """Launch `o2-ctf-reader-workflow | o2-<det>-digits-writer-workflow` inside *cwd*."""
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
    """Full pipeline for a single AliEn directory."""
    if alien_dir.startswith("alien://"):
        alien_dir = "/" + alien_dir[8:]      # drop scheme, keep leading slash

    run_id   = pathlib.Path(alien_dir).name
    run_tag  = f"run_{run_id}"
    run_tmp  = workdir / run_tag
    run_tmp.mkdir(parents=True, exist_ok=True)

    lst_file = run_tmp / f"{run_tag}_ctf_full.lst"
    log_file = run_tmp / f"{run_tag}.log"
    alien_dir = alien_dir + "/raw"

    ok, err = collect_ctfs(alien_dir, lst_file, n_files=100)
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
        description="Bulk FT0 / FV0 digit production from CTFs (with dashboard)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    ap.add_argument("-l", "--list", default="laser_paths.lst",
                    help="File with AliEn directories")
    ap.add_argument("-d", "--det", required=True, choices=["FT0", "FV0"],
                    help="Detector to digitise")
    ap.add_argument("-j", "--jobs", type=int, default=1,
                    help="Parallel jobs")
    ap.add_argument("--workdir", default=None,
                    help="Parent work dir (auto-timestamp if omitted)")
    ap.add_argument("--no-dashboard", action="store_true",
                    help="Disable dashboard updates")
    return ap.parse_args()

def main() -> None:
    args = parse_cli()
    console = Console()

    # Initialize dashboard client
    dashboard = None
    if not args.no_dashboard:
        try:
            dashboard = DashboardClient()
            dashboard.set_process_name(f"CTF {args.det} Digit Production")
        except Exception as e:
            console.print(f"[yellow]Warning: Could not connect to dashboard: {e}[/]")
            console.print("[yellow]Continuing without dashboard integration...[/]")

    # Read directories list
    try:
        dirs = [p.strip() for p in pathlib.Path(args.list).read_text().splitlines()
                if p.strip() and not p.lstrip().startswith("#")]
    except FileNotFoundError:
        console.print(f"[red]⨯ List file not found: {args.list}[/]")
        if dashboard:
            dashboard.send_update("Error", 0, {'Error': f'List file not found: {args.list}'})
        sys.exit(1)
    except Exception as e:
        console.print(f"[red]⨯ Error reading list file: {e}[/]")
        if dashboard:
            dashboard.send_update("Error", 0, {'Error': f'Error reading list file: {e}'})
        sys.exit(1)

    if not dirs:
        console.print("[red]⨯ list file is empty – nothing to do.[/]")
        if dashboard:
            dashboard.send_update("Error", 0, {'Error': 'Empty list file'})
        sys.exit(1)

    # Setup work directories
    workdir = pathlib.Path(args.workdir or f"work/{datetime.datetime.utcnow():%y%m%d-%H%M%S}")
    digits_out = workdir / "digits"
    
    try:
        workdir.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        console.print(f"[red]⨯ Could not create work directory: {e}[/]")
        if dashboard:
            dashboard.send_update("Error", 0, {'Error': f'Could not create work directory: {e}'})
        sys.exit(1)

    # Notify dashboard of start
    if dashboard:
        dashboard.start_processing(len(dirs), args.det)

    console.rule(f"[bold yellow]{args.det} digit production[/]  "
                 f"({len(dirs)} runs, {args.jobs} job{'s' if args.jobs>1 else ''})")

    # Setup progress tracking
    progress = Progress(
        TextColumn("[bold]{task.description}"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total}"),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        console=console, transient=False)
    t_id = progress.add_task("processing", total=len(dirs))

    # Results table
    results = Table(title="Results", box=None)
    results.add_column("Run", justify="right")
    results.add_column("Status", justify="center")
    results.add_column("Log")

    # Processing statistics
    ok = err = 0
    current_run = ""
    
    # Main processing loop
    with progress, concurrent.futures.ThreadPoolExecutor(max_workers=args.jobs) as pool:

        def _done_cb(fut: concurrent.futures.Future):
            nonlocal ok, err, current_run
            try:
                run, success, log = fut.result()
                results.add_row(run, "[green]✓" if success else "[red]✗", log)
                
                if success:
                    ok += 1
                    if dashboard:
                        dashboard.report_success(f"run_{run}")
                else:
                    err += 1
                    if dashboard:
                        dashboard.report_error(f"run_{run}", "Processing failed")
                
                current_run = f"run_{run}"
                
                # Update dashboard with progress
                if dashboard:
                    dashboard.update_progress(ok, len(dirs), current_run, err, args.det)
                
                # Update console progress
                progress.update(t_id, advance=1)
                
            except Exception as e:
                console.print(f"[red]Error in callback: {e}[/]")
                err += 1
                progress.update(t_id, advance=1)

        # Submit all jobs
        console.print(f"[blue]Submitting {len(dirs)} jobs to {args.jobs} workers...[/]")
        
        for i, d in enumerate(dirs):
            try:
                fut = pool.submit(process_run, d, args.det, workdir, digits_out)
                fut.add_done_callback(_done_cb)
            except Exception as e:
                console.print(f"[red]Error submitting job {i+1}: {e}[/]")
                err += 1
                progress.update(t_id, advance=1)
        
        # Wait for all jobs to complete
        pool.shutdown(wait=True)

    # Final dashboard update
    if dashboard:
        dashboard.finish_processing(ok, err, args.det)

    # Display final results
    console.rule("[green]Finished")
    console.print(results)
    console.print(f"[bold green]Success:[/] {ok}   •   [bold red]Failed:[/] {err}")
    console.print(f"[dim]All artefacts: {workdir}[/]")
    
    if dashboard:
        console.print("[dim]Dashboard updates sent successfully[/]")

# ────────────────────────────────
if __name__ == "__main__":
    main()