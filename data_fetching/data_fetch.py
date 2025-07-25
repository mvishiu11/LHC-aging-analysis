#!/usr/bin/env python3
"""
data_fetch.py – bulk digit production from CTFs (with dashboard integration)

Modified version that sends real-time updates to the system dashboard
"""
from __future__ import annotations
import argparse, concurrent.futures, datetime, pathlib, shutil, subprocess, sys
from typing import Tuple, Optional
import random
import os

from rich.console import Console
from rich.progress import (
    Progress, TextColumn, BarColumn, TimeElapsedColumn, TimeRemainingColumn)
from rich.table import Table

# Import the dashboard client
from monitor_client import DashboardClient

# Try to import uproot for integrity checking
try:
    import uproot
    UPROOT_AVAILABLE = True
except ImportError:
    UPROOT_AVAILABLE = False

# ───────────────────────── cache & integrity helpers ──────────────────────────
def check_file_integrity(file_path: pathlib.Path, det: str) -> Tuple[bool, str, dict]:
    """
    Check if a ROOT file has the expected structure and content
    Returns (is_valid, error_message, file_info)
    """
    if not UPROOT_AVAILABLE:
        # Basic check if uproot is not available
        if not file_path.exists():
            return False, "File not found", {}
        
        file_size = file_path.stat().st_size
        if file_size < 1024:  # Less than 1KB
            return False, f"File too small ({file_size} bytes)", {"size": file_size}
        
        return True, "OK (basic check only)", {"size": file_size}
    
    try:
        # Check file exists and size
        if not file_path.exists():
            return False, "File not found", {}
        
        file_size = file_path.stat().st_size
        if file_size < 1024:  # Less than 1KB
            return False, f"File too small ({file_size} bytes)", {"size": file_size}
        
        # Try to open and check structure
        with uproot.open(file_path) as file:
            # Check for required tree
            if "o2sim" not in file:
                available = list(file.keys())
                return False, f"Missing 'o2sim' tree. Available: {available}", {"keys": available}
            
            tree = file["o2sim"]
            
            # Check tree entries
            try:
                num_entries = len(tree)
                if num_entries == 0:
                    return False, "Tree has no entries", {"entries": 0}
            except:
                return False, "Cannot read tree entries", {}
            
            # Check for required branches based on detector
            det_upper = det.upper()
            required_branches = [f"{det_upper}DIGITSCH/{det_upper}DIGITSCH.QTCAmpl", 
                               f"{det_upper}DIGITSCH/{det_upper}DIGITSCH.ChId"]
            missing_branches = [branch for branch in required_branches if branch not in tree]
            
            if missing_branches:
                return False, f"Missing branches: {missing_branches}", {"missing_branches": missing_branches}
            
            # Try to read a small sample to check data integrity
            try:
                qtc_branch = f"{det_upper}DIGITSCH/{det_upper}DIGITSCH.QTCAmpl"
                ch_branch = f"{det_upper}DIGITSCH/{det_upper}DIGITSCH.ChId"
                
                qtc_sample = tree[qtc_branch].array(library="np", entry_stop=min(10, num_entries))
                ch_sample = tree[ch_branch].array(library="np", entry_stop=min(10, num_entries))
                
                # Check if we can flatten without errors
                total_qtc_entries = sum(len(event) for event in qtc_sample)
                total_ch_entries = sum(len(event) for event in ch_sample)
                
                if total_qtc_entries == 0:
                    return False, "No QTCAmpl data in sample", {"entries": num_entries, "qtc_entries": 0}
                
                return True, "OK", {
                    "entries": num_entries, 
                    "qtc_entries": total_qtc_entries,
                    "ch_entries": total_ch_entries,
                    "size": file_size
                }
                
            except Exception as e:
                return False, f"Data read error: {e}", {"entries": num_entries}
        
    except Exception as e:
        return False, f"File access error: {e}", {}

def check_cache_for_run(run_id: str, det: str, cache_dir: pathlib.Path, 
                       console: Console) -> Tuple[Optional[pathlib.Path], str]:
    """
    Check if a run exists in cache and is valid.
    Returns (cache_file_path, status_message) where cache_file_path is None if not found/invalid
    """
    if not cache_dir or not cache_dir.exists():
        return None, "Cache directory not available"
    
    # Look for cache file (case insensitive detector matching)
    det_lower = det.lower()
    cache_pattern = f"run_{run_id}_{det_lower}digits.root"
    cache_file = cache_dir / cache_pattern
    
    if not cache_file.exists():
        return None, f"Not found in cache"
    
    # Check integrity
    is_valid, error_msg, info = check_file_integrity(cache_file, det)
    if not is_valid:
        console.print(f"[yellow]Cache file for run {run_id} is invalid: {error_msg}[/]")
        return None, f"Invalid cache file: {error_msg}"
    
    return cache_file, f"Valid cache file found ({info.get('size', 0)} bytes)"

def use_cached_file(cache_file: pathlib.Path, digits_out: pathlib.Path, 
                   run_tag: str, det: str, console: Console) -> Tuple[bool, str]:
    """
    Try to use cached file via symlink, then copy, then fail.
    Returns (success, method_used)
    """
    digits_out.mkdir(exist_ok=True)
    digits_dst = digits_out / f"{run_tag}_{det.lower()}digits.root"
    
    # Remove existing file if present
    if digits_dst.exists():
        digits_dst.unlink()
    
    # Try symlink first
    try:
        os.symlink(cache_file, digits_dst)
        return True, "symlink"
    except Exception as e:
        console.print(f"[yellow]Symlink failed for {run_tag}: {e}[/]")
    
    # Try copy as fallback
    try:
        shutil.copy2(cache_file, digits_dst)
        return True, "copy"
    except Exception as e:
        console.print(f"[yellow]Copy failed for {run_tag}: {e}[/]")
        return False, f"failed: {e}"

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
                workdir: pathlib.Path, digits_out: pathlib.Path,
                cache_dir: Optional[pathlib.Path] = None,
                console: Optional[Console] = None
                ) -> Tuple[str, bool, str, str]:
    """Full pipeline for a single AliEn directory. Returns (run_id, success, log_path, method)"""
    if alien_dir.startswith("alien://"):
        alien_dir = "/" + alien_dir[8:]      # drop scheme, keep leading slash

    run_id   = pathlib.Path(alien_dir).name
    run_tag  = f"run_{run_id}"
    run_tmp  = workdir / run_tag
    run_tmp.mkdir(parents=True, exist_ok=True)

    log_file = run_tmp / f"{run_tag}.log"
    
    # Check cache first
    if cache_dir:
        cache_file, cache_status = check_cache_for_run(run_id, det, cache_dir, console or Console())
        if cache_file:
            success, method = use_cached_file(cache_file, digits_out, run_tag, det, console or Console())
            if success:
                # Write a log entry about cache usage
                log_file.write_text(f"# {datetime.datetime.utcnow():%F %T}  DET={det}\n"
                                  f"# Used cached file via {method}: {cache_file}\n"
                                  f"# Cache status: {cache_status}\n")
                return run_id, True, str(log_file), f"cache-{method}"
            else:
                # Cache file exists but couldn't be used, log and continue with normal processing
                if console:
                    console.print(f"[yellow]Cache file exists for {run_id} but couldn't be used ({method}), falling back to normal processing[/]")
    
    # Normal processing pipeline
    lst_file = run_tmp / f"{run_tag}_ctf_full.lst"
    alien_dir = alien_dir # + "/raw"

    ok, err = collect_ctfs(alien_dir, lst_file)
    if not ok:
        log_file.write_text(f"alien_find failed for {alien_dir}\n{err}\n")
        return run_id, False, str(log_file), "error"

    rc = run_workflow(det, lst_file, run_tmp, log_file)

    # move / rename digits file (if produced) immediately
    digits_src = run_tmp / f"o2_{det.lower()}digits.root"
    if rc == 0 and digits_src.exists():
        digits_out.mkdir(exist_ok=True)
        digits_dst = digits_out / f"{run_tag}_{det.lower()}digits.root"
        shutil.move(digits_src, digits_dst)

    return run_id, (rc == 0), str(log_file), "processed"

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
    ap.add_argument("--cache-dir", default=None,
                    help="Directory with cached digit files")
    ap.add_argument("--no-dashboard", action="store_true",
                    help="Disable dashboard updates")
    return ap.parse_args()

def main() -> None:
    args = parse_cli()
    console = Console()

    # Check uproot availability for integrity checking
    if not UPROOT_AVAILABLE:
        console.print("[yellow]Warning: uproot not available, using basic file integrity checks[/]")

    # Initialize dashboard client
    dashboard = None
    if not args.no_dashboard:
        try:
            dashboard = DashboardClient()
            dashboard.set_process_name(f"CTF {args.det} Digit Production")
        except Exception as e:
            console.print(f"[yellow]Warning: Could not connect to dashboard: {e}[/]")
            console.print("[yellow]Continuing without dashboard integration...[/]")

    # Setup cache directory
    cache_dir = None
    if args.cache_dir:
        cache_dir = pathlib.Path(args.cache_dir)
        if not cache_dir.exists():
            console.print(f"[yellow]Warning: Cache directory does not exist: {cache_dir}[/]")
            cache_dir = None
        else:
            console.print(f"[blue]Using cache directory: {cache_dir}[/]")

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
                 f"({len(dirs)} runs, {args.jobs} job{'s' if args.jobs>1 else ''})"
                 + (f"  [dim]cache: {cache_dir}[/]" if cache_dir else ""))

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
    results.add_column("Method", justify="center")
    results.add_column("Log")

    # Processing statistics
    ok = err = 0
    cache_hits = copy_fallbacks = symlink_successes = processed_runs = 0
    current_run = ""
    
    # Main processing loop
    with progress, concurrent.futures.ThreadPoolExecutor(max_workers=args.jobs) as pool:

        def _done_cb(fut: concurrent.futures.Future):
            nonlocal ok, err, current_run, cache_hits, copy_fallbacks, symlink_successes, processed_runs
            try:
                run, success, log, method = fut.result()
                
                # Update method statistics
                if method.startswith("cache-"):
                    cache_hits += 1
                    if method == "cache-symlink":
                        symlink_successes += 1
                    elif method == "cache-copy":
                        copy_fallbacks += 1
                elif method == "processed":
                    processed_runs += 1
                
                # Format method display
                method_display = {
                    "cache-symlink": "[green]cache→link[/]",
                    "cache-copy": "[yellow]cache→copy[/]", 
                    "processed": "[blue]processed[/]",
                    "error": "[red]error[/]"
                }.get(method, method)
                
                results.add_row(run, "[green]✓" if success else "[red]✗", method_display, log)
                
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
                fut = pool.submit(process_run, d, args.det, workdir, digits_out, cache_dir, console)
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
    
    # Cache statistics
    if cache_dir:
        console.print(f"[dim]Cache hits: {cache_hits} (symlinks: {symlink_successes}, copies: {copy_fallbacks}), processed: {processed_runs}[/]")
    
    console.print(f"[dim]All artefacts: {workdir}[/]")
    
    if dashboard:
        console.print("[dim]Dashboard updates sent successfully[/]")

# ────────────────────────────────
if __name__ == "__main__":
    main()