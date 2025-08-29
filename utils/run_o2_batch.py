#!/usr/bin/env python3
# run_o2_batch.py — iterate an index and execute a command per input (.lst OR single .root URL)
# Ubuntu 24.04 ready — now with a dash of tasteful flair 🤘
#
# Example:
#   ./run_o2_batch.py runs.index \
#     --cmd 'o2-ctf-reader-workflow --ctf-input {lst} --ctf-dict ccdb --onlyDet FT0 --severity=error -b | \
#            o2-qc --config json://$HOME/alice/QualityControl/Modules/FIT/FT0/etc/ft0-adc-mip-trending.json -b' \
#     --timeout 120 --grace 20 --kill-after 10 --outdir logs
#
# Placeholders in --cmd:
#   {lst}  -> path to .lst OR a single .root URL/path (works unchanged)
#   {run}  -> run id (e.g., 564222)
#
# Signals: sends SIGINT to the whole pipeline on timeout, then TERM → KILL if needed.

import argparse
import os
import random
import shlex
import signal
import subprocess
import sys
import time
from pathlib import Path

REMOTE_PREFIXES = ("alien://", "root://")

# ---------- UI / Flair ----------

class UI:
    def __init__(self, plain: bool = False, no_emoji: bool = False):
        self.is_tty = sys.stdout.isatty()
        self.plain = plain or not self.is_tty
        self.no_emoji = no_emoji or self.plain
        # colors
        if self.plain:
            self.RESET = self.DIM = self.BOLD = self.OK = self.WARN = self.ERR = ""
        else:
            self.RESET = "\033[0m"
            self.DIM   = "\033[2m"
            self.BOLD  = "\033[1m"
            self.OK    = "\033[32m"
            self.WARN  = "\033[33m"
            self.ERR   = "\033[31m"

        self._spinner = "|/-\\"
        self._sp_i = 0

    def emoji(self, name, default=""):
        if self.no_emoji:
            return default
        return {
            "Run": "▶️",
            "OK": "✅",
            "TO": "⏰",
            "FAIL": "❌",
            "SKIP": "⏭️",
            "DRY": "📝",
            "Zap": "⚡",
            "Skull": "💀",
        }.get(name, default)

    def spin(self):
        ch = self._spinner[self._sp_i]
        self._sp_i = (self._sp_i + 1) % len(self._spinner)
        return ch

    def banner(self, total, timeout, grace, kill_after):
        if self.plain:
            print(f"[INFO] O2 Batch Runner — {total} jobs | timeout={timeout}s grace={grace}s kill_after={kill_after}s")
            return
        art = r"""
   ___  ____        __        __        __      __         __
  / _ )/ __ )____ _/ /_____ _/ /_____ _/ /___  / /__  ____/ /
 / _  / __  / __ `/ __/ __ `/ __/ __ `/ / __ \/ / _ \/ __  / 
/____/____/_/\__,_/\__/\__,_/\__/\__,_/_/ /_/_/\___/\__,_/  
"""
        print(self.OK + art + self.RESET)
        print(f"{self.BOLD}O2 Batch Runner{self.RESET}  "
              f"{self.DIM}jobs={total}  timeout={timeout}s  grace={grace}s  kill_after={kill_after}s{self.RESET}\n")

    def sig_line(self, msg, level="info"):
        col = {"info": self.DIM, "warn": self.WARN, "err": self.ERR, "ok": self.OK}.get(level, "")
        print(col + msg + self.RESET)

    def progress_line(self, i, total, run_id, extra=""):
        bar_len = 24
        filled = int(bar_len * i / max(1, total))
        bar = "[" + "#" * filled + "-" * (bar_len - filled) + "]"
        prefix = f"{self.BOLD}{i}/{total}{self.RESET}"
        print(f"{prefix} {bar} {run_id} {extra}")

    def summary_box(self, stats, out_path):
        ok = stats.get("ok", 0)
        to = stats.get("timeout", 0)
        fail = stats.get("failed", 0)
        skip = stats.get("skipped", 0)
        miss = stats.get("missing", 0)
        dry = stats.get("dry-run", 0)
        total = sum(stats.values())
        lines = [
            f"Completed: {self.OK}{ok}{self.RESET}",
            f"Timeouts: {self.WARN}{to}{self.RESET}",
            f"Failed:   {self.ERR}{fail}{self.RESET}",
            f"Skipped:  {skip}",
            f"Missing:  {miss}",
            f"Dry-run:  {dry}",
            f"Summary:  {out_path}",
        ]
        box_w = max(len(_strip_ansi(s)) for s in lines) + 4
        top = "┌" + "─" * (box_w - 2) + "┐"
        bot = "└" + "─" * (box_w - 2) + "┘"
        print("\n" + top)
        for s in lines:
            pad = " " * (box_w - 2 - len(_strip_ansi(s)))
            print("│ " + s + pad + "│")
        print(bot)

def _strip_ansi(s: str) -> str:
    import re
    return re.sub(r"\x1b\[[0-9;]*m", "", s)

# ---------- Core ----------

def is_remote_input(s: str) -> bool:
    return s.startswith(REMOTE_PREFIXES)

def parse_index(path: Path):
    """Parse TSV/space-separated 'runId <path>' lines.
    <path> may be a local .lst/.root path or a remote URL (alien://, root://)."""
    runs = []
    with path.open("r", encoding="utf-8") as f:
        for lineno, line in enumerate(f, 1):
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split()
            if len(parts) < 2:
                print(f"[WARN] {path}:{lineno}: expected 'runId <path>', got: {line}", file=sys.stderr)
                continue
            run_id = parts[0]
            input_path = " ".join(parts[1:])  # keep raw (do NOT resolve remote URLs)
            runs.append((run_id, input_path))
    return runs

def ensure_outdir(outdir: Path):
    outdir.mkdir(parents=True, exist_ok=True)

def fmt_secs(s):
    return f"{int(s)}s"

def run_command_for_input(run_id: str, input_str: str, cmd_template: str,
                          timeout: int, grace: int, kill_after: int, logdir: Path, ui: UI,
                          easter_eggs: bool):
    quoted_input = shlex.quote(input_str)
    cmd = cmd_template.format(lst=quoted_input, run=shlex.quote(run_id))

    timestamp = time.strftime("%Y%m%d-%H%M%S")
    safe_run = "".join(c for c in run_id if c.isalnum() or c in ("-", "_"))
    log_path = logdir / f"{safe_run}-{timestamp}.log"

    with log_path.open("wb") as logf:
        header = f"[INFO] run={run_id} input={input_str}\n[INFO] cmd={cmd}\n\n"
        logf.write(header.encode())
        logf.flush()

        start = time.time()
        proc = subprocess.Popen(
            ["bash", "-lc", cmd],
            stdout=logf,
            stderr=subprocess.STDOUT,
            preexec_fn=os.setsid,  # new process group
            env=os.environ.copy(),
        )

        last_tick = start
        timed_out = False
        rc = None

        try:
            while True:
                rc = proc.poll()
                now = time.time()
                if rc is not None:
                    break
                if now - start >= timeout:
                    timed_out = True
                    ui.sig_line(f"  {ui.emoji('TO','[TO]')} Timeout {fmt_secs(timeout)} → sending SIGINT to pgid {proc.pid}…", "warn")
                    os.killpg(proc.pid, signal.SIGINT)
                    # graceful wait
                    for _ in range(grace):
                        rc = proc.poll()
                        if rc is not None:
                            break
                        time.sleep(1)
                    if rc is None:
                        ui.sig_line(f"  {ui.emoji('Zap','[TERM]')} Still running after {fmt_secs(grace)} → SIGTERM…", "warn")
                        os.killpg(proc.pid, signal.SIGTERM)
                        for _ in range(kill_after):
                            rc = proc.poll()
                            if rc is not None:
                                break
                            time.sleep(1)
                        if rc is None:
                            ui.sig_line(f"  {ui.emoji('Skull','[KILL]')} Escalating to SIGKILL.", "err")
                            os.killpg(proc.pid, signal.SIGKILL)
                            proc.wait()
                            rc = proc.returncode
                    break

                # tiny spinner heartbeat
                if now - last_tick >= 0.2 and ui.is_tty and not ui.plain:
                    elapsed = fmt_secs(now - start)
                    sys.stdout.write(f"    {ui.DIM}{ui.spin()} running {elapsed}{ui.RESET}\r")
                    sys.stdout.flush()
                    last_tick = now
                time.sleep(0.1)
        finally:
            if ui.is_tty and not ui.plain:
                sys.stdout.write(" " * 60 + "\r")  # clear line
                sys.stdout.flush()
            end = time.time()
            duration = end - start

    status = "ok"
    if timed_out:
        status = "timeout"
    if rc not in (0, None):
        status = status if status == "timeout" else "failed"

    # tasteful easter egg (rare, short, disabled with --plain or --no-emoji or if not TTY)
    if easter_eggs and ui.is_tty and not ui.plain and random.random() < 0.07:
        quips = [
            "May the SIGINT be ever in your favor.",
            "FairMQ: handshake now, shutdown later.",
            "If it hangs, we politely knock… then we *really* knock.",
            "Pro tip: increase --grace if QC needs more tea time.",
        ]
        ui.sig_line(f"  🥚 {random.choice(quips)}", "info")

    return {
        "run": run_id,
        "lst": input_str,  # keep legacy column name
        "rc": rc if rc is not None else -999,
        "status": status,
        "elapsed_s": round(duration, 3),
        "log": str(log_path),
    }

def main():
    ap = argparse.ArgumentParser(description="Batch-run O2/QC workflows with timeout & graceful shutdown.")
    ap.add_argument("index", type=Path, help="Path to index (format: <runId> <.lst or .root URL/path>)")
    ap.add_argument("--cmd", type=str, required=False, default=(
        "o2-ctf-reader-workflow --ctf-input {lst} --ctf-dict ccdb --onlyDet FT0 --severity=error -b | "
        "o2-qc --config json://$HOME/alice/QualityControl/Modules/FIT/FT0/etc/ft0-adc-mip-trending.json -b"
    ), help="Command template to execute. Use {lst} and {run} placeholders.")
    ap.add_argument("--timeout", type=int, default=120, help="Seconds before sending SIGINT to the process group.")
    ap.add_argument("--grace", type=int, default=20, help="Seconds to wait after SIGINT before SIGTERM.")
    ap.add_argument("--kill-after", type=int, default=10, help="Seconds to wait after SIGTERM before SIGKILL.")
    ap.add_argument("--outdir", type=Path, default=Path("logs"), help="Directory for per-run logs and summary.")
    ap.add_argument("--summary", type=str, default="summary.tsv", help="Summary filename (TSV) inside outdir.")
    ap.add_argument("--skip-existing", action="store_true", help="Skip run if a log for that run id exists in outdir.")
    ap.add_argument("--dry-run", action="store_true", help="Don’t execute, just print planned commands.")

    # flair toggles
    ap.add_argument("--plain", action="store_true", help="Disable colors, emojis, ASCII art, and spinner.")
    ap.add_argument("--no-emoji", action="store_true", help="Disable emojis (keep colors/art).")
    ap.add_argument("--no-eggs", action="store_true", help="Disable easter eggs.")

    args = ap.parse_args()
    ui = UI(plain=args.plain, no_emoji=args.no_emoji)

    ensure_outdir(args.outdir)
    runs = parse_index(args.index)
    total = len(runs)
    if total == 0:
        print(f"[ERROR] No runs parsed from {args.index}.")
        sys.exit(2)

    # quick resume heuristic
    existing = set()
    if args.skip_existing:
        for p in args.outdir.glob("*.log"):
            run_prefix = p.name.split("-", 1)[0]
            existing.add(run_prefix)

    ui.banner(total, args.timeout, args.grace, args.kill_after)
    print(f"[INFO] Logs -> {args.outdir.resolve()}\n")

    summary_rows = []
    stats = {"ok":0, "timeout":0, "failed":0, "skipped":0, "missing":0, "dry-run":0}
    t0 = time.time()

    for i, (run_id, input_str) in enumerate(runs, 1):
        ui.progress_line(i, total, run_id)

        # Local existence check for non-remote inputs
        if not is_remote_input(input_str):
            p = Path(input_str).expanduser()
            if not p.exists():
                print(f"    {ui.ERR}{ui.emoji('FAIL','[X]')}{ui.RESET} input not found -> {input_str}")
                summary_rows.append({"run": run_id, "lst": input_str, "rc": -1, "status": "missing", "elapsed_s": 0, "log": ""})
                stats["missing"] += 1
                continue

        if args.skip_existing and run_id in existing:
            print(f"    {ui.DIM}{ui.emoji('SKIP','[skip]')} skipping (log already exists){ui.RESET}")
            summary_rows.append({"run": run_id, "lst": input_str, "rc": 0, "status": "skipped", "elapsed_s": 0, "log": ""})
            stats["skipped"] += 1
            continue

        planned_cmd = args.cmd.format(lst=shlex.quote(input_str), run=shlex.quote(run_id))
        if args.dry_run:
            print(f"    {ui.DIM}{ui.emoji('DRY','[dry]')} {planned_cmd}{ui.RESET}")
            summary_rows.append({"run": run_id, "lst": input_str, "rc": 0, "status": "dry-run", "elapsed_s": 0, "log": ""})
            stats["dry-run"] += 1
            continue

        print(f"    {ui.emoji('Run','>')} starting  (timeout {args.timeout}s)")
        res = run_command_for_input(
            run_id, input_str, args.cmd, args.timeout, args.grace, args.kill_after, args.outdir, ui, easter_eggs=(not args.no_eggs)
        )
        status_emoji = {
            "ok": ui.emoji("OK","[ok]"),
            "timeout": ui.emoji("TO","[to]"),
            "failed": ui.emoji("FAIL","[x]"),
            "missing": ui.emoji("FAIL","[x]"),
            "skipped": ui.emoji("SKIP","[skip]"),
            "dry-run": ui.emoji("DRY","[dry]")
        }.get(res["status"], "")
        col = {"ok": ui.OK, "timeout": ui.WARN, "failed": ui.ERR}.get(res["status"], "")
        print(f"    {col}{status_emoji} {res['status']}{ui.RESET} (rc={res['rc']}, elapsed={fmt_secs(res['elapsed_s'])}) log={res['log']}")
        summary_rows.append(res)
        stats[res["status"]] = stats.get(res["status"], 0) + 1

    # write summary TSV
    summary_path = args.outdir / args.summary
    with summary_path.open("w", encoding="utf-8") as sf:
        sf.write("run\tlst\trc\tstatus\telapsed_s\tlog\n")
        for r in summary_rows:
            sf.write(f"{r['run']}\t{r['lst']}\t{r['rc']}\t{r['status']}\t{r['elapsed_s']}\t{r['log']}\n")

    t1 = time.time()
    print(f"\n[INFO] Finished {sum(stats.values())}/{total} in {fmt_secs(t1 - t0)}.")
    ui.summary_box(stats, str(summary_path.resolve()))
    print(f"{ui.DIM}[HINT] Pass *_sample.index to run a single alien:// .root per run. "
          f"Use --plain to disable flair.{ui.RESET}")

if __name__ == "__main__":
    main()
