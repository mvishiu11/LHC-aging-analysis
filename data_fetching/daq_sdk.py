#!/usr/bin/env python3
"""
daq_sdk.py  –  tiny helper‑SDK for data‑acquisition jobs & dashboards
"""

from __future__ import annotations
import json, os, socket, time, psutil, shutil
from dataclasses import dataclass, asdict
from typing import Dict, Any, Mapping, Optional, Protocol


# ─────────────────────────── public “contract” ────────────────────────────
class JobReporter(Protocol):
    """Anything that can emit JSON‑serialisable job‑telemetry packets."""

    def send(self, packet: Mapping[str, Any]) -> None:  ...
    def close(self) -> None:  ...


@dataclass
class ProgressPacket:
    name: str
    status: str
    progress: float                    # 0–100
    details: Dict[str, Any]
    ts: float = time.time()

    def as_json(self) -> bytes:
        return json.dumps(asdict(self)).encode()


# ───────────────────────────── dashboard pipe ─────────────────────────────
class DashboardSocket(JobReporter):
    """Binary‑compatible drop‑in replacement for your old DashboardClient."""

    def __init__(self, host: str = "localhost", port: int = 9999,
                 job_name: str = "DAQ job"):
        self.addr   = (host, port)
        self.job    = job_name
        self._sock: Optional[socket.socket] = None
        self._start = time.time()

    # -------------------------------- convenience helpers
    def start(self, total_items: int, detector: str = "", **extra):
        self._send("Starting", 0, dict(Total=total_items, Detector=detector, **extra))

    def step(self, done: int, total: int, failed: int = 0, **extra):
        done_tot = done + failed
        pct      = 0 if total == 0 else done_tot / total * 100
        elapsed  = time.time() - self._start
        eta      = (elapsed / done_tot * (total - done_tot)) if done_tot else 0
        self._send("Processing" if done_tot < total else "Finishing", pct, {
            **extra, "Completed": done, "Failed": failed, "ETA": f"{eta:.0f}s",
            "Elapsed": f"{elapsed:.0f}s", "Total": total
        })

    def finish(self, ok: int, failed: int, **extra):
        total = ok + failed
        self._send("Completed" if failed == 0 else "Completed with Errors",
                   100, dict(TotalCompleted=ok, TotalFailed=failed,
                             SuccessRate=f"{ok/total*100:.1f}%", **extra))
        self.close()

    # -------------------------------- core send
    def _send(self, status: str, progress: float, details: Dict[str, Any]):
        payload = ProgressPacket(self.job, status, progress, details).as_json()
        try:
            self._sock = socket.create_connection(self.addr, timeout=2)
            self._sock.send(payload)
        except OSError:
            pass
        finally:
            if self._sock:
                self._sock.close()

    def close(self):  pass  # nothing persistent


# ─────────────────────────── optional rich metrics ────────────────────────
def collect_resource_snapshot() -> Dict[str, Any]:
    """Host‑side resource metrics (lightweight, no psutil? → return {})"""
    try:
        cpu = psutil.cpu_percent()
        mem = psutil.virtual_memory().percent
        net = psutil.net_io_counters()
        disk = shutil.disk_usage(os.path.expanduser("~"))
        return {
            "CPU_%": cpu,
            "RAM_%": mem,
            "TX_MB": round(net.bytes_sent / 1e6, 1),
            "RX_MB": round(net.bytes_recv / 1e6, 1),
            "DiskFree_GB": round(disk.free / 1e9, 1)
        }
    except Exception:
        return {}
