import json
import hashlib
import os
import sys
from datetime import datetime, timedelta
from threading import Lock
from time import sleep
from os.path import join as path_join

from nxc.paths import NXC_PATH
from nxc.logger import nxc_logger

RUNS_DIR = path_join(NXC_PATH, "runs")


def _ensure_runs_dir():
    os.makedirs(RUNS_DIR, exist_ok=True)


def _build_run_id(args, targets):
    """Deterministic run ID from the command-line arguments and target list."""
    key_parts = [
        args.protocol,
        str(sorted(targets)),
        str(getattr(args, "username", [])),
        str(getattr(args, "password", [])),
        str(getattr(args, "module", None)),
    ]
    digest = hashlib.sha256("|".join(key_parts).encode()).hexdigest()[:16]
    return f"{args.protocol}_{digest}"


def _run_state_path(run_id):
    return path_join(RUNS_DIR, f"{run_id}.json")


def save_run_state(run_id, protocol, targets, completed_targets):
    """Persist the current run state to disk."""
    _ensure_runs_dir()
    state = {
        "run_id": run_id,
        "protocol": protocol,
        "targets": targets,
        "completed_targets": list(completed_targets),
        "updated_at": datetime.now().isoformat(),
    }
    path = _run_state_path(run_id)
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(state, f, indent=2)
    os.replace(tmp, path)


def load_run_state(run_id):
    """Load a saved run state. Returns None if not found."""
    path = _run_state_path(run_id)
    if not os.path.exists(path):
        return None
    with open(path) as f:
        return json.load(f)


def get_last_run_id():
    """Return the run_id of the most recently updated state file, or None."""
    _ensure_runs_dir()
    files = [f for f in os.listdir(RUNS_DIR) if f.endswith(".json")]
    if not files:
        return None
    files.sort(key=lambda f: os.path.getmtime(path_join(RUNS_DIR, f)), reverse=True)
    with open(path_join(RUNS_DIR, files[0])) as f:
        data = json.load(f)
    return data.get("run_id")


def filter_completed_targets(run_id, targets):
    """Remove already-completed targets for a resumed run. Returns filtered list."""
    state = load_run_state(run_id)
    if state is None:
        nxc_logger.fail(f"No saved run found with id '{run_id}'. Cannot resume.")
        sys.exit(1)

    completed = set(state.get("completed_targets", []))
    remaining = [t for t in targets if t not in completed]
    skipped = len(targets) - len(remaining)
    if skipped:
        nxc_logger.display(f"Resuming run '{run_id}': skipping {skipped} already-completed target(s), {len(remaining)} remaining")
    if not remaining:
        nxc_logger.success("All targets from the previous run are already completed.")
        sys.exit(0)
    return remaining


class RunStateTracker:
    """Thread-safe tracker that records completed targets during a run."""

    def __init__(self, run_id, protocol, all_targets):
        self.run_id = run_id
        self.protocol = protocol
        self.all_targets = list(all_targets)
        self.completed = set()
        self._lock = Lock()
        save_run_state(run_id, protocol, self.all_targets, self.completed)

    def mark_done(self, target):
        with self._lock:
            self.completed.add(target)
            save_run_state(self.run_id, self.protocol, self.all_targets, self.completed)


# ---------------------------------------------------------------------------
# Scheduling: time window and run-day checks
# ---------------------------------------------------------------------------

def parse_hhmm(value):
    """Parse an HHMM string into (hour, minute). Raises ValueError on bad input."""
    if len(value) != 4 or not value.isdigit():
        raise ValueError(f"Invalid HHMM format: '{value}'. Expected exactly 4 digits, e.g. 0830 or 1730.")
    hour, minute = int(value[:2]), int(value[2:])
    if hour > 23 or minute > 59:
        raise ValueError(f"Invalid time: '{value}'. Hours must be 0-23, minutes 0-59.")
    return hour, minute


def parse_run_days(value):
    """Parse a comma-separated day string like '1,2,3,4,5' into a set of ints (1=Mon .. 7=Sun)."""
    days = set()
    for part in value.split(","):
        part = part.strip()
        if not part.isdigit() or not (1 <= int(part) <= 7):
            raise ValueError(f"Invalid run day: '{part}'. Must be 1-7 (1=Monday, 7=Sunday).")
        days.add(int(part))
    return days


DAY_NAMES = {1: "Monday", 2: "Tuesday", 3: "Wednesday", 4: "Thursday", 5: "Friday", 6: "Saturday", 7: "Sunday"}


def check_run_days(run_days):
    """If today is not a valid run day, wait until the next valid day (at 00:00) or exit.

    Returns once the current day is valid.
    """
    while True:
        today = datetime.now().isoweekday()  # 1=Mon .. 7=Sun
        if today in run_days:
            return
        next_day = _next_valid_day(today, run_days)
        day_name = DAY_NAMES[next_day]
        now = datetime.now()
        target_dt = _next_weekday_dt(now, next_day)
        wait_secs = (target_dt - now).total_seconds()
        nxc_logger.display(f"Today is {DAY_NAMES[today]} which is not in the run-days schedule. Waiting until {day_name} ({target_dt.strftime('%Y-%m-%d %H:%M')}) ...")
        sleep(wait_secs)


def _next_valid_day(current_isoweekday, valid_days):
    for offset in range(1, 8):
        candidate = ((current_isoweekday - 1 + offset) % 7) + 1
        if candidate in valid_days:
            return candidate
    return current_isoweekday


def _next_weekday_dt(now, target_isoweekday):
    """Return a datetime for midnight on the next occurrence of target_isoweekday."""
    current = now.isoweekday()
    days_ahead = (target_isoweekday - current) % 7
    if days_ahead == 0:
        days_ahead = 7
    target_date = now.date() + timedelta(days=days_ahead)
    return datetime.combine(target_date, datetime.min.time())


def wait_for_start_time(start_hour, start_minute):
    """Block until the clock reaches start_time today (or immediately if already past)."""
    now = datetime.now()
    start_dt = now.replace(hour=start_hour, minute=start_minute, second=0, microsecond=0)
    if now >= start_dt:
        return
    wait_secs = (start_dt - now).total_seconds()
    nxc_logger.display(f"Waiting until start time {start_hour:02d}:{start_minute:02d} ({int(wait_secs)}s) ...")
    sleep(wait_secs)


def is_past_end_time(end_hour, end_minute):
    """Return True if the current time is at or past the end time."""
    now = datetime.now()
    end_dt = now.replace(hour=end_hour, minute=end_minute, second=0, microsecond=0)
    return now >= end_dt
