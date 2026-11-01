"""Shared helpers for the member self-check scripts (Python standard library only).

Each member runs their own check to confirm their part works, against:
  * your laptop:  make local, then make check-mX   - cloud-only steps are skipped
  * AWS:          make up,    then make check-mX   - everything runs (uses the live load balancer)
"""
import json
import os
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BASE = os.environ.get("BASE", "http://localhost:8000").rstrip("/")
LIVE = not any(h in BASE for h in ("localhost", "127.0.0.1"))
TF1 = ROOT / "infra" / "terraform" / "01-platform"

_n = {"step": 0, "pass": 0, "fail": 0, "skip": 0}
C = {"h": "\033[1;36m", "ok": "\033[32m", "bad": "\033[31m", "dim": "\033[2m", "end": "\033[0m"}


# ------------------------------------------------------------------ output
def title(text):
    print(f"\n{C['h']}{'=' * 72}\n  {text}\n  target: {BASE}  ({'AWS - live' if LIVE else 'local'})\n{'=' * 72}{C['end']}")


def step(text, explain=""):
    _n["step"] += 1
    print(f"\n{C['h']}-- Step {_n['step']}: {text}{C['end']}")
    if explain:
        print(f"{C['dim']}   {explain}{C['end']}")


def check(name, ok, detail=""):
    _n["pass" if ok else "fail"] += 1
    mark = f"{C['ok']}PASS{C['end']}" if ok else f"{C['bad']}FAIL{C['end']}"
    print(f"   {mark} {name}" + (f"  ({detail})" if detail and not ok else ""))
    return ok


def skip(name, why="runs on AWS only"):
    _n["skip"] += 1
    print(f"   {C['dim']}SKIP {name} - {why}{C['end']}")


def show(label, value):
    if isinstance(value, (dict, list)):
        value = json.dumps(value, indent=2, default=str)
        value = "\n".join("      " + line for line in value.splitlines()[:25])
        print(f"   {label}:\n{value}")
    else:
        print(f"   {label}: {value}")


def done():
    print(f"\n{C['h']}  {_n['pass']} passed, {_n['fail']} failed, {_n['skip']} skipped{C['end']}\n")
    sys.exit(1 if _n["fail"] else 0)


# ------------------------------------------------------------------ HTTP
def call(method, path, body=None, params=None, url=None):
    target = url or BASE + path
    if params:
        target += "?" + "&".join(f"{k}={v}" for k, v in params.items())
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(target, data=data, method=method, headers={"content-type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            raw = r.read()
            return r.status, (json.loads(raw) if raw and raw[:1] in b"[{" else raw.decode(errors="ignore"))
    except urllib.error.HTTPError as e:
        raw = e.read()
        try:
            return e.code, json.loads(raw)
        except Exception:
            return e.code, raw.decode(errors="ignore")
    except Exception as e:
        return 0, str(e)


def get(path, **params):
    return call("GET", path, params=params or None)


def post(path, body=None, **params):
    return call("POST", path, body, params=params or None)


def wait(fn, timeout=30, every=1.0):
    """Retry fn() until it returns something truthy (or timeout); returns that value or None."""
    end = time.time() + timeout
    while time.time() < end:
        try:
            v = fn()
            if v:
                return v
        except Exception:
            pass
        time.sleep(every)
    return None


# ------------------------------------------------------------------ cloud CLIs (live steps)
def have(*tools):
    return all(shutil.which(t) for t in tools)


def run(cmd, show_output=True):
    """Run a shell command, print it and (the first lines of) its output."""
    print(f"   {C['dim']}$ {cmd}{C['end']}")
    r = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    out = (r.stdout or r.stderr).strip()
    if show_output and out:
        print("\n".join("      " + line for line in out.splitlines()[:20]))
    return r.returncode, r.stdout.strip()


def tf(name):
    r = subprocess.run(["terraform", f"-chdir={TF1}", "output", "-raw", name], capture_output=True, text=True)
    return r.stdout.strip() if r.returncode == 0 else ""


def region():
    return tf("region") or os.environ.get("AWS_REGION", "ap-south-1")


# ------------------------------------------------------------------ domain shortcuts
def supplier_with_stock(qty=100, name="Acme Components"):
    _, sup = post("/api/suppliers", {"name": name, "country": "India", "contact_email": "ops@acme.example"})
    sku = "SKU-" + uuid.uuid4().hex[:6].upper()
    post("/api/inventory/items", {"sku": sku, "supplier_id": sup["supplier_id"], "name": "Industrial pump",
                                  "quantity": qty, "reorder_level": 5})
    return sup, sku


def book(supplier_id, sku, qty=2, carrier="DHL"):
    return post("/api/shipments", {"supplier_id": supplier_id, "retailer_id": "RET-BLR-01",
                                   "retailer_email": "store@retailer.example", "origin": "Mumbai, IN",
                                   "destination": "Bengaluru, IN", "carrier": carrier,
                                   "items": [{"sku": sku, "quantity": qty}]})


def scan(tid, status, location="Pune Hub", hours_ago=0, carrier="DHL", **extra):
    return post("/api/tracking/events", {"tracking_id": tid, "carrier": carrier, "status": status,
                                         "location": location,
                                         "event_time": int(time.time() * 1000) - int(hours_ago * 3600_000), **extra})


def status_of(tid):
    code, body = get(f"/api/status/{tid}")
    return body if code == 200 else None


def wait_status(tid, expected, timeout=30):
    return wait(lambda: (lambda b: b if b and b["state"]["status"] == expected else None)(status_of(tid)), timeout)
