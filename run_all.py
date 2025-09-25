from __future__ import annotations
import sys, os, subprocess
from pathlib import Path

PY = sys.executable


def echo(msg: str) -> None:
    print(msg, flush=True)


def run(cmd):
    if isinstance(cmd, (list, tuple)) and cmd and str(cmd[0]).lower() == "python":
        cmd = [PY] + list(cmd[1:])
    elif isinstance(cmd, str) and cmd.startswith("python "):
        cmd = cmd.replace("python ", f'"{PY}" ', 1)
    echo("\n▶ " + (" ".join(cmd) if isinstance(cmd, (list, tuple)) else cmd))
    return subprocess.run(cmd, check=False).returncode


def step_userstats():
    path = os.environ.get("KOSIS_USERSTATS_FILE", "my_userstats.txt")
    if not Path(path).exists():
        echo(f"[skip] userstats file not found: {path}")
        return 0
    out = os.environ.get("KOSIS_DATA_OUT", "data_rows.csv")
    verbose = os.environ.get("KOSIS_VERBOSE", "1") not in (
        "0",
        "false",
        "False",
        "off",
        "OFF",
    )
    cmd = [
        PY,
        "run_build_catalog.py",
        "--mode",
        "userstats",
        "--userstats",
        path,
        "--out",
        out,
    ]
    if verbose:
        cmd.append("--verbose")
    return run(cmd)


def step_direct():
    out = os.environ.get("KOSIS_OUT", "series_catalog.csv")
    vw = os.environ.get("KOSIS_VWCD", "MT_ZTITLE")
    roots = os.environ.get("KOSIS_ROOTS", "AUTO")
    depth = os.environ.get("KOSIS_MAX_DEPTH", "5")
    cmd = [
        PY,
        "run_build_catalog.py",
        "--mode",
        "direct",
        "--vwcd",
        vw,
        "--roots",
        roots,
        "--max-depth",
        str(depth),
        "--out",
        out,
        "--verbose",
    ]
    return run(cmd)


def main():
    echo("[env] .env detected. If key not loaded, src/config.py uses dotenv to load it.")
    rc = step_userstats()
    if rc != 0:
        return rc
    rc = step_direct()
    if rc != 0:
        return rc
    echo("\n[run_all] done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

