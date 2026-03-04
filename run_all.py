#!/usr/bin/env python3
"""
run_all.py
- Lokalno: požene vse scraperje (ali izbrane) zaporedno
- GitHub Actions: se navadno ne uporablja (tam je boljša matrix strategija v workflow-u),
  ampak je vseeno uporabno za ročni test.

Uporaba:
  python run_all.py
  python run_all.py --shops OBI Merkur
  python run_all.py --list
"""

import argparse
import os
import subprocess
import sys
from pathlib import Path

SCRIPTS = {
    "OBI": "ObiV1_github_fixed_v4.py",
    "KALCER": "KalcerV1_github_fixed_v4.py",
    "MERKUR": "MerkurV1_github_fixed_v5.py",
    "TEHNOLES": "TehnolesV1_github_fixed_v5.py",
    "SLOVENIJALES": "SlovenijalesV1_github_fixed_v5.py",
    "ZAGOZEN": "ZagozenV1_github_fixed_v5.py",
}


def is_ci() -> bool:
    return os.environ.get("GITHUB_ACTIONS", "").lower() == "true" or os.environ.get("CI")


def run_script(script_path: Path) -> int:
    env = os.environ.copy()

    # Če si na CI, piši outpute v artifacts/
    if is_ci():
        env.setdefault("OUTPUT_DIR", "artifacts")

    print(f"\n=== RUN: {script_path.name} ===")
    proc = subprocess.run([sys.executable, str(script_path)], env=env)
    return proc.returncode


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--shops", nargs="*", help="Seznam trgovin (npr. OBI MERKUR). Če izpustiš, zažene vse.")
    parser.add_argument("--list", action="store_true", help="Izpiši možne trgovine in končaj.")
    args = parser.parse_args()

    if args.list:
        print("Možne trgovine:")
        for k in SCRIPTS:
            print(f" - {k}")
        return 0

    wanted = [s.upper() for s in (args.shops or list(SCRIPTS.keys()))]
    unknown = [s for s in wanted if s not in SCRIPTS]
    if unknown:
        print(f"Neznane trgovine: {unknown}")
        return 2

    repo_dir = Path(__file__).resolve().parent
    rc = 0
    for shop in wanted:
        script = repo_dir / SCRIPTS[shop]
        if not script.exists():
            print(f"Manjka skripta: {script}")
            rc = 2
            continue
        code = run_script(script)
        if code != 0:
            print(f"NAPAKA: {script.name} exit={code}")
            rc = code if rc == 0 else rc

    return rc


if __name__ == "__main__":
    raise SystemExit(main())
