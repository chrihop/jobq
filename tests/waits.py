#!/usr/bin/env python3
"""Run real CLI/MCP blocking wait checks with an exclusive, test-owned daemon.

python3 tests/waits.py target/release/jobq
Never stops an existing daemon; the normal singleton lock remains enforced.
"""
import os
from pathlib import Path
import subprocess
import sys
from smoke import BIN, Fixture


if __name__ == "__main__":
    fixture = Fixture()
    try:
        fixture.configure(runners=3, queues='["1s", "5s", "10s", "60s"]', cap="256MiB")
        fixture.rpc("reload")
        env = dict(os.environ, JOBQ_TEST_SOCKET=str(fixture.sock))
        scripts = Path(__file__).parent
        subprocess.run([sys.executable, str(scripts / "wait_live.py"), str(BIN),
                        str(fixture.root / "wait-evidence")], env=env, check=True, timeout=120)
        subprocess.run([sys.executable, str(scripts / "wait_many.py"), str(BIN),
                        "--live", "--long"], env=env, check=True, timeout=120)
    finally:
        fixture.close()
