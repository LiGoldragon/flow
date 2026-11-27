"""Contract for the gate's per-case isolation and complete failure inventory."""
from pathlib import Path
import subprocess
import sys
import tempfile


def main(runner):
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        binary = root / "fake"
        binary.write_text(f"#!{sys.executable}\n" + r"""
import os, pathlib, subprocess, sys, time
names = ["pass", "assertion", "hang", "last", "ignored"]
if "--list" in sys.argv:
    for name in (["ignored"] if "--ignored" in sys.argv else names):
        print(name + ": test")
    sys.exit(0)
name = sys.argv[sys.argv.index("--exact") + 1]
if name == "ignored": raise RuntimeError("ignored test executed")
with open("seen", "a") as record:
    print(name, file=record)
if name == "assertion":
    print("original assertion: actual != expected", flush=True)
    sys.exit(101)
if name == "hang":
    nested = "import os,pathlib,subprocess,sys,time; leaf=subprocess.Popen([sys.executable,'-c','import time;time.sleep(600)'],start_new_session=True); pathlib.Path('descendant').write_text(str(os.getpid())+' '+str(leaf.pid)); time.sleep(600)"
    subprocess.Popen([sys.executable, "-c", nested], start_new_session=True)
    print("entered blocking fixture", flush=True)
    time.sleep(600)
print("test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out")
""")
        binary.chmod(0o700)
        result = subprocess.run(
            [sys.executable, runner, "--seconds", "2", "--jobs", "1", binary],
            cwd=tmp, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            timeout=15,
        )
        print(result.stdout, flush=True)
        assert result.returncode == 1
        assert (root / "seen").read_text().splitlines() == ["pass", "assertion", "hang", "last"]
        for expected in ["PASS pass", "PASS last", "FAIL assertion", "FAIL hang",
                         "original assertion: actual != expected", "entered blocking fixture",
                         "TIMEOUT after 2s: hang", "2 passed; 2 failed; 1 ignored; 5 total",
                         "failed: assertion", "failed: hang"]:
            assert expected in result.stdout, expected
        nested = (root / "descendant").read_text().split()
        assert len(nested) == 2
        assert all(not Path("/proc", pid).exists() for pid in nested)


if __name__ == "__main__":
    main(sys.argv[1])
