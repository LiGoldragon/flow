"""Run every enabled Rust test with its own deadline and descendant reaper."""
import argparse
import concurrent.futures
import ctypes
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import tempfile


def run_case(binary, name, seconds):
    # Become the reparenting point even for a fixture that forks and calls
    # setsid. Killing a process group alone cannot clean up such a fixture.
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(36, 1, 0, 0, 0) != 0:  # PR_SET_CHILD_SUBREAPER
        raise OSError(ctypes.get_errno(), "cannot own fixture descendants")
    with tempfile.TemporaryFile() as log:
        command = [binary, "--exact", name, "--test-threads=1"]
        case = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT,
                                start_new_session=True)
        try:
            code = case.wait(timeout=seconds)
        except subprocess.TimeoutExpired:
            print(f"TIMEOUT after {seconds}s: {name}", flush=True)
            case.kill()
            case.wait()
            code = 124
        finally:
            # Once a child exits, any grandchildren are adopted by this
            # reaper, including those in unrelated process groups.
            children = Path(f"/proc/self/task/{os.getpid()}/children")
            while owned := children.read_text().split():
                for pid in map(int, owned):
                    try:
                        os.kill(pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                for pid in map(int, owned):
                    try:
                        os.waitpid(pid, 0)
                    except ChildProcessError:
                        pass
        log.seek(0)
        detail = log.read().decode(errors="replace")
        if code or not re.search(r"^test result: ok\. 1 passed;", detail, re.M):
            print(detail, flush=True)
            return code or 1
    return 0


def cases(binary, ignored, seconds):
    opts = ["--ignored"] if ignored else []
    output = subprocess.check_output([binary, "--list", *opts], text=True,
                                     timeout=seconds)
    return [line[:-6] for line in output.splitlines() if line.endswith(": test")]


def suite(binaries, seconds, jobs):
    selected = []
    ignored = 0
    for binary in binaries:
        all_names = cases(binary, False, seconds)
        skipped = set(cases(binary, True, seconds))
        if not skipped.issubset(all_names):
            raise RuntimeError(f"{binary}: inconsistent test inventory")
        selected.extend((binary, name) for name in all_names if name not in skipped)
        ignored += len(skipped)
    def run(pair):
        binary, name = pair
        with tempfile.TemporaryFile() as output:
            done = subprocess.run([sys.executable, __file__, "--case", binary, name,
                                   "--seconds", str(seconds)], stdout=output,
                                  stderr=subprocess.STDOUT)
            output.seek(0)
            return name, done.returncode, output.read().decode(errors="replace")
    print(f"running {len(selected)} enabled tests; {jobs} workers; {seconds}s per test", flush=True)
    failures = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=jobs) as workers:
        for future in concurrent.futures.as_completed([workers.submit(run, p) for p in selected]):
            name, code, output = future.result()
            if code:
                failures.append(name)
                print(f"FAIL {name}\n{output}", flush=True)
            else:
                print(f"PASS {name}", flush=True)
    print(f"test result: {len(selected)-len(failures)} passed; {len(failures)} failed; "
          f"{ignored} ignored; {len(selected)+ignored} total", flush=True)
    for name in sorted(failures):
        print(f"failed: {name}")
    return int(not selected or bool(failures))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--case", nargs=2)
    parser.add_argument("--seconds", type=int, required=True)
    parser.add_argument("--jobs", type=int, default=4)
    parser.add_argument("binaries", nargs="*")
    args = parser.parse_args()
    sys.exit(run_case(*args.case, args.seconds) if args.case else
             suite(args.binaries, args.seconds, args.jobs))
