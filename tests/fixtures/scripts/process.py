import os
from pathlib import Path
import signal
import subprocess
import sys
import time


def main() -> None:
    match sys.argv[1]:
        case "print":
            print(sys.argv[2])
        case "exit":
            raise SystemExit(int(sys.argv[2]))
        case "sleep":
            time.sleep(float(sys.argv[2]))
        case "pid":
            print(os.getpid(), flush=True)
            time.sleep(60)
        case "streams":
            print("out")
            print("err", file=sys.stderr)
            sys.exit(7)
        case "stdin":
            print(sys.stdin.read())
        case "uppercase":
            print(sys.stdin.read().upper())
        case "binary_stdin":
            sys.stdout.buffer.write(sys.stdin.buffer.read())
        case "binary_output":
            os.write(1, bytes([255, 0]))
        case "large_output":
            sys.stdout.write("x" * 1000000)
        case "environment":
            print(os.getcwd())
            print(os.environ["PROCESS_TEST_INHERITED"])
            print(os.environ["PROCESS_TEST_OVERLAY"])
        case "child_group":
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
            subprocess.Popen([sys.executable, __file__, "sleep", "60"])
            print("ready", flush=True)
            time.sleep(60)
        case "heartbeat":
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
            print("ready", flush=True)
            while True:
                Path(sys.argv[2]).write_text(str(time.monotonic_ns()))
                time.sleep(0.01)
        case "parent":
            child = subprocess.Popen(
                [sys.executable, "-u", __file__, "heartbeat", sys.argv[2]],
                stdout=subprocess.PIPE,
            )
            assert child.stdout is not None
            child.stdout.readline()
            print(child.pid, flush=True)
            time.sleep(60)
        case "noop":
            pass
        case _:
            raise ValueError(sys.argv[1])


if __name__ == "__main__":
    main()
