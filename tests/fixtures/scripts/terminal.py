import curses
import json
import os
import signal
import sys
import tty


def draw(screen: curses.window) -> None:
    screen.addstr(0, 0, "TUI ready")
    screen.refresh()
    assert screen.getch() == ord("q")


def main() -> None:
    match sys.argv[1]:
        case "info":
            descriptor = os.open("/dev/tty", os.O_RDWR)
            try:
                size = os.get_terminal_size(descriptor)
                print(
                    json.dumps(
                        {
                            "isatty": [os.isatty(fd) for fd in (0, 1, 2)],
                            "rows": size.lines,
                            "cols": size.columns,
                            "foreground": os.tcgetpgrp(descriptor) == os.getpgrp(),
                            "session_leader": os.getsid(0) == os.getpid(),
                            "term": os.environ.get("TERM"),
                            "cwd": os.getcwd(),
                        }
                    )
                )
            finally:
                os.close(descriptor)
        case "raw" | "bulk":
            tty.setraw(0)
            os.write(1, b"ready")
            data = bytearray()
            count = 5 if sys.argv[1] == "raw" else 65536
            while len(data) < count:
                data.extend(os.read(0, count - len(data)))
            os.write(1, bytes(data))
        case "interrupt":
            signal.signal(signal.SIGINT, signal.SIG_DFL)
            print("ready", flush=True)
            signal.pause()
        case "curses":
            curses.wrapper(draw)
            print("TUI finished")
        case "flood":
            while True:
                os.write(1, b"x" * 16384)
        case _:
            raise ValueError(sys.argv[1])


if __name__ == "__main__":
    main()
