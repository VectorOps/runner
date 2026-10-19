"""Isolated child launcher establishing a controlling terminal before exec."""

import errno
import os
import signal
import sys


def main() -> None:
    error_fd = int(sys.argv[1])
    try:
        os.set_inheritable(error_fd, False)
        os.login_tty(0)
        signal.signal(signal.SIGPIPE, signal.SIG_DFL)
        signal.signal(signal.SIGXFSZ, signal.SIG_DFL)
        os.execvpe(sys.argv[2], sys.argv[2:], os.environ)
    except OSError as exc:
        message = f"{exc.errno or errno.EIO}\n{exc}".encode("utf-8", errors="replace")
        os.write(error_fd, message[:4096])
        os._exit(127)


if __name__ == "__main__":
    main()
