"""Package entrypoint for the TCP benchmark runner."""

from .tcp_bench import main


if __name__ == "__main__":
    raise SystemExit(main())
