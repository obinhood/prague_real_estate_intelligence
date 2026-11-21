"""
Launch the Prague Real Estate Intelligence API + dashboard.
Run from the project root:
    ./.venv/bin/python start_api.py
Then open the printed local URL in your browser.
"""
from __future__ import annotations

import os
import socket

import uvicorn


def _find_available_port(start_port: int, host: str = "127.0.0.1", attempts: int = 20) -> int:
    for port in range(start_port, start_port + attempts):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                sock.bind((host, port))
            except OSError:
                continue
            return port
    raise RuntimeError(f"No open port found in range {start_port}-{start_port + attempts - 1}.")


if __name__ == "__main__":
    host = os.getenv("HOST", "0.0.0.0")
    default_port = int(os.getenv("PORT", "8000"))
    port = _find_available_port(default_port)
    localhost_url = f"http://127.0.0.1:{port}"

    if port != default_port:
        print(f"Port {default_port} is busy; starting dashboard on {localhost_url} instead.")
    else:
        print(f"Starting dashboard on {localhost_url}")

    uvicorn.run("src.api.main:app", host=host, port=port, reload=True)
