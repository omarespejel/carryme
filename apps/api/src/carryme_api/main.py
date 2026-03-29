"""CLI entrypoint for the carryme API service."""

import os

import uvicorn

DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 8000


def get_server_host() -> str:
    """Return the bind host for the API process."""

    return os.getenv("CARRYME_API_HOST", DEFAULT_HOST)


def get_server_port() -> int:
    """Return the bind port for the API process."""

    raw_port = os.getenv("CARRYME_API_PORT")
    if raw_port is None:
        return DEFAULT_PORT
    return int(raw_port)


def main() -> None:
    """Run the FastAPI development server."""

    uvicorn.run(
        "carryme_api.app:app",
        host=get_server_host(),
        port=get_server_port(),
        reload=False,
    )


if __name__ == "__main__":
    main()
