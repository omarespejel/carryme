"""CLI entrypoint for the carryme API service."""

import uvicorn


def main() -> None:
    """Run the FastAPI development server."""

    uvicorn.run("carryme_api.app:app", host="127.0.0.1", port=8000, reload=False)


if __name__ == "__main__":
    main()
