from carryme_api.app import app
from fastapi.testclient import TestClient


def test_health_endpoint() -> None:
    client = TestClient(app)

    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {
        "service": {
            "name": "carryme-api",
            "version": "0.1.0",
            "environment": "development",
        },
        "status": "ok",
    }


def test_versioned_health_endpoint() -> None:
    client = TestClient(app)

    response = client.get("/v1/health")

    assert response.status_code == 200
    assert response.json()["status"] == "ok"
