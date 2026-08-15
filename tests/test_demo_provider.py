from fastapi.testclient import TestClient

from demo_provider.app import app


def test_demo_provider_health_is_public_and_minimal():
    with TestClient(app) as client:
        response = client.get("/health")
        docs = client.get("/docs")
        schema = client.get("/openapi.json")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
    assert docs.status_code == 404
    assert schema.status_code == 404
