import socket

import asyncpg
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.common.errors import ApiError, install_error_handlers


def make_app():
    app = FastAPI()
    install_error_handlers(app)

    @app.get("/notfound")
    def a():
        raise ApiError(404, "object not found")

    @app.get("/refused")
    def b():
        raise ConnectionRefusedError("postgres down")

    @app.get("/dbdown")
    def c():
        raise asyncpg.exceptions.CannotConnectNowError("starting up")

    @app.get("/dns")
    def c2():
        raise socket.gaierror(-3, "Temporary failure in name resolution")

    @app.get("/boom")
    def d():
        raise RuntimeError("secret internal detail")

    @app.get("/busy")
    def e():
        raise ApiError(503, "busy", "server_busy", headers={"Retry-After": "5"})

    @app.get("/int/{n}")
    def f(n: int):
        return {"n": n}

    return TestClient(app, raise_server_exceptions=False)


def test_api_error_json_shape():
    r = make_app().get("/notfound")
    assert r.status_code == 404 and r.json() == {"error": {"code": "not_found", "message": "object not found"}}


def test_dependency_outage_is_503_not_500():
    c = make_app()
    for path in ("/refused", "/dbdown", "/dns"):
        r = c.get(path)
        assert r.status_code == 503 and r.json()["error"]["code"] == "service_unavailable"
        assert r.headers["retry-after"] == "5"


def test_unexpected_error_is_500_without_leaking_details():
    r = make_app().get("/boom")
    assert r.status_code == 500 and "secret" not in r.text


def test_custom_headers_and_codes():
    r = make_app().get("/busy")
    assert r.status_code == 503 and r.json()["error"]["code"] == "server_busy" and r.headers["retry-after"] == "5"


def test_validation_error_is_400_json():
    r = make_app().get("/int/abc")
    assert r.status_code == 400 and r.json()["error"]["code"] == "invalid_request"


def test_unknown_route_is_json_404():
    r = make_app().get("/nope")
    assert r.status_code == 404 and "error" in r.json()
