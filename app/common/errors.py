import asyncio
import logging
import socket

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from app.common.responses import JSONResponseNL as JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

try:  # the metadata database driver exists only in the controller's runtime, but the image has it everywhere
    import asyncpg
    _DB_DOWN: tuple = (asyncpg.exceptions.PostgresConnectionError, asyncpg.exceptions.InterfaceError,
                       asyncpg.exceptions.CannotConnectNowError)
except ImportError:  # pragma: no cover
    _DB_DOWN = ()

# "a required service is unavailable": refused/reset connections, DNS failure for a stopped container
# (socket.gaierror is an OSError but not a ConnectionError), timeouts, and the driver's own connection errors
_DEP_DOWN: tuple = (ConnectionError, socket.gaierror, asyncio.TimeoutError, TimeoutError) + _DB_DOWN

_CODES = {
    400: "invalid_request", 404: "not_found", 409: "conflict",
    500: "internal_error", 503: "service_unavailable",
}


class ApiError(Exception):
    def __init__(self, status: int, message: str, code: str | None = None, extra: dict | None = None,
                 headers: dict | None = None):
        self.status = status
        self.message = message
        self.code = code or _CODES.get(status, "error")
        self.extra = extra or {}
        self.headers = headers or {}


def _body(status: int, message: str, code: str | None = None, extra: dict | None = None,
          headers: dict | None = None) -> JSONResponse:
    return JSONResponse(
        headers=headers,
        status_code=status,
        content={"error": {"code": code or _CODES.get(status, "error"), "message": message, **(extra or {})}},
    )


def install_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(ApiError)
    async def _api(_: Request, e: ApiError):
        return _body(e.status, e.message, e.code, e.extra, e.headers)

    @app.exception_handler(StarletteHTTPException)
    async def _http(_: Request, e: StarletteHTTPException):
        return _body(e.status_code, str(e.detail))

    @app.exception_handler(RequestValidationError)
    async def _val(_: Request, e: RequestValidationError):
        return _body(400, "; ".join(f"{'.'.join(map(str, x['loc']))}: {x['msg']}" for x in e.errors()))

    async def _dep(_: Request, e: Exception):
        # the assignment maps "required service/storage unavailable" to 503
        logging.getLogger("error").warning("dependency unavailable: %r", e)
        return _body(503, "a required service (metadata database) is unavailable; retry shortly",
                     "service_unavailable", headers={"Retry-After": "5"})

    for exc_type in _DEP_DOWN:  # Starlette registers one exception class per handler
        app.add_exception_handler(exc_type, _dep)

    @app.exception_handler(Exception)
    async def _any(_: Request, e: Exception):
        logging_msg = f"{type(e).__name__}: {e}"
        import logging
        logging.getLogger("error").exception(logging_msg)
        return _body(500, "internal server error")
