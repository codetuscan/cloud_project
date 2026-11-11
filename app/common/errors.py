from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from app.common.responses import JSONResponseNL as JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

_CODES = {
    400: "invalid_request", 404: "not_found", 409: "conflict",
    500: "internal_error", 503: "service_unavailable",
}


class ApiError(Exception):
    def __init__(self, status: int, message: str, code: str | None = None, extra: dict | None = None):
        self.status = status
        self.message = message
        self.code = code or _CODES.get(status, "error")
        self.extra = extra or {}


def _body(status: int, message: str, code: str | None = None, extra: dict | None = None) -> JSONResponse:
    return JSONResponse(
        status_code=status,
        content={"error": {"code": code or _CODES.get(status, "error"), "message": message, **(extra or {})}},
    )


def install_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(ApiError)
    async def _api(_: Request, e: ApiError):
        return _body(e.status, e.message, e.code, e.extra)

    @app.exception_handler(StarletteHTTPException)
    async def _http(_: Request, e: StarletteHTTPException):
        return _body(e.status_code, str(e.detail))

    @app.exception_handler(RequestValidationError)
    async def _val(_: Request, e: RequestValidationError):
        return _body(400, "; ".join(f"{'.'.join(map(str, x['loc']))}: {x['msg']}" for x in e.errors()))

    @app.exception_handler(Exception)
    async def _any(_: Request, e: Exception):
        logging_msg = f"{type(e).__name__}: {e}"
        import logging
        logging.getLogger("error").exception(logging_msg)
        return _body(500, "internal server error")
