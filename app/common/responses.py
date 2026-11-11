from fastapi.responses import JSONResponse


class JSONResponseNL(JSONResponse):
    """JSON response with a trailing newline so curl output stays readable."""

    def render(self, content) -> bytes:
        return super().render(content) + b"\n"
