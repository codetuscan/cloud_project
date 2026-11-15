"""Pure-ASGI middleware: tags every response with X-Request-ID (keeps a client-supplied one)."""
import uuid


class RequestIdMiddleware:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)
        rid = next((v.decode() for k, v in scope["headers"] if k == b"x-request-id"), None) or uuid.uuid4().hex[:12]

        async def send_with_id(message):
            if message["type"] == "http.response.start":
                message.setdefault("headers", []).append((b"x-request-id", rid.encode()))
            await send(message)

        await self.app(scope, receive, send_with_id)
