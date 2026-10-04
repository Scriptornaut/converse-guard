"""Custom gateway entry point. Start with uvicorn, not the unwrapped litellm CLI."""
from contextlib import asynccontextmanager
from litellm.proxy import proxy_server
from native_guard import ConverseOutputMiddleware, get_scanner

app = proxy_server.app
scanner = get_scanner()
app.add_middleware(ConverseOutputMiddleware, scanner=scanner)
original_lifespan = app.router.lifespan_context

@asynccontextmanager
async def lifespan(app):
    try:
        async with original_lifespan(app):
            yield
    finally:
        await scanner.close()

app.router.lifespan_context = lifespan
