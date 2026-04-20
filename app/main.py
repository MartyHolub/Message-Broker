from fastapi import FastAPI

from app.broker import router
from app.database import init_db


def create_app() -> FastAPI:
    application = FastAPI(title="Custom Message Broker")
    application.include_router(router)

    @application.on_event("startup")
    async def on_startup() -> None:
        await init_db()

    return application


app = create_app()
