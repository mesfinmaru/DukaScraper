from fastapi import APIRouter

from app.api.routes import (
    analytics,
    articles,
    auth,
    credentials,
    exports,
    jobs,
    monitoring,
    storage,
)
from app.api.websocket.job_status import router as ws_router

api_router = APIRouter()

# Register all sub-routers here
api_router.include_router(auth.router, prefix="/auth", tags=["Authentication"])
api_router.include_router(jobs.router, prefix="/jobs", tags=["Scraping Jobs"])
api_router.include_router(articles.router, prefix="/articles", tags=["Articles"])
api_router.include_router(storage.router, prefix="/storage", tags=["Storage"])
api_router.include_router(analytics.router, prefix="/analytics", tags=["Analytics"])
api_router.include_router(credentials.router, prefix="/credentials", tags=["Credential Management"])
api_router.include_router(exports.router, prefix="/exports", tags=["Export"])
api_router.include_router(monitoring.router, prefix="/monitoring", tags=["Monitoring"])
api_router.include_router(ws_router, tags=["WebSocket"])