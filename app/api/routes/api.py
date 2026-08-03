from fastapi import APIRouter

from app.api.routes import articles, jobs, storage

api_router = APIRouter()

# Register all sub-routers here
api_router.include_router(jobs.router, prefix="/jobs", tags=["Scraping Jobs"])
api_router.include_router(articles.router, prefix="/articles", tags=["Articles"])
api_router.include_router(storage.router, prefix="/storage", tags=["Storage"])
