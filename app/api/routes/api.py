from fastapi import APIRouter

from app.api.routes import crawl, jobs

# from app.api.routes import search  # We will add this later when ES is ready

api_router = APIRouter()

# Register all sub-routers here
api_router.include_router(jobs.router, prefix="/jobs", tags=["Scraping Jobs"])
api_router.include_router(crawl.router, prefix="/crawl", tags=["Crawl"])