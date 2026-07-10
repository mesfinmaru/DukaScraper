"""Duka Scraper - FastAPI application entry point."""

from fastapi import FastAPI

app = FastAPI(
    title="Duka Scraper",
    description="Amharic-focused web scraping and content extraction platform",
    version="0.1.0",
)


@app.get("/health")
async def health_check():
    return {"status": "ok"}
