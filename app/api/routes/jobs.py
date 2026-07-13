import uuid

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, HttpUrl

from app.common.logger.logger import logger
from app.pipeline.producer.kafka_producer import kafka_producer

router = APIRouter()
RAW_INTEL_TOPIC = "threat.intel.raw"

class ScrapeRequest(BaseModel):
    """Schema for incoming scraping requests from the UI or external systems."""
    url: HttpUrl
    layer: str = "surface" # 'surface', 'deep', or 'dark'

@router.post("/trigger", status_code=202)
async def trigger_scrape_job(request: ScrapeRequest):
    """
    Receives a URL and publishes a scraping job to the Kafka pipeline.
    The appropriate worker will pick this up asynchronously.
    """
    job_id = str(uuid.uuid4())
    
    try:
        # Construct the event payload
        job_event = {
            "job_id": job_id,
            "target_url": str(request.url),
            "target_layer": request.layer,
            "status": "pending"
        }
        
        # Publish the job to Kafka for the workers to consume
        await kafka_producer.publish_event(
            topic=RAW_INTEL_TOPIC,
            key=job_id,
            value=job_event
        )
        
        logger.info(f"Successfully triggered job {job_id} for URL: {request.url}")
        return {
            "message": "Scraping job submitted successfully",
            "job_id": job_id,
            "queue": RAW_INTEL_TOPIC
        }
        
    except Exception as e:
        logger.error(f"Failed to submit scrape job: {e}")
        raise HTTPException(status_code=500, detail="Internal Pipeline Error")