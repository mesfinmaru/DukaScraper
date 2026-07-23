from fastapi import APIRouter

router = APIRouter()


@router.get("/")
async def list_storage_items():
    """
    Placeholder endpoint for listing items in storage.
    This would typically interact with MinIO or another storage backend.
    """
    return {"message": "Storage endpoint is active. Implement listing logic here."}