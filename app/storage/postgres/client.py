from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.common.config.settings import settings
from app.common.logger.logger import logger


class PostgresManager:
    """
    Manages the asynchronous connection to the PostgreSQL database.
    Uses SQLAlchemy and asyncpg for high-performance database operations.
    """
    def __init__(self):
        # Database URL should be in your .env file (e.g., postgresql+asyncpg://user:pass@localhost:5432/dukascraper)
        db_url = getattr(settings, "DATABASE_URL", "postgresql+asyncpg://postgres:postgres@localhost:5432/dukascraper")
        
        # Create the async engine
        self.engine = create_async_engine(db_url, echo=False, pool_size=20, max_overflow=10)
        
        # Create a session factory
        self.SessionLocal = async_sessionmaker(
            bind=self.engine,
            class_=AsyncSession,
            expire_on_commit=False
        )

    async def connect(self) -> None:
        """Pings the database to verify connectivity on startup."""
        try:
            async with self.engine.begin() as conn:
                # Simple query to test the connection
                await conn.execute(text("SELECT 1"))
            logger.info("PostgreSQL Database connected successfully.")
        except Exception as e:
            logger.error(f"PostgreSQL Connection Error: {e}")
            raise e

    async def close(self) -> None:
        """Closes the connection pool on shutdown."""
        await self.engine.dispose()
        logger.info("PostgreSQL connection closed.")

# Global database manager instance
pg_client = PostgresManager()