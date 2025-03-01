from typing import Dict, List, Any
import asyncio
from datetime import datetime, timedelta
from ..core.redis_manager import RedisManager

class VisitCounterService:

    def __init__(self):
        """Initialize the visit counter service with Redis manager and in-memory cache"""
        self.redis_manager = RedisManager()
        self.cache: Dict[str, Dict[str, Any]] = {}  # Stores {page_id: {"value": count, "expiry": datetime}}

    async def increment_visit(self, page_id: str) -> None:
        """
        Increment visit count for a page, update Redis, and store in cache.
        
        Args:
            page_id: Unique identifier for the page
        """
        new_count = await self.redis_manager.increment(page_id, 1) # Updating Redis

         # Update cache immediately after Redis update
        self.cache[page_id] = {
            "value": new_count,
            "expiry": datetime.utcnow() + timedelta(seconds=5)
        }

    async def get_visit_count(self, page_id: str) -> int:
        """
         Get visit count for a page, using in-memory cache when valid.
        
        Args:
            page_id: Unique identifier for the page
            
        Returns:
            Dictionary containing visit count and data source (in-memory or Redis)
        """
        current_time = datetime.utcnow()

        # If cache exists and hasn't expired, return cached value
        if page_id in self.cache and self.cache[page_id]["expiry"] > current_time:
            return {"visits": self.cache[page_id]["value"], "served_via": "in_memory"}

        # If cache expired or missing, fetch from Redis
        redis_count = await self.redis_manager.get(page_id)

        # Store fetched count in cache with a TTL of 5 seconds
        self.cache[page_id] = {
            "value": redis_count,
            "expiry": current_time + timedelta(seconds=5)
        }

        return {"visits": redis_count, "served_via": "redis"}
