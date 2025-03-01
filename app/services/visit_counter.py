from typing import Dict, Any
import asyncio
from datetime import datetime, timedelta
from ..core.redis_manager import RedisManager

class VisitCounterService:
    def __init__(self):
        self.redis_manager = RedisManager()
        self.cache: Dict[str, Dict[str, Any]] = {}
        self.buffer: Dict[str, int] = {}
        asyncio.create_task(self._flush_to_redis())

    async def increment_visit(self, page_id: str):
        """Increment visit count in the buffer and cache."""
        current_time = datetime.utcnow()

        # Fetching the latest count from Redis if not already cached
        redis_count, redis_node = await self.redis_manager.get(page_id) or (0, "unknown")
        cached_value = self.cache.get(page_id, {"value": redis_count})["value"]

        # Incrementing buffer and cache
        self.buffer[page_id] = self.buffer.get(page_id, 0) + 1
        self.cache[page_id] = {
            "value": cached_value + 1,
            "expiry": current_time + timedelta(seconds=5)
        }

    async def get_visit_count(self, page_id: str) -> Dict[str, Any]:
        """Retrieve visit count, combining Redis and pending increments."""
        current_time = datetime.utcnow()

        # Serving from cache if valid
        if page_id in self.cache and self.cache[page_id]["expiry"] > current_time:
            return {"visits": self.cache[page_id]["value"], "served_via": "in_memory"}

        # Flushing buffer if there's a cache miss
        if self.buffer.get(page_id, 0) > 0:
            await self._flush_to_redis()

        # Fetching count from Redis
        redis_count, redis_node = await self.redis_manager.get(page_id) or (0, "unknown")

        # Ensuring key exists in Redis
        if redis_count is None:
            await self.redis_manager.increment(page_id, 0)
            redis_count = 0

        # Combining Redis value with pending increments in buffer
        total_count = redis_count + self.buffer.get(page_id, 0)

        # Storing in cache with 5s TTL
        self.cache[page_id] = {
            "value": total_count,
            "expiry": current_time + timedelta(seconds=5)
        }
        return {"visits": total_count, "served_via": redis_node}

    async def _flush_to_redis(self):
        """Flush buffered visit counts to Redis every 30 seconds."""
        if self.buffer:
            # Copy buffer and clear it before updating Redis
            batch_data = self.buffer.copy()
            self.buffer.clear()

            # Batch updating Redis
            for page_id, count in batch_data.items():
                await self.redis_manager.increment(page_id, count)

        await asyncio.sleep(30)  # Flushing every 30 seconds