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

        redis_count, redis_node = await self.redis_manager.get(page_id) or (0, "unknown")
        cached_value = self.cache.get(page_id, {"value": redis_count})["value"]

        self.buffer[page_id] = self.buffer.get(page_id, 0) + 1
        self.cache[page_id] = {
            "value": cached_value + 1,
            "expiry": current_time + timedelta(seconds=5)
        }

    async def get_visit_count(self, page_id: str) -> Dict[str, Any]:
        """Retrieve visit count, combining Redis and pending increments."""
        current_time = datetime.utcnow()

        # Check if the value is in the cache and not expired
        if page_id in self.cache and self.cache[page_id]["expiry"] > current_time:
            return {"visits": self.cache[page_id]["value"], "served_via": "in_memory"}

        # Flush buffered increments to Redis if necessary
        if self.buffer.get(page_id, 0) > 0:
            await self._flush_to_redis()

        # Get the value from Redis
        redis_count, redis_node = await self.redis_manager.get(page_id)

        # If the key doesn't exist in Redis, initialize it
        if redis_count is None:
            await self.redis_manager.increment(page_id, 0)
            redis_count = 0

        # Calculate the total count (Redis count + buffered increments)
        total_count = redis_count + self.buffer.get(page_id, 0)

        # Update the cache
        self.cache[page_id] = {
            "value": total_count,
            "expiry": current_time + timedelta(seconds=5)
        }

        return {"visits": total_count, "served_via": redis_node}

    async def _flush_to_redis(self):
        """Flush buffered visit counts to Redis every 30 seconds."""
        if self.buffer:
            batch_data = self.buffer.copy()
            self.buffer.clear()

            for page_id, count in batch_data.items():
                try:
                    await self.redis_manager.increment(page_id, count)
                except Exception as e:
                    print(f"Failed to flush to Redis: {e}")
                    # Re-add failed increments to the buffer
                    self.buffer[page_id] = self.buffer.get(page_id, 0) + count

        await asyncio.sleep(30)