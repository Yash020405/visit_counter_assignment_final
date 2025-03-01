from typing import Dict, List, Any
import asyncio
from datetime import datetime
from ..core.redis_manager import RedisManager

class VisitCounterService:
    visit_counts: Dict[str, int] = {}  # Shared across all instances

    def __init__(self):
        """Initialize the visit counter service with Redis manager"""
        self.redis_manager = RedisManager()

    async def increment_visit(self, page_id: str) -> None:
        """
        Increment visit count for a page
        
        Args:
            page_id: Unique identifier for the page
        """
        if page_id not in VisitCounterService.visit_counts:
            VisitCounterService.visit_counts[page_id] = 0
        VisitCounterService.visit_counts[page_id] += 1

    async def get_visit_count(self, page_id: str) -> int:
        """
        Get current visit count for a page
        
        Args:
            page_id: Unique identifier for the page
            
        Returns:
            Current visit count
        """
        return VisitCounterService.visit_counts.get(page_id, 0)
