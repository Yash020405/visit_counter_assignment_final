import redis
from typing import Dict, Tuple
from .consistent_hash import ConsistentHash
from .config import settings
import time

class RedisManager:
    def __init__(self):
        """Initialize Redis connection pools and consistent hashing"""
        self.connection_pools: Dict[str, redis.ConnectionPool] = {}
        self.redis_clients: Dict[str, redis.Redis] = {}

        # Parse Redis nodes from comma-separated string
        redis_nodes = [node.strip() for node in settings.REDIS_NODES.split(",") if node.strip()]
        self.consistent_hash = ConsistentHash(redis_nodes, settings.VIRTUAL_NODES)

        # Initializing Redis connections
        for node in redis_nodes:
            self._add_redis_node(node)

    def _add_redis_node(self, node: str):
        """Dynamically add a Redis node"""
        if node in self.redis_clients:
            return  # Avoid duplicate connections

        retries = 3
        for attempt in range(retries):
            try:
                self.connection_pools[node] = redis.ConnectionPool.from_url(node)
                self.redis_clients[node] = redis.Redis(connection_pool=self.connection_pools[node], decode_responses=True)
                self.consistent_hash.add_node(node)
                break
            except redis.ConnectionError as e:
                if attempt == retries - 1:
                    raise e
                time.sleep(1)  # Wait before retrying

    async def get_connection(self, key: str) -> Tuple[redis.Redis, str]:
        """Get Redis connection for the given key using consistent hashing."""
        node = self.consistent_hash.get_node(key)
        port = node.split(":")[-1]

        if node == "redis://redis1:6379":
            port = "7070"
        elif node == "redis://redis2:6379":
            port = "7071"
        return self.redis_clients[node], f"redis_{port}"

    async def increment(self, key: str, amount: int = 1) -> int:
        """Increment a counter in Redis"""
        try:
            redis_client, _ = await self.get_connection(key)
            new_value = redis_client.incrby(key, amount)
            return new_value
        except redis.RedisError as e:
            print(f"Redis Increment Error: {e}")
            return 0

    async def get(self, key: str) -> Tuple[int, str]:
        """Get value for a key from Redis"""
        try:
            redis_client, node = await self.get_connection(key)
            value = redis_client.get(key)
            return (int(value) if value is not None else 0), node
        except redis.RedisError as e:
            print(f"Redis Get Error: {e}")
            return 0, "unknown"

    def add_new_redis_shard(self, node: str):
        """Public method to add a new Redis shard dynamically"""
        self._add_redis_node(node)