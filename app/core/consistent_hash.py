import hashlib
from bisect import bisect
from typing import List, Dict

class ConsistentHash:
    def __init__(self, nodes: List[str], virtual_nodes: int = 100):
        """Initialize consistent hashing with virtual nodes for each real node."""
        self.virtual_nodes = virtual_nodes
        self.hash_ring: Dict[int, str] = {}
        self.sorted_keys: List[int] = []
        self.nodes = set()

        for node in nodes:
            self.add_node(node)

    def _hash(self, key: str) -> int:
        """Generate a hash value for a given key."""
        return int(hashlib.md5(key.encode()).hexdigest(), 16)

    def add_node(self, node: str) -> None:
        """Add a new Redis node with virtual nodes."""
        if node in self.nodes:
            return  # Avoid duplicates
        self.nodes.add(node)

        for i in range(self.virtual_nodes):
            virtual_node_key = f"{node}#{i}"
            hash_value = self._hash(virtual_node_key)
            self.hash_ring[hash_value] = node
            self.sorted_keys.append(hash_value)

        self.sorted_keys.sort()

    def remove_node(self, node: str) -> None:
        """Remove a node and its virtual nodes from the hash ring."""
        if node not in self.nodes:
            return
        self.nodes.remove(node)

        to_remove = [key for key, n in self.hash_ring.items() if n == node]
        for key in to_remove:
            del self.hash_ring[key]
            self.sorted_keys.remove(key)

    def get_node(self, key: str) -> str:
        """Find the responsible Redis node for a given key."""
        if not self.sorted_keys:
            raise ValueError("No nodes available in the hash ring.")
        key_hash = self._hash(key)
        index = bisect(self.sorted_keys, key_hash)
        if index == len(self.sorted_keys):
            index = 0
        return self.hash_ring[self.sorted_keys[index]]