import hashlib
from typing import List, Dict, Any
from bisect import bisect

class ConsistentHash:
    def __init__(self, nodes: List[str], virtual_nodes: int = 100):
        """
        Initialize the consistent hash ring
        
        Args:
            nodes: List of node identifiers (parsed from comma-separated string)
            virtual_nodes: Number of virtual nodes per physical node
        """
        
        # Initialized the hash ring with virtual nodes
        self.virtual_nodes = virtual_nodes
        self.hash_ring: Dict[int, str] = {} 
        self.sorted_keys: List[int] = []

        # For each physical node, created virtual_nodes number of virtual nodes
        for node in nodes:
            self.add_node(node)

    def _hash(self, key: str) -> int:
        """
        Generate a hash value for a given key
        
        Args:
            key: The input key to hash
            
        Returns:
            An integer hash value
        """
        return int(hashlib.md5(key.encode()).hexdigest(), 16)
    
    def add_node(self, node: str) -> None:
        """
        Add a new node to the hash ring
        
        Args:
            node: Node identifier to add
        """
        # Implement adding a new node
        for i in range(self.virtual_nodes):
            virtual_node_key = f"{node}#{i}" 
            hash_value = self._hash(virtual_node_key)
            self.hash_ring[hash_value] = node
            self.sorted_keys.append(hash_value)  

        self.sorted_keys.sort()

    def remove_node(self, node: str) -> None:
        """
        Remove a node from the hash ring
        
        Args:
            node: Node identifier to remove
        """
        # Implementing removing a node
        for i in range(self.virtual_nodes):
            virtual_node_key = f"{node}#{i}"
            hash_value = self._hash(virtual_node_key)
            
            if hash_value in self.hash_ring:
                del self.hash_ring[hash_value]
                self.sorted_keys.remove(hash_value)

    def get_node(self, key: str) -> str:
        """
        Get the node responsible for the given key
        
        Args:
            key: The key to look up
            
        Returns:
            The node responsible for the key
        """
         # Implementing node lookup
        key_hash = self._hash(key)
        index = bisect(self.sorted_keys, key_hash)  # Finding the first node after key's hash
        
        # If no such node exists, wrapping around to the first node
        if index == len(self.sorted_keys):
            index = 0
        
        return self.hash_ring[self.sorted_keys[index]]
    