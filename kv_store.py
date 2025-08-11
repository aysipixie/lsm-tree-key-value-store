import os
import threading
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime

from lsm_tree import LSMTree


class KeyValueStore:
    """
    High-performance key-value store implementing LSM Tree architecture
    with Write-Ahead Logging, SSTables, and compaction strategies.
    """
    
    def __init__(self, data_dir: str = "kv_data", wal_file: str = "kv_wal.log"):
        """
        Initialize the key-value store.
        
        Args:
            data_dir: Directory to store SSTable files
            wal_file: Path to Write-Ahead Log file
        """
        self.data_dir = data_dir
        self.wal_file = wal_file
        self.lock = threading.RLock()
        
        # Create data directory if it doesn't exist
        os.makedirs(data_dir, exist_ok=True)
        
        # Initialize LSM Tree
        self.lsm_tree = LSMTree(data_dir, wal_file)
        
        print(f"Key-Value Store initialized with data directory: {data_dir}")
        self._print_startup_stats()
    
    def _print_startup_stats(self):
        """Print startup statistics"""
        stats = self.lsm_tree.get_stats()
        print(f"Startup stats: {stats['total_active_keys']} keys, "
              f"{stats['sstables']['count']} SSTables, "
              f"{stats['wal']['total_entries']} WAL entries")
    
    # CRUD Operations
    
    def create(self, key: str, value: Any) -> bool:
        """
        Create a new key-value pair.
        Returns True if successful, False if key already exists.
        """
        if not isinstance(key, str) or not key.strip():
            raise ValueError("Key must be a non-empty string")
        
        with self.lock:
            # Check if key already exists
            if self.lsm_tree.get(key) is not None:
                return False
            
            # Create the key-value pair
            return self.lsm_tree.put(key, value)
    
    def read(self, key: str) -> Optional[Any]:
        """
        Read value by key.
        Returns the value if key exists, None otherwise.
        """
        if not isinstance(key, str) or not key.strip():
            raise ValueError("Key must be a non-empty string")
        
        return self.lsm_tree.get(key)
    
    def update(self, key: str, value: Any) -> bool:
        """
        Update an existing key-value pair.
        Returns True if key was updated, False if key doesn't exist.
        """
        if not isinstance(key, str) or not key.strip():
            raise ValueError("Key must be a non-empty string")
        
        with self.lock:
            # Check if key exists
            if self.lsm_tree.get(key) is None:
                return False
            
            # Update the key-value pair
            return self.lsm_tree.put(key, value)
    
    def delete(self, key: str) -> bool:
        """
        Delete a key-value pair.
        Returns True if key was deleted, False if key doesn't exist.
        """
        if not isinstance(key, str) or not key.strip():
            raise ValueError("Key must be a non-empty string")
        
        return self.lsm_tree.delete(key)
    
    def put(self, key: str, value: Any) -> bool:
        """
        Put (upsert) a key-value pair.
        Creates the key if it doesn't exist, updates it if it does.
        Returns True if successful.
        """
        if not isinstance(key, str) or not key.strip():
            raise ValueError("Key must be a non-empty string")
        
        return self.lsm_tree.put(key, value)
    
    def get(self, key: str) -> Optional[Any]:
        """Alias for read method"""
        return self.read(key)
    
    # Advanced Operations
    
    def exists(self, key: str) -> bool:
        """Check if a key exists in the store"""
        return self.read(key) is not None
    
    def get_all_keys(self) -> List[str]:
        """Get all keys in the store"""
        return self.lsm_tree.get_all_keys()
    
    def get_all_items(self) -> Dict[str, Any]:
        """Get all key-value pairs in the store"""
        result = {}
        for key in self.get_all_keys():
            value = self.read(key)
            if value is not None:
                result[key] = value
        return result
    
    def get_range(self, start_key: str = None, end_key: str = None) -> Dict[str, Any]:
        """
        Get all key-value pairs in a range [start_key, end_key).
        
        Args:
            start_key: Starting key (inclusive). None means start from beginning.
            end_key: Ending key (exclusive). None means go to end.
        """
        return self.lsm_tree.get_range(start_key, end_key)
    
    def count(self) -> int:
        """Get total number of key-value pairs"""
        return len(self.get_all_keys())
    
    def is_empty(self) -> bool:
        """Check if the store is empty"""
        return self.count() == 0
    
    def clear(self):
        """Clear all data from the store"""
        with self.lock:
            self.lsm_tree.clear_all_data()
            print("Key-Value Store cleared")
    
    # Maintenance Operations
    
    def force_flush(self):
        """Force flush memtable to SSTable"""
        with self.lock:
            self.lsm_tree.force_flush()
            print("Memtable flushed to SSTable")
    
    def force_compaction(self):
        """Force compaction of SSTables"""
        with self.lock:
            self.lsm_tree.force_compact()
            print("SSTable compaction completed")
    
    def get_stats(self) -> Dict:
        """Get comprehensive statistics about the key-value store"""
        stats = self.lsm_tree.get_stats()
        
        # Add KV store specific stats
        stats.update({
            'kv_store': {
                'data_directory': self.data_dir,
                'wal_file': self.wal_file,
                'total_keys': stats['total_active_keys']
            }
        })
        
        return stats
    
    def health_check(self) -> Dict[str, Any]:
        """Perform health check on the key-value store"""
        try:
            stats = self.get_stats()
            
            health_status = {
                'status': 'healthy',
                'timestamp': datetime.now().isoformat(),
                'checks': {
                    'wal_accessible': os.path.exists(self.wal_file),
                    'data_dir_accessible': os.path.exists(self.data_dir),
                    'memtable_operational': stats['memtable']['size'] >= 0,
                    'sstables_accessible': stats['sstables']['count'] >= 0
                }
            }
            
            # Check if any health check failed
            if not all(health_status['checks'].values()):
                health_status['status'] = 'unhealthy'
            
            health_status['stats'] = stats
            return health_status
            
        except Exception as e:
            return {
                'status': 'error',
                'timestamp': datetime.now().isoformat(),
                'error': str(e)
            }
    
    # Batch Operations
    
    def batch_put(self, items: Dict[str, Any]) -> Dict[str, bool]:
        """
        Put multiple key-value pairs in a batch.
        Returns a dictionary mapping each key to success status.
        """
        results = {}
        with self.lock:
            for key, value in items.items():
                try:
                    results[key] = self.put(key, value)
                except Exception as e:
                    results[key] = False
                    print(f"Error putting key '{key}': {e}")
        return results
    
    def batch_get(self, keys: List[str]) -> Dict[str, Any]:
        """
        Get multiple values by keys in a batch.
        Returns a dictionary mapping each key to its value (or None if not found).
        """
        results = {}
        for key in keys:
            try:
                results[key] = self.read(key)
            except Exception as e:
                results[key] = None
                print(f"Error getting key '{key}': {e}")
        return results
    
    def batch_delete(self, keys: List[str]) -> Dict[str, bool]:
        """
        Delete multiple keys in a batch.
        Returns a dictionary mapping each key to success status.
        """
        results = {}
        with self.lock:
            for key in keys:
                try:
                    results[key] = self.delete(key)
                except Exception as e:
                    results[key] = False
                    print(f"Error deleting key '{key}': {e}")
        return results
    
    # Context Manager Support
    
    def __enter__(self):
        """Context manager entry"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - perform cleanup if needed"""
        pass
    
    def __len__(self):
        """Return number of key-value pairs"""
        return self.count()
    
    def __contains__(self, key):
        """Support 'in' operator"""
        return self.exists(key)
    
    def __getitem__(self, key):
        """Support dict-like access: store[key]"""
        value = self.read(key)
        if value is None:
            raise KeyError(key)
        return value
    
    def __setitem__(self, key, value):
        """Support dict-like assignment: store[key] = value"""
        self.put(key, value)
    
    def __delitem__(self, key):
        """Support dict-like deletion: del store[key]"""
        if not self.delete(key):
            raise KeyError(key)


# Factory function for easy initialization
def create_kv_store(data_dir: str = "kv_data", wal_file: str = "kv_wal.log") -> KeyValueStore:
    """
    Factory function to create a new KeyValueStore instance.
    
    Args:
        data_dir: Directory to store SSTable files
        wal_file: Path to Write-Ahead Log file
    
    Returns:
        KeyValueStore instance
    """
    return KeyValueStore(data_dir, wal_file)


# Example usage and testing
if __name__ == "__main__":
    # Create a key-value store
    kv_store = create_kv_store()
    
    # CRUD operations
    print("=== CRUD Operations Demo ===")
    
    # Create
    print(f"Create 'user1': {kv_store.create('user1', {'name': 'Alice', 'age': 25})}")
    print(f"Create 'user1' again: {kv_store.create('user1', {'name': 'Bob', 'age': 30})}")  # Should fail
    
    # Read
    print(f"Read 'user1': {kv_store.read('user1')}")
    print(f"Read 'nonexistent': {kv_store.read('nonexistent')}")
    
    # Update
    print(f"Update 'user1': {kv_store.update('user1', {'name': 'Alice Updated', 'age': 26})}")
    print(f"Update 'nonexistent': {kv_store.update('nonexistent', {'name': 'Test'})}")  # Should fail
    print(f"Read 'user1' after update: {kv_store.read('user1')}")
    
    # Put (upsert)
    print(f"Put 'user2': {kv_store.put('user2', {'name': 'Charlie', 'age': 35})}")
    print(f"Put 'user1' (update): {kv_store.put('user1', {'name': 'Alice Final', 'age': 27})}")
    
    # Batch operations
    print("\\n=== Batch Operations Demo ===")
    batch_data = {
        'product1': {'name': 'Laptop', 'price': 1200},
        'product2': {'name': 'Mouse', 'price': 25},
        'product3': {'name': 'Keyboard', 'price': 75}
    }
    print(f"Batch put: {kv_store.batch_put(batch_data)}")
    print(f"Batch get: {kv_store.batch_get(['user1', 'product1', 'nonexistent'])}")
    
    # List all keys and items
    print(f"\\nAll keys: {kv_store.get_all_keys()}")
    print(f"Total count: {kv_store.count()}")
    
    # Range query
    print(f"Range query (product1 to product3): {kv_store.get_range('product1', 'product3')}")
    
    # Delete
    print(f"\\nDelete 'user2': {kv_store.delete('user2')}")
    print(f"Delete 'nonexistent': {kv_store.delete('nonexistent')}")  # Should fail
    
    # Final state
    print(f"\\nFinal keys: {kv_store.get_all_keys()}")
    
    # Statistics
    print(f"\\n=== Statistics ===")
    stats = kv_store.get_stats()
    print(f"Memtable size: {stats['memtable']['size']}")
    print(f"SSTables count: {stats['sstables']['count']}")
    print(f"WAL entries: {stats['wal']['total_entries']}")
    
    # Health check
    health = kv_store.health_check()
    print(f"\\nHealth status: {health['status']}")
    
    print("\\n=== Demo completed ===")
