import threading
from datetime import datetime
from typing import Any, Dict, List, Optional
from collections import OrderedDict

from wal import WriteAheadLog, WALOperation
from sstable import SSTable, SSTableManager, SSTableEntry


class Memtable:
    """
    In-memory table that stores the most recent writes.
    Implemented as a sorted dictionary for efficient operations.
    """
    
    MAX_SIZE = 30  # Maximum number of entries before flush to SSTable
    
    def __init__(self):
        self.data = OrderedDict()  # Maintains insertion order
        self.lock = threading.RLock()
        self._sort_required = False
    
    def put(self, key: str, value: Any, timestamp: datetime = None):
        """Insert or update a key-value pair"""
        with self.lock:
            timestamp = timestamp or datetime.now()
            self.data[key] = {'value': value, 'timestamp': timestamp, 'deleted': False}
            self._sort_required = True
    
    def get(self, key: str) -> Optional[Any]:
        """Get value by key"""
        with self.lock:
            entry = self.data.get(key)
            if entry and not entry['deleted']:
                return entry['value']
            return None
    
    def delete(self, key: str, timestamp: datetime = None):
        """Mark a key as deleted (tombstone)"""
        with self.lock:
            timestamp = timestamp or datetime.now()
            self.data[key] = {'value': None, 'timestamp': timestamp, 'deleted': True}
            self._sort_required = True
    
    def get_sorted_entries(self) -> List[SSTableEntry]:
        """Get all entries sorted by key"""
        with self.lock:
            entries = []
            for key in sorted(self.data.keys()):
                entry_data = self.data[key]
                entries.append(SSTableEntry(
                    key=key,
                    value=entry_data['value'],
                    timestamp=entry_data['timestamp'],
                    deleted=entry_data['deleted']
                ))
            return entries
    
    def is_full(self) -> bool:
        """Check if memtable has reached maximum capacity"""
        return len(self.data) >= self.MAX_SIZE
    
    def is_empty(self) -> bool:
        """Check if memtable is empty"""
        return len(self.data) == 0
    
    def size(self) -> int:
        """Get number of entries in memtable"""
        return len(self.data)
    
    def clear(self):
        """Clear all entries from memtable"""
        with self.lock:
            self.data.clear()
            self._sort_required = False


class LSMTree:
    """
    Log-Structured Merge Tree implementation.
    Provides efficient writes through memtable and efficient reads through SSTables.
    """
    
    def __init__(self, data_dir: str = "data", wal_file: str = "wal.log"):
        self.memtable = Memtable()
        self.wal = WriteAheadLog(wal_file)
        self.sstable_manager = SSTableManager(data_dir)
        self.lock = threading.RLock()
        self.compaction_threshold = 5  # Number of SSTables that trigger compaction
        
        # Recovery from WAL on startup
        self._recover_from_wal()
    
    def _recover_from_wal(self):
        """Recover state from Write-Ahead Log"""
        print("Recovering from WAL...")
        entries = self.wal.get_all_entries()
        
        for entry in entries:
            if entry.operation == WALOperation.PUT:
                self.memtable.put(entry.key, entry.value, entry.timestamp)
            elif entry.operation == WALOperation.DELETE:
                self.memtable.delete(entry.key, entry.timestamp)
        
        print(f"Recovered {len(entries)} operations from WAL")
        
        # If memtable is full after recovery, flush it
        if self.memtable.is_full():
            self._flush_memtable()
    
    def put(self, key: str, value: Any) -> bool:
        """
        Insert or update a key-value pair.
        Returns True if successful.
        """
        with self.lock:
            # Log operation to WAL first
            self.wal.log_operation(WALOperation.PUT, key, value)
            
            # Add to memtable
            self.memtable.put(key, value)
            
            # Check if memtable needs to be flushed
            if self.memtable.is_full():
                self._flush_memtable()
            
            # Check if compaction is needed
            self._maybe_compact()
            
            return True
    
    def get(self, key: str) -> Optional[Any]:
        """
        Retrieve value by key.
        Searches memtable first, then SSTables from newest to oldest.
        """
        with self.lock:
            # Check memtable first (most recent data)
            value = self.memtable.get(key)
            if value is not None:
                return value
            
            # Check if key is marked as deleted in memtable
            if key in self.memtable.data and self.memtable.data[key]['deleted']:
                return None
            
            # Search SSTables from newest to oldest
            sstables = self.sstable_manager.get_sstables()
            for sstable in reversed(sstables):  # Newest first
                value = sstable.get(key)
                if value is not None:
                    return value
                
                # Check if key is marked as deleted in this SSTable
                entries = sstable.get_all_entries()
                for entry in entries:
                    if entry.key == key and entry.deleted:
                        return None
            
            return None
    
    def delete(self, key: str) -> bool:
        """
        Delete a key-value pair.
        Returns True if key existed, False otherwise.
        """
        with self.lock:
            # Log operation to WAL first
            self.wal.log_operation(WALOperation.DELETE, key)
            
            # Check if key exists
            existed = self.get(key) is not None
            
            # Add tombstone to memtable
            self.memtable.delete(key)
            
            # Check if memtable needs to be flushed
            if self.memtable.is_full():
                self._flush_memtable()
            
            # Check if compaction is needed
            self._maybe_compact()
            
            return existed
    
    def _flush_memtable(self):
        """Flush memtable to a new SSTable"""
        if self.memtable.is_empty():
            return
        
        print("Flushing memtable to SSTable...")
        
        # Create new SSTable
        sstable = self.sstable_manager.create_sstable()
        
        # Transfer all entries from memtable to SSTable
        entries = self.memtable.get_sorted_entries()
        for entry in entries:
            sstable.entries.append(entry)
        
        sstable._save_to_file()
        
        # Clear memtable
        self.memtable.clear()
        
        print(f"Flushed {len(entries)} entries to SSTable {sstable.table_id}")
    
    def _maybe_compact(self):
        """Check if compaction is needed and perform it"""
        sstables = self.sstable_manager.get_sstables()
        
        if len(sstables) >= self.compaction_threshold:
            self._compact()
    
    def _compact(self):
        """Perform compaction of SSTables"""
        sstables = self.sstable_manager.get_sstables()
        
        if len(sstables) < 2:
            return
        
        print(f"Starting compaction of {len(sstables)} SSTables...")
        
        # Simple strategy: merge oldest SSTables first
        # In a production system, you might use more sophisticated strategies
        tables_to_merge = sstables[:min(3, len(sstables))]  # Merge up to 3 tables
        
        merged_table = self.sstable_manager.merge_sstables(tables_to_merge)
        
        if merged_table:
            print(f"Compacted {len(tables_to_merge)} SSTables into {merged_table.table_id}")
        
        # Clean up empty tables
        self.sstable_manager.cleanup_empty_tables()
    
    def force_flush(self):
        """Force flush memtable to SSTable"""
        with self.lock:
            self._flush_memtable()
    
    def force_compact(self):
        """Force compaction of all SSTables"""
        with self.lock:
            self._compact()
    
    def get_all_keys(self) -> List[str]:
        """Get all active keys in the LSM tree"""
        with self.lock:
            keys = set()
            
            # Get keys from memtable
            for key, entry in self.memtable.data.items():
                if not entry['deleted']:
                    keys.add(key)
                else:
                    keys.discard(key)  # Remove if marked as deleted
            
            # Get keys from SSTables (newest to oldest)
            sstables = self.sstable_manager.get_sstables()
            processed_keys = set()
            
            for sstable in reversed(sstables):
                for entry in sstable.get_all_entries():
                    if entry.key not in processed_keys:
                        if entry.deleted:
                            keys.discard(entry.key)
                        else:
                            keys.add(entry.key)
                        processed_keys.add(entry.key)
            
            return sorted(list(keys))
    
    def get_range(self, start_key: str = None, end_key: str = None) -> Dict[str, Any]:
        """Get all key-value pairs in a range"""
        with self.lock:
            result = {}
            all_keys = self.get_all_keys()
            
            for key in all_keys:
                if start_key and key < start_key:
                    continue
                if end_key and key >= end_key:
                    break
                
                value = self.get(key)
                if value is not None:
                    result[key] = value
            
            return result
    
    def get_stats(self) -> Dict:
        """Get comprehensive statistics about the LSM tree"""
        with self.lock:
            sstables = self.sstable_manager.get_sstables()
            wal_stats = self.wal.get_stats()
            
            # Calculate total entries across all SSTables
            total_sstable_entries = sum(table.size() for table in sstables)
            active_sstable_entries = sum(
                sum(1 for e in table.get_all_entries() if not e.deleted)
                for table in sstables
            )
            
            return {
                'memtable': {
                    'size': self.memtable.size(),
                    'max_size': Memtable.MAX_SIZE,
                    'is_full': self.memtable.is_full()
                },
                'sstables': {
                    'count': len(sstables),
                    'total_entries': total_sstable_entries,
                    'active_entries': active_sstable_entries,
                    'details': [table.get_stats() for table in sstables]
                },
                'wal': wal_stats,
                'total_active_keys': len(self.get_all_keys()),
                'compaction_threshold': self.compaction_threshold
            }
    
    def clear_all_data(self):
        """Clear all data (use with caution)"""
        with self.lock:
            # Clear memtable
            self.memtable.clear()
            
            # Remove all SSTables
            for sstable in self.sstable_manager.get_sstables():
                sstable.delete_file()
            self.sstable_manager.sstables.clear()
            
            # Clear WAL
            self.wal.clear()
            
            print("All data cleared")
