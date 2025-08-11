import json
import os
import threading
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime


class SSTableEntry:
    """Represents a single entry in an SSTable"""
    
    def __init__(self, key: str, value: Any, timestamp: datetime = None, deleted: bool = False):
        self.key = key
        self.value = value
        self.timestamp = timestamp or datetime.now()
        self.deleted = deleted
    
    def to_dict(self) -> Dict:
        return {
            'key': self.key,
            'value': self.value,
            'timestamp': self.timestamp.isoformat(),
            'deleted': self.deleted
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'SSTableEntry':
        return cls(
            data['key'],
            data['value'],
            datetime.fromisoformat(data['timestamp']),
            data.get('deleted', False)
        )


class SSTable:
    """
    Sorted String Table implementation.
    Stores key-value pairs in sorted order for efficient range queries and merging.
    """
    
    MAX_SIZE = 30  # Maximum number of entries per SSTable
    
    def __init__(self, table_id: str, file_path: str = None):
        self.table_id = table_id
        self.file_path = file_path or f"sstable_{table_id}.sst"
        self.entries: List[SSTableEntry] = []
        self.lock = threading.RLock()
        self._load_from_file()
    
    def _load_from_file(self):
        """Load SSTable from file if it exists"""
        if os.path.exists(self.file_path):
            try:
                with open(self.file_path, 'r') as f:
                    data = json.load(f)
                    self.entries = [SSTableEntry.from_dict(entry_data) for entry_data in data.get('entries', [])]
                    self._sort_entries()
            except (json.JSONDecodeError, IOError):
                self.entries = []
    
    def _save_to_file(self):
        """Save SSTable to file"""
        with self.lock:
            data = {
                'table_id': self.table_id,
                'entries': [entry.to_dict() for entry in self.entries],
                'created_at': datetime.now().isoformat()
            }
            
            temp_file = self.file_path + '.tmp'
            try:
                with open(temp_file, 'w') as f:
                    json.dump(data, f, indent=2)
                    f.flush()
                    os.fsync(f.fileno())
                
                # Atomic replace
                os.replace(temp_file, self.file_path)
            except IOError:
                if os.path.exists(temp_file):
                    os.remove(temp_file)
                raise
    
    def _sort_entries(self):
        """Sort entries by key"""
        self.entries.sort(key=lambda x: x.key)
    
    def _binary_search(self, key: str) -> Tuple[int, bool]:
        """
        Binary search for key position.
        Returns (index, found) where index is the position and found indicates if key exists.
        """
        left, right = 0, len(self.entries)
        
        while left < right:
            mid = (left + right) // 2
            if self.entries[mid].key < key:
                left = mid + 1
            elif self.entries[mid].key > key:
                right = mid
            else:
                return mid, True
        
        return left, False
    
    def put(self, key: str, value: Any, timestamp: datetime = None) -> bool:
        """
        Add or update a key-value pair in the SSTable.
        Returns True if successful, False if SSTable is full and key doesn't exist.
        """
        with self.lock:
            timestamp = timestamp or datetime.now()
            index, found = self._binary_search(key)
            
            if found:
                # Update existing entry
                self.entries[index] = SSTableEntry(key, value, timestamp, False)
            else:
                # Add new entry
                if len(self.entries) >= self.MAX_SIZE:
                    return False  # SSTable is full
                
                self.entries.insert(index, SSTableEntry(key, value, timestamp, False))
            
            self._save_to_file()
            return True
    
    def get(self, key: str) -> Optional[Any]:
        """Get value by key. Returns None if key doesn't exist or is deleted."""
        with self.lock:
            index, found = self._binary_search(key)
            if found and not self.entries[index].deleted:
                return self.entries[index].value
            return None
    
    def delete(self, key: str, timestamp: datetime = None) -> bool:
        """
        Mark a key as deleted (tombstone).
        Returns True if key was found, False otherwise.
        """
        with self.lock:
            timestamp = timestamp or datetime.now()
            index, found = self._binary_search(key)
            
            if found:
                self.entries[index] = SSTableEntry(key, None, timestamp, True)
                self._save_to_file()
                return True
            else:
                # Add tombstone even if key doesn't exist to handle deletions
                # from other SSTables during compaction
                if len(self.entries) < self.MAX_SIZE:
                    self.entries.insert(index, SSTableEntry(key, None, timestamp, True))
                    self._save_to_file()
                return False
    
    def get_all_entries(self) -> List[SSTableEntry]:
        """Get all entries in the SSTable"""
        with self.lock:
            return self.entries.copy()
    
    def get_range(self, start_key: str = None, end_key: str = None) -> List[SSTableEntry]:
        """Get entries in a key range [start_key, end_key)"""
        with self.lock:
            result = []
            
            for entry in self.entries:
                if start_key and entry.key < start_key:
                    continue
                if end_key and entry.key >= end_key:
                    break
                result.append(entry)
            
            return result
    
    def is_full(self) -> bool:
        """Check if SSTable has reached maximum capacity"""
        return len(self.entries) >= self.MAX_SIZE
    
    def is_empty(self) -> bool:
        """Check if SSTable is empty"""
        return len(self.entries) == 0
    
    def size(self) -> int:
        """Get number of entries in SSTable"""
        return len(self.entries)
    
    def get_stats(self) -> Dict:
        """Get SSTable statistics"""
        with self.lock:
            active_entries = sum(1 for e in self.entries if not e.deleted)
            deleted_entries = sum(1 for e in self.entries if e.deleted)
            
            return {
                'table_id': self.table_id,
                'total_entries': len(self.entries),
                'active_entries': active_entries,
                'deleted_entries': deleted_entries,
                'is_full': self.is_full(),
                'file_size': os.path.getsize(self.file_path) if os.path.exists(self.file_path) else 0
            }
    
    def compact(self) -> 'SSTable':
        """
        Create a compacted version of this SSTable with deleted entries removed.
        Returns a new SSTable with only active entries.
        """
        with self.lock:
            compacted_table = SSTable(f"{self.table_id}_compacted")
            
            for entry in self.entries:
                if not entry.deleted:
                    compacted_table.entries.append(entry)
            
            compacted_table._save_to_file()
            return compacted_table
    
    def delete_file(self):
        """Delete the SSTable file from disk"""
        if os.path.exists(self.file_path):
            os.remove(self.file_path)


class SSTableManager:
    """Manages multiple SSTables and handles merging operations"""
    
    def __init__(self, data_dir: str = "data"):
        self.data_dir = data_dir
        self.sstables: List[SSTable] = []
        self.table_counter = 0
        self.lock = threading.RLock()
        
        # Create data directory if it doesn't exist
        os.makedirs(data_dir, exist_ok=True)
        self._load_existing_tables()
    
    def _load_existing_tables(self):
        """Load existing SSTables from data directory"""
        for filename in os.listdir(self.data_dir):
            if filename.endswith('.sst'):
                table_id = filename[:-4]  # Remove .sst extension
                file_path = os.path.join(self.data_dir, filename)
                sstable = SSTable(table_id, file_path)
                if not sstable.is_empty():
                    self.sstables.append(sstable)
                    
                    # Update counter to avoid ID conflicts
                    try:
                        table_num = int(table_id.split('_')[-1])
                        self.table_counter = max(self.table_counter, table_num)
                    except (ValueError, IndexError):
                        pass
    
    def create_sstable(self) -> SSTable:
        """Create a new SSTable"""
        with self.lock:
            self.table_counter += 1
            table_id = f"table_{self.table_counter}"
            file_path = os.path.join(self.data_dir, f"{table_id}.sst")
            
            sstable = SSTable(table_id, file_path)
            self.sstables.append(sstable)
            return sstable
    
    def merge_sstables(self, tables: List[SSTable]) -> SSTable:
        """
        Merge multiple SSTables into a single SSTable.
        Handles duplicate keys by keeping the most recent entry.
        """
        if not tables:
            return None
        
        with self.lock:
            # Create new merged table
            merged_table = self.create_sstable()
            
            # Collect all entries from all tables
            all_entries = []
            for table in tables:
                all_entries.extend(table.get_all_entries())
            
            # Sort by key, then by timestamp (most recent last)
            all_entries.sort(key=lambda x: (x.key, x.timestamp))
            
            # Keep only the most recent entry for each key
            merged_entries = {}
            for entry in all_entries:
                if (entry.key not in merged_entries or 
                    entry.timestamp > merged_entries[entry.key].timestamp):
                    merged_entries[entry.key] = entry
            
            # Add non-deleted entries to merged table
            for key in sorted(merged_entries.keys()):
                entry = merged_entries[key]
                if not entry.deleted:
                    merged_table.entries.append(entry)
            
            merged_table._save_to_file()
            
            # Remove old tables
            for table in tables:
                if table in self.sstables:
                    self.sstables.remove(table)
                    table.delete_file()
            
            return merged_table
    
    def get_sstables(self) -> List[SSTable]:
        """Get all SSTables"""
        with self.lock:
            return self.sstables.copy()
    
    def cleanup_empty_tables(self):
        """Remove empty SSTables"""
        with self.lock:
            empty_tables = [table for table in self.sstables if table.is_empty()]
            for table in empty_tables:
                self.sstables.remove(table)
                table.delete_file()
