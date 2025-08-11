import json
import os
import threading
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple


class WALOperation(Enum):
    """Types of operations that can be logged in WAL"""
    PUT = "PUT"
    DELETE = "DELETE"


class WALEntry:
    """Represents a single entry in the Write-Ahead Log"""
    
    def __init__(self, operation: WALOperation, key: str, value: Any = None, timestamp: datetime = None):
        self.operation = operation
        self.key = key
        self.value = value
        self.timestamp = timestamp or datetime.now()
        self.sequence_number = None
    
    def to_dict(self) -> Dict:
        return {
            'operation': self.operation.value,
            'key': self.key,
            'value': self.value,
            'timestamp': self.timestamp.isoformat(),
            'sequence_number': self.sequence_number
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'WALEntry':
        entry = cls(
            WALOperation(data['operation']),
            data['key'],
            data['value'],
            datetime.fromisoformat(data['timestamp'])
        )
        entry.sequence_number = data['sequence_number']
        return entry


class WriteAheadLog:
    """
    Write-Ahead Log implementation for ensuring data durability.
    All operations are logged before being applied to the main data structure.
    """
    
    def __init__(self, wal_file: str = "wal.log"):
        self.wal_file = wal_file
        self.sequence_counter = 0
        self.lock = threading.Lock()
        self._initialize_wal()
    
    def _initialize_wal(self):
        """Initialize WAL file and recover sequence counter"""
        if os.path.exists(self.wal_file):
            # Recover sequence counter from existing WAL
            try:
                with open(self.wal_file, 'r') as f:
                    lines = f.readlines()
                    if lines:
                        last_line = lines[-1].strip()
                        if last_line:
                            last_entry = json.loads(last_line)
                            self.sequence_counter = last_entry.get('sequence_number', 0)
            except (json.JSONDecodeError, IOError):
                self.sequence_counter = 0
        else:
            # Create new WAL file
            open(self.wal_file, 'a').close()
    
    def log_operation(self, operation: WALOperation, key: str, value: Any = None) -> int:
        """
        Log an operation to the WAL before it's applied to the data structure.
        Returns the sequence number of the logged operation.
        """
        with self.lock:
            self.sequence_counter += 1
            entry = WALEntry(operation, key, value)
            entry.sequence_number = self.sequence_counter
            
            # Write to WAL file
            with open(self.wal_file, 'a') as f:
                f.write(json.dumps(entry.to_dict()) + '\n')
                f.flush()  # Ensure data is written to disk immediately
                os.fsync(f.fileno())  # Force OS to write to disk
            
            return self.sequence_counter
    
    def get_all_entries(self) -> List[WALEntry]:
        """Retrieve all entries from the WAL for recovery purposes"""
        entries = []
        if not os.path.exists(self.wal_file):
            return entries
        
        with open(self.wal_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        entry_data = json.loads(line)
                        entries.append(WALEntry.from_dict(entry_data))
                    except json.JSONDecodeError:
                        continue  # Skip corrupted entries
        
        return entries
    
    def get_entries_after_sequence(self, sequence_number: int) -> List[WALEntry]:
        """Get all entries after a specific sequence number for incremental recovery"""
        all_entries = self.get_all_entries()
        return [entry for entry in all_entries if entry.sequence_number > sequence_number]
    
    def truncate_before_sequence(self, sequence_number: int):
        """Remove WAL entries before a specific sequence number (used after compaction)"""
        with self.lock:
            entries = self.get_all_entries()
            remaining_entries = [entry for entry in entries if entry.sequence_number >= sequence_number]
            
            # Rewrite WAL file with remaining entries
            with open(self.wal_file, 'w') as f:
                for entry in remaining_entries:
                    f.write(json.dumps(entry.to_dict()) + '\n')
    
    def clear(self):
        """Clear the WAL file (use with caution)"""
        with self.lock:
            open(self.wal_file, 'w').close()
            self.sequence_counter = 0
    
    def get_stats(self) -> Dict:
        """Get WAL statistics"""
        entries = self.get_all_entries()
        put_count = sum(1 for e in entries if e.operation == WALOperation.PUT)
        delete_count = sum(1 for e in entries if e.operation == WALOperation.DELETE)
        
        return {
            'total_entries': len(entries),
            'put_operations': put_count,
            'delete_operations': delete_count,
            'current_sequence': self.sequence_counter,
            'wal_file_size': os.path.getsize(self.wal_file) if os.path.exists(self.wal_file) else 0
        }
