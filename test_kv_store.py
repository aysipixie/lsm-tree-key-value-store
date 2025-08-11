import unittest
import os
import shutil
import tempfile
from datetime import datetime
from typing import Dict, Any

from kv_store import KeyValueStore, create_kv_store
from lsm_tree import LSMTree, Memtable
from sstable import SSTable, SSTableManager
from wal import WriteAheadLog, WALOperation


class TestWriteAheadLog(unittest.TestCase):
    """Test Write-Ahead Log functionality"""
    
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.wal_file = os.path.join(self.test_dir, "test_wal.log")
        self.wal = WriteAheadLog(self.wal_file)
    
    def tearDown(self):
        shutil.rmtree(self.test_dir)
    
    def test_log_operation(self):
        """Test logging operations to WAL"""
        seq1 = self.wal.log_operation(WALOperation.PUT, "key1", "value1")
        seq2 = self.wal.log_operation(WALOperation.DELETE, "key2")
        
        self.assertEqual(seq1, 1)
        self.assertEqual(seq2, 2)
        
        entries = self.wal.get_all_entries()
        self.assertEqual(len(entries), 2)
        self.assertEqual(entries[0].operation, WALOperation.PUT)
        self.assertEqual(entries[0].key, "key1")
        self.assertEqual(entries[0].value, "value1")
        self.assertEqual(entries[1].operation, WALOperation.DELETE)
        self.assertEqual(entries[1].key, "key2")
    
    def test_wal_recovery(self):
        """Test WAL recovery after restart"""
        # Log some operations
        self.wal.log_operation(WALOperation.PUT, "key1", "value1")
        self.wal.log_operation(WALOperation.PUT, "key2", "value2")
        
        # Create new WAL instance (simulating restart)
        new_wal = WriteAheadLog(self.wal_file)
        
        entries = new_wal.get_all_entries()
        self.assertEqual(len(entries), 2)
        self.assertEqual(new_wal.sequence_counter, 2)
    
    def test_wal_truncation(self):
        """Test WAL truncation functionality"""
        # Log several operations
        for i in range(5):
            self.wal.log_operation(WALOperation.PUT, f"key{i}", f"value{i}")
        
        # Truncate before sequence 3
        self.wal.truncate_before_sequence(3)
        
        entries = self.wal.get_all_entries()
        self.assertEqual(len(entries), 3)  # Should keep sequences 3, 4, 5
        self.assertEqual(entries[0].sequence_number, 3)


class TestSSTable(unittest.TestCase):
    """Test SSTable functionality"""
    
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.sstable_file = os.path.join(self.test_dir, "test_table.sst")
        self.sstable = SSTable("test_table", self.sstable_file)
    
    def tearDown(self):
        shutil.rmtree(self.test_dir)
    
    def test_put_and_get(self):
        """Test basic put and get operations"""
        self.assertTrue(self.sstable.put("key1", "value1"))
        self.assertTrue(self.sstable.put("key2", "value2"))
        
        self.assertEqual(self.sstable.get("key1"), "value1")
        self.assertEqual(self.sstable.get("key2"), "value2")
        self.assertIsNone(self.sstable.get("nonexistent"))
    
    def test_update(self):
        """Test updating existing keys"""
        self.sstable.put("key1", "value1")
        self.sstable.put("key1", "updated_value")
        
        self.assertEqual(self.sstable.get("key1"), "updated_value")
        self.assertEqual(self.sstable.size(), 1)
    
    def test_delete(self):
        """Test deletion (tombstone) functionality"""
        self.sstable.put("key1", "value1")
        self.assertTrue(self.sstable.delete("key1"))
        
        self.assertIsNone(self.sstable.get("key1"))
    
    def test_max_size_limit(self):
        """Test SSTable maximum size limit"""
        # Fill SSTable to maximum capacity
        for i in range(SSTable.MAX_SIZE):
            self.assertTrue(self.sstable.put(f"key{i}", f"value{i}"))
        
        # Try to add one more - should fail
        self.assertFalse(self.sstable.put("overflow_key", "overflow_value"))
        self.assertTrue(self.sstable.is_full())
    
    def test_sorted_order(self):
        """Test that entries are maintained in sorted order"""
        keys = ["zebra", "apple", "banana", "cherry"]
        for key in keys:
            self.sstable.put(key, f"value_{key}")
        
        entries = self.sstable.get_all_entries()
        sorted_keys = [entry.key for entry in entries]
        
        self.assertEqual(sorted_keys, sorted(keys))
    
    def test_range_query(self):
        """Test range query functionality"""
        keys = ["a", "b", "c", "d", "e"]
        for key in keys:
            self.sstable.put(key, f"value_{key}")
        
        range_entries = self.sstable.get_range("b", "d")
        range_keys = [entry.key for entry in range_entries]
        
        self.assertEqual(range_keys, ["b", "c"])  # [b, d) - excludes d
    
    def test_persistence(self):
        """Test that SSTable persists to disk"""
        self.sstable.put("key1", "value1")
        self.sstable.put("key2", "value2")
        
        # Create new SSTable instance (simulating restart)
        new_sstable = SSTable("test_table", self.sstable_file)
        
        self.assertEqual(new_sstable.get("key1"), "value1")
        self.assertEqual(new_sstable.get("key2"), "value2")
        self.assertEqual(new_sstable.size(), 2)


class TestSSTableManager(unittest.TestCase):
    """Test SSTable Manager functionality"""
    
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.manager = SSTableManager(self.test_dir)
    
    def tearDown(self):
        shutil.rmtree(self.test_dir)
    
    def test_create_sstable(self):
        """Test creating new SSTables"""
        table1 = self.manager.create_sstable()
        table2 = self.manager.create_sstable()
        
        self.assertNotEqual(table1.table_id, table2.table_id)
        self.assertEqual(len(self.manager.get_sstables()), 2)
    
    def test_merge_sstables(self):
        """Test merging multiple SSTables"""
        table1 = self.manager.create_sstable()
        table2 = self.manager.create_sstable()
        
        # Add data to tables
        table1.put("key1", "value1_table1")
        table1.put("key2", "value2_table1")
        
        table2.put("key1", "value1_table2")  # Duplicate key, newer timestamp
        table2.put("key3", "value3_table2")
        
        # Merge tables
        merged = self.manager.merge_sstables([table1, table2])
        
        self.assertIsNotNone(merged)
        # Should keep the newer value for key1
        # Exact value depends on timestamp comparison in implementation


class TestMemtable(unittest.TestCase):
    """Test Memtable functionality"""
    
    def setUp(self):
        self.memtable = Memtable()
    
    def test_put_and_get(self):
        """Test basic put and get operations"""
        self.memtable.put("key1", "value1")
        self.memtable.put("key2", "value2")
        
        self.assertEqual(self.memtable.get("key1"), "value1")
        self.assertEqual(self.memtable.get("key2"), "value2")
        self.assertIsNone(self.memtable.get("nonexistent"))
    
    def test_update(self):
        """Test updating existing keys"""
        self.memtable.put("key1", "value1")
        self.memtable.put("key1", "updated_value")
        
        self.assertEqual(self.memtable.get("key1"), "updated_value")
        self.assertEqual(self.memtable.size(), 1)
    
    def test_delete(self):
        """Test deletion functionality"""
        self.memtable.put("key1", "value1")
        self.memtable.delete("key1")
        
        self.assertIsNone(self.memtable.get("key1"))
    
    def test_max_size(self):
        """Test memtable maximum size"""
        # Fill memtable to capacity
        for i in range(Memtable.MAX_SIZE):
            self.memtable.put(f"key{i}", f"value{i}")
        
        self.assertTrue(self.memtable.is_full())
        self.assertEqual(self.memtable.size(), Memtable.MAX_SIZE)
    
    def test_sorted_entries(self):
        """Test getting sorted entries"""
        keys = ["zebra", "apple", "banana"]
        for key in keys:
            self.memtable.put(key, f"value_{key}")
        
        entries = self.memtable.get_sorted_entries()
        sorted_keys = [entry.key for entry in entries]
        
        self.assertEqual(sorted_keys, sorted(keys))


class TestLSMTree(unittest.TestCase):
    """Test LSM Tree functionality"""
    
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.wal_file = os.path.join(self.test_dir, "lsm_wal.log")
        self.lsm_tree = LSMTree(self.test_dir, self.wal_file)
    
    def tearDown(self):
        shutil.rmtree(self.test_dir)
    
    def test_put_and_get(self):
        """Test basic put and get operations"""
        self.assertTrue(self.lsm_tree.put("key1", "value1"))
        self.assertTrue(self.lsm_tree.put("key2", "value2"))
        
        self.assertEqual(self.lsm_tree.get("key1"), "value1")
        self.assertEqual(self.lsm_tree.get("key2"), "value2")
        self.assertIsNone(self.lsm_tree.get("nonexistent"))
    
    def test_delete(self):
        """Test deletion functionality"""
        self.lsm_tree.put("key1", "value1")
        self.assertTrue(self.lsm_tree.delete("key1"))
        
        self.assertIsNone(self.lsm_tree.get("key1"))
        self.assertFalse(self.lsm_tree.delete("nonexistent"))
    
    def test_memtable_flush(self):
        """Test memtable flushing to SSTable"""
        initial_sstables = len(self.lsm_tree.sstable_manager.get_sstables())
        
        # Fill memtable to trigger flush
        for i in range(Memtable.MAX_SIZE + 1):
            self.lsm_tree.put(f"key{i}", f"value{i}")
        
        # Should have created at least one SSTable
        final_sstables = len(self.lsm_tree.sstable_manager.get_sstables())
        self.assertGreater(final_sstables, initial_sstables)
        
        # Memtable should not be full anymore
        self.assertFalse(self.lsm_tree.memtable.is_full())
    
    def test_compaction_trigger(self):
        """Test that compaction is triggered when threshold is reached"""
        # Set low compaction threshold for testing
        self.lsm_tree.compaction_threshold = 2
        
        # Create enough data to trigger multiple flushes
        for i in range(Memtable.MAX_SIZE * 3):
            self.lsm_tree.put(f"key{i:04d}", f"value{i}")
        
        # Should have triggered compaction
        sstables = self.lsm_tree.sstable_manager.get_sstables()
        self.assertLessEqual(len(sstables), 3)  # Compaction should have merged some
    
    def test_get_all_keys(self):
        """Test getting all keys from LSM tree"""
        keys = ["apple", "banana", "cherry"]
        for key in keys:
            self.lsm_tree.put(key, f"value_{key}")
        
        all_keys = self.lsm_tree.get_all_keys()
        self.assertEqual(set(all_keys), set(keys))
    
    def test_range_query(self):
        """Test range query across memtable and SSTables"""
        keys = ["a", "b", "c", "d", "e"]
        for key in keys:
            self.lsm_tree.put(key, f"value_{key}")
        
        # Force flush to create SSTables
        self.lsm_tree.force_flush()
        
        # Add more keys to memtable
        self.lsm_tree.put("f", "value_f")
        self.lsm_tree.put("g", "value_g")
        
        range_result = self.lsm_tree.get_range("b", "f")
        expected_keys = ["b", "c", "d", "e"]
        
        self.assertEqual(set(range_result.keys()), set(expected_keys))


class TestKeyValueStore(unittest.TestCase):
    """Test Key-Value Store functionality"""
    
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.kv_store = create_kv_store(
            data_dir=os.path.join(self.test_dir, "data"),
            wal_file=os.path.join(self.test_dir, "wal.log")
        )
    
    def tearDown(self):
        shutil.rmtree(self.test_dir)
    
    def test_crud_operations(self):
        """Test CRUD operations"""
        # Create
        self.assertTrue(self.kv_store.create("user1", {"name": "Alice", "age": 25}))
        self.assertFalse(self.kv_store.create("user1", {"name": "Bob", "age": 30}))  # Duplicate
        
        # Read
        user1 = self.kv_store.read("user1")
        self.assertEqual(user1["name"], "Alice")
        self.assertEqual(user1["age"], 25)
        self.assertIsNone(self.kv_store.read("nonexistent"))
        
        # Update
        self.assertTrue(self.kv_store.update("user1", {"name": "Alice Updated", "age": 26}))
        self.assertFalse(self.kv_store.update("nonexistent", {"name": "Test"}))
        
        updated_user = self.kv_store.read("user1")
        self.assertEqual(updated_user["name"], "Alice Updated")
        
        # Delete
        self.assertTrue(self.kv_store.delete("user1"))
        self.assertFalse(self.kv_store.delete("nonexistent"))
        self.assertIsNone(self.kv_store.read("user1"))
    
    def test_put_upsert(self):
        """Test put (upsert) functionality"""
        # Create new key
        self.assertTrue(self.kv_store.put("key1", "value1"))
        self.assertEqual(self.kv_store.get("key1"), "value1")
        
        # Update existing key
        self.assertTrue(self.kv_store.put("key1", "updated_value"))
        self.assertEqual(self.kv_store.get("key1"), "updated_value")
    
    def test_batch_operations(self):
        """Test batch operations"""
        # Batch put
        batch_data = {
            "product1": {"name": "Laptop", "price": 1200},
            "product2": {"name": "Mouse", "price": 25},
            "product3": {"name": "Keyboard", "price": 75}
        }
        results = self.kv_store.batch_put(batch_data)
        self.assertTrue(all(results.values()))
        
        # Batch get
        batch_results = self.kv_store.batch_get(["product1", "product2", "nonexistent"])
        self.assertEqual(batch_results["product1"]["name"], "Laptop")
        self.assertEqual(batch_results["product2"]["name"], "Mouse")
        self.assertIsNone(batch_results["nonexistent"])
        
        # Batch delete
        delete_results = self.kv_store.batch_delete(["product1", "nonexistent"])
        self.assertTrue(delete_results["product1"])
        self.assertFalse(delete_results["nonexistent"])
    
    def test_advanced_operations(self):
        """Test advanced operations"""
        # Setup test data
        test_data = {
            "apple": "fruit",
            "banana": "fruit", 
            "carrot": "vegetable",
            "date": "fruit"
        }
        for key, value in test_data.items():
            self.kv_store.put(key, value)
        
        # Test exists
        self.assertTrue(self.kv_store.exists("apple"))
        self.assertFalse(self.kv_store.exists("nonexistent"))
        
        # Test get_all_keys
        all_keys = self.kv_store.get_all_keys()
        self.assertEqual(set(all_keys), set(test_data.keys()))
        
        # Test get_all_items
        all_items = self.kv_store.get_all_items()
        self.assertEqual(all_items, test_data)
        
        # Test range query
        range_result = self.kv_store.get_range("banana", "date")
        expected = {"banana": "fruit", "carrot": "vegetable"}
        self.assertEqual(range_result, expected)
        
        # Test count
        self.assertEqual(self.kv_store.count(), len(test_data))
        
        # Test is_empty
        self.assertFalse(self.kv_store.is_empty())
    
    def test_dict_like_interface(self):
        """Test dictionary-like interface"""
        # Test __setitem__ and __getitem__
        self.kv_store["key1"] = "value1"
        self.assertEqual(self.kv_store["key1"], "value1")
        
        # Test __contains__
        self.assertTrue("key1" in self.kv_store)
        self.assertFalse("nonexistent" in self.kv_store)
        
        # Test __len__
        self.kv_store["key2"] = "value2"
        self.assertEqual(len(self.kv_store), 2)
        
        # Test __delitem__
        del self.kv_store["key1"]
        self.assertFalse("key1" in self.kv_store)
        
        # Test KeyError on missing key
        with self.assertRaises(KeyError):
            _ = self.kv_store["nonexistent"]
        
        with self.assertRaises(KeyError):
            del self.kv_store["nonexistent"]
    
    def test_input_validation(self):
        """Test input validation"""
        # Empty key should raise ValueError
        with self.assertRaises(ValueError):
            self.kv_store.put("", "value")
        
        with self.assertRaises(ValueError):
            self.kv_store.put("   ", "value")  # Whitespace only
        
        # Non-string key should raise ValueError
        with self.assertRaises(ValueError):
            self.kv_store.put(123, "value")
    
    def test_health_check(self):
        """Test health check functionality"""
        health = self.kv_store.health_check()
        
        self.assertEqual(health["status"], "healthy")
        self.assertTrue(health["checks"]["wal_accessible"])
        self.assertTrue(health["checks"]["data_dir_accessible"])
        self.assertTrue(health["checks"]["memtable_operational"])
        self.assertTrue(health["checks"]["sstables_accessible"])
    
    def test_statistics(self):
        """Test statistics functionality"""
        # Add some data
        for i in range(10):
            self.kv_store.put(f"key{i}", f"value{i}")
        
        stats = self.kv_store.get_stats()
        
        self.assertIn("memtable", stats)
        self.assertIn("sstables", stats)
        self.assertIn("wal", stats)
        self.assertIn("kv_store", stats)
        
        self.assertEqual(stats["total_active_keys"], 10)
        self.assertGreaterEqual(stats["wal"]["total_entries"], 10)
    
    def test_maintenance_operations(self):
        """Test maintenance operations"""
        # Add data to memtable
        for i in range(5):
            self.kv_store.put(f"key{i}", f"value{i}")
        
        initial_sstables = len(self.kv_store.lsm_tree.sstable_manager.get_sstables())
        
        # Force flush
        self.kv_store.force_flush()
        
        final_sstables = len(self.kv_store.lsm_tree.sstable_manager.get_sstables())
        self.assertGreaterEqual(final_sstables, initial_sstables)
        
        # Force compaction (may or may not do anything depending on current state)
        self.kv_store.force_compaction()
    
    def test_clear_operation(self):
        """Test clear operation"""
        # Add some data
        for i in range(5):
            self.kv_store.put(f"key{i}", f"value{i}")
        
        self.assertFalse(self.kv_store.is_empty())
        
        # Clear all data
        self.kv_store.clear()
        
        self.assertTrue(self.kv_store.is_empty())
        self.assertEqual(self.kv_store.count(), 0)


class TestFailoverScenarios(unittest.TestCase):
    """Test fail-over and recovery scenarios"""
    
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
    
    def tearDown(self):
        shutil.rmtree(self.test_dir)
    
    def test_wal_recovery(self):
        """Test recovery from WAL after simulated crash"""
        data_dir = os.path.join(self.test_dir, "data")
        wal_file = os.path.join(self.test_dir, "wal.log")
        
        # Create store and add data
        kv_store1 = KeyValueStore(data_dir, wal_file)
        kv_store1.put("key1", "value1")
        kv_store1.put("key2", "value2")
        kv_store1.delete("key1")
        kv_store1.put("key3", "value3")
        
        # Simulate crash by creating new instance
        kv_store2 = KeyValueStore(data_dir, wal_file)
        
        # Should have recovered state from WAL
        self.assertIsNone(kv_store2.get("key1"))  # Was deleted
        self.assertEqual(kv_store2.get("key2"), "value2")
        self.assertEqual(kv_store2.get("key3"), "value3")
    
    def test_sstable_persistence(self):
        """Test SSTable persistence across restarts"""
        data_dir = os.path.join(self.test_dir, "data")
        wal_file = os.path.join(self.test_dir, "wal.log")
        
        # Create store and add enough data to trigger SSTable creation
        kv_store1 = KeyValueStore(data_dir, wal_file)
        for i in range(Memtable.MAX_SIZE + 5):
            kv_store1.put(f"key{i:04d}", f"value{i}")
        
        # Force flush to ensure SSTables are created
        kv_store1.force_flush()
        
        # Simulate restart
        kv_store2 = KeyValueStore(data_dir, wal_file)
        
        # Should have all data available
        for i in range(Memtable.MAX_SIZE + 5):
            self.assertEqual(kv_store2.get(f"key{i:04d}"), f"value{i}")


if __name__ == "__main__":
    # Run all tests
    unittest.main(verbosity=2)
