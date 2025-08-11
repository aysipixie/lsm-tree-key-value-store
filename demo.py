#!/usr/bin/env python3
"""
Demo script for LSM Key-Value Store
This script demonstrates the key features and functionality of the system.
"""

from kv_store import create_kv_store
import json
import time

def print_separator(title):
    """Print a formatted separator with title"""
    print("\n" + "="*60)
    print(f" {title} ".center(60, "="))
    print("="*60)

def print_stats(kv_store):
    """Print system statistics"""
    stats = kv_store.get_stats()
    print(f"📊 System Stats:")
    print(f"   • Total Keys: {stats['total_active_keys']}")
    print(f"   • Memtable: {stats['memtable']['size']}/{stats['memtable']['max_size']} entries")
    print(f"   • SSTables: {stats['sstables']['count']} tables")
    print(f"   • WAL Entries: {stats['wal']['total_entries']}")
    if stats['memtable']['is_full']:
        print("   ⚠️  Memtable is FULL - will flush on next write")
    if stats['sstables']['count'] >= stats['compaction_threshold']:
        print("   ⚠️  Compaction threshold reached - compaction needed")

def demo_basic_operations():
    """Demonstrate basic CRUD operations"""
    print_separator("BASIC CRUD OPERATIONS")
    
    # Create a new key-value store
    kv_store = create_kv_store(data_dir="demo_data", wal_file="demo_wal.log")
    print("✅ Key-Value Store initialized")
    
    # CREATE operations
    print("\n🔹 CREATE Operations:")
    users = [
        {"id": "user_001", "name": "Alice Johnson", "role": "Developer", "active": True},
        {"id": "user_002", "name": "Bob Smith", "role": "Manager", "active": True},
        {"id": "user_003", "name": "Carol Davis", "role": "Designer", "active": False}
    ]
    
    for user in users:
        success = kv_store.put(f"user:{user['id']}", user)
        print(f"   ✓ Created {user['name']} ({user['role']})")
    
    print_stats(kv_store)
    
    # READ operations
    print("\n🔹 READ Operations:")
    for i in range(1, 4):
        user_key = f"user:user_00{i}"
        user_data = kv_store.get(user_key)
        if user_data:
            status = "🟢 Active" if user_data['active'] else "🔴 Inactive"
            print(f"   📖 {user_key}: {user_data['name']} - {status}")
        else:
            print(f"   ❌ {user_key}: Not found")
    
    # UPDATE operations
    print("\n🔹 UPDATE Operations:")
    kv_store.put("user:user_003", {
        "id": "user_003", 
        "name": "Carol Davis-Wilson", 
        "role": "Senior Designer", 
        "active": True
    })
    print("   ✓ Updated Carol's info - promoted and activated")
    
    updated_user = kv_store.get("user:user_003")
    print(f"   📖 Updated data: {updated_user['name']} - {updated_user['role']}")
    
    # DELETE operations
    print("\n🔹 DELETE Operations:")
    deleted = kv_store.delete("user:user_002")
    print(f"   {'✓' if deleted else '❌'} Deleted user:user_002 (Bob Smith)")
    
    print_stats(kv_store)
    return kv_store

def demo_batch_operations(kv_store):
    """Demonstrate batch operations"""
    print_separator("BATCH OPERATIONS")
    
    # Batch PUT
    products = {
        "product:laptop_001": {
            "name": "Gaming Laptop Pro",
            "category": "Electronics",
            "price": 1299.99,
            "stock": 15
        },
        "product:mouse_001": {
            "name": "Wireless Gaming Mouse",
            "category": "Electronics", 
            "price": 79.99,
            "stock": 45
        },
        "product:keyboard_001": {
            "name": "Mechanical Keyboard RGB",
            "category": "Electronics",
            "price": 129.99,
            "stock": 23
        },
        "product:monitor_001": {
            "name": "4K Ultra-wide Monitor",
            "category": "Electronics",
            "price": 599.99,
            "stock": 8
        }
    }
    
    print("🔹 Batch PUT Operations:")
    results = kv_store.batch_put(products)
    successful = sum(results.values())
    print(f"   ✓ Successfully added {successful}/{len(products)} products")
    
    # Batch GET
    print("\n🔹 Batch GET Operations:")
    product_keys = list(products.keys())
    batch_results = kv_store.batch_get(product_keys)
    
    print("   📖 Retrieved products:")
    for key, value in batch_results.items():
        if value:
            print(f"      • {value['name']}: ${value['price']} ({value['stock']} in stock)")
    
    print_stats(kv_store)
    return kv_store

def demo_range_queries(kv_store):
    """Demonstrate range queries"""
    print_separator("RANGE QUERIES")
    
    # Add some configuration data
    configs = {
        "config:app_name": "LSM Demo Application",
        "config:version": "1.0.0",
        "config:debug": True,
        "config:max_connections": 1000,
        "config:timeout": 30
    }
    
    for key, value in configs.items():
        kv_store.put(key, value)
    
    print("🔹 Added configuration data")
    
    # Range queries
    print("\n🔹 Range Query Examples:")
    
    # Get all users
    users = kv_store.get_range("user:", "user;")  # Using ; as it comes after : in ASCII
    print(f"   👥 Users ({len(users)} found):")
    for key, user in users.items():
        print(f"      • {key}: {user['name']}")
    
    # Get all products
    products = kv_store.get_range("product:", "product;")
    print(f"\n   🛍️  Products ({len(products)} found):")
    for key, product in products.items():
        print(f"      • {product['name']}: ${product['price']}")
    
    # Get all configs
    configs_result = kv_store.get_range("config:", "config;")
    print(f"\n   ⚙️  Configuration ({len(configs_result)} found):")
    for key, value in configs_result.items():
        print(f"      • {key.replace('config:', '')}: {value}")
    
    print_stats(kv_store)
    return kv_store

def demo_lsm_features(kv_store):
    """Demonstrate LSM Tree specific features"""
    print_separator("LSM TREE FEATURES")
    
    print("🔹 Filling memtable to demonstrate flushing...")
    
    # Add enough data to trigger memtable flush
    test_data = {}
    for i in range(25):  # Close to memtable limit
        key = f"test:bulk_data_{i:03d}"
        value = {
            "id": i,
            "data": f"Test data item {i}",
            "timestamp": time.time(),
            "metadata": {"batch": "demo", "type": "test"}
        }
        test_data[key] = value
        kv_store.put(key, value)
    
    print(f"   ✓ Added {len(test_data)} test entries")
    print_stats(kv_store)
    
    # Add more data to trigger flush
    print("\n🔹 Adding more data to trigger memtable flush...")
    for i in range(10):
        key = f"test:overflow_{i:03d}"
        value = {"id": i + 1000, "data": f"Overflow data {i}"}
        kv_store.put(key, value)
        if i == 5:  # Check stats midway
            print_stats(kv_store)
    
    print("\n🔹 Final state after operations:")
    print_stats(kv_store)
    
    # Force flush if needed
    print("\n🔹 Force flushing memtable...")
    kv_store.force_flush()
    print_stats(kv_store)
    
    # Demonstrate compaction
    print("\n🔹 Demonstrating compaction...")
    kv_store.force_compaction()
    print_stats(kv_store)
    
    return kv_store

def demo_durability_features(kv_store):
    """Demonstrate durability and WAL features"""
    print_separator("DURABILITY & RECOVERY")
    
    # Show current WAL stats
    stats = kv_store.get_stats()
    print("🔹 Current WAL Statistics:")
    print(f"   • Total entries: {stats['wal']['total_entries']}")
    print(f"   • PUT operations: {stats['wal']['put_operations']}")
    print(f"   • DELETE operations: {stats['wal']['delete_operations']}")
    print(f"   • Current sequence: {stats['wal']['current_sequence']}")
    print(f"   • File size: {stats['wal']['wal_file_size']} bytes")
    
    # Add some operations to show WAL growth
    print("\n🔹 Adding operations to demonstrate WAL logging...")
    session_data = {
        "session:12345": {"user_id": "user_001", "login_time": time.time()},
        "session:12346": {"user_id": "user_003", "login_time": time.time()},
        "session:12347": {"user_id": "user_001", "login_time": time.time()}
    }
    
    for key, value in session_data.items():
        kv_store.put(key, value)
        print(f"   ✓ Session logged: {key}")
    
    # Delete one session
    kv_store.delete("session:12346")
    print("   ✓ Session deleted: session:12346")
    
    print("\n🔹 Updated WAL Statistics:")
    stats = kv_store.get_stats()
    print(f"   • Total entries: {stats['wal']['total_entries']}")
    print(f"   • PUT operations: {stats['wal']['put_operations']}")
    print(f"   • DELETE operations: {stats['wal']['delete_operations']}")
    
    return kv_store

def demo_performance_analysis(kv_store):
    """Demonstrate performance analysis"""
    print_separator("PERFORMANCE ANALYSIS")
    
    # Get comprehensive stats
    stats = kv_store.get_stats()
    
    print("🔹 System Performance Metrics:")
    print(f"\n   📊 Data Distribution:")
    print(f"      • Total active keys: {stats['total_active_keys']}")
    print(f"      • Memtable usage: {stats['memtable']['size']}/{stats['memtable']['max_size']} ({stats['memtable']['size']/stats['memtable']['max_size']*100:.1f}%)")
    print(f"      • SSTables count: {stats['sstables']['count']}")
    print(f"      • Total SSTable entries: {stats['sstables']['total_entries']}")
    print(f"      • Active SSTable entries: {stats['sstables']['active_entries']}")
    
    if stats['sstables']['total_entries'] > 0:
        efficiency = stats['sstables']['active_entries'] / stats['sstables']['total_entries'] * 100
        print(f"      • Storage efficiency: {efficiency:.1f}%")
    
    print(f"\n   🏃 Operation Statistics:")
    print(f"      • Total WAL entries: {stats['wal']['total_entries']}")
    print(f"      • PUT operations: {stats['wal']['put_operations']}")
    print(f"      • DELETE operations: {stats['wal']['delete_operations']}")
    
    print(f"\n   💾 Storage Information:")
    print(f"      • Data directory: {stats['kv_store']['data_directory']}")
    print(f"      • WAL file: {stats['kv_store']['wal_file']}")
    print(f"      • WAL file size: {stats['wal']['wal_file_size']} bytes")
    
    # Health check
    health = kv_store.health_check()
    status_emoji = "🟢" if health['status'] == 'healthy' else "🔴"
    print(f"\n   {status_emoji} System Health: {health['status'].upper()}")
    
    for check, result in health['checks'].items():
        status_icon = "✅" if result else "❌"
        check_name = check.replace('_', ' ').title()
        print(f"      {status_icon} {check_name}")

def main():
    """Run the complete demo"""
    print("🚀 LSM Key-Value Store Demo")
    print("=" * 60)
    print("This demo showcases the complete functionality of our")
    print("Log-Structured Merge (LSM) Tree based key-value store.")
    
    try:
        # Clean start
        import os
        import shutil
        if os.path.exists("demo_data"):
            shutil.rmtree("demo_data")
        if os.path.exists("demo_wal.log"):
            os.remove("demo_wal.log")
        
        # Run demonstrations
        kv_store = demo_basic_operations()
        kv_store = demo_batch_operations(kv_store)
        kv_store = demo_range_queries(kv_store)
        kv_store = demo_lsm_features(kv_store)
        kv_store = demo_durability_features(kv_store)
        demo_performance_analysis(kv_store)
        
        print_separator("DEMO COMPLETED")
        print("✅ All demonstrations completed successfully!")
        print("\n🌐 To see the web interface:")
        print("   1. Run: python3 web_interface.py")
        print("   2. Open: http://localhost:5000")
        print("\n🧪 To run tests:")
        print("   python3 test_kv_store.py")
        
        print(f"\n📁 Demo files created:")
        print(f"   • Data directory: demo_data/")
        print(f"   • WAL file: demo_wal.log")
        print(f"   • Total keys in store: {len(kv_store.get_all_keys())}")
        
    except KeyboardInterrupt:
        print("\n\n⚡ Demo interrupted by user")
    except Exception as e:
        print(f"\n❌ Error during demo: {e}")
        raise

if __name__ == "__main__":
    main()
