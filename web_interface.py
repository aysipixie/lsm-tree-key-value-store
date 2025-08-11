from flask import Flask, render_template, request, jsonify, redirect, url_for
import json
import os
from datetime import datetime
from typing import Dict, Any

from kv_store import create_kv_store

# Create Flask app
app = Flask(__name__)
app.secret_key = 'kv_store_demo_key'

# Initialize key-value store
kv_store = create_kv_store(data_dir="web_kv_data", wal_file="web_kv_wal.log")

# Helper function to serialize data for JSON responses
def serialize_for_json(obj):
    """Convert objects to JSON serializable format"""
    if isinstance(obj, datetime):
        return obj.isoformat()
    return str(obj)

@app.route('/')
def index():
    """Main dashboard page"""
    try:
        stats = kv_store.get_stats()
        health = kv_store.health_check()
        all_keys = kv_store.get_all_keys()[:50]  # Limit to first 50 keys for display
        
        return render_template('index.html', 
                             stats=stats, 
                             health=health, 
                             keys=all_keys,
                             total_keys=len(kv_store.get_all_keys()))
    except Exception as e:
        return render_template('error.html', error=str(e))

@app.route('/api/stats')
def api_stats():
    """API endpoint for statistics"""
    try:
        stats = kv_store.get_stats()
        return jsonify({
            'success': True,
            'data': stats
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/health')
def api_health():
    """API endpoint for health check"""
    try:
        health = kv_store.health_check()
        return jsonify({
            'success': True,
            'data': health
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/keys')
def api_keys():
    """API endpoint to get all keys"""
    try:
        keys = kv_store.get_all_keys()
        return jsonify({
            'success': True,
            'data': keys,
            'count': len(keys)
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/get/<key>')
def api_get(key):
    """API endpoint to get a value by key"""
    try:
        value = kv_store.get(key)
        return jsonify({
            'success': True,
            'key': key,
            'value': value,
            'exists': value is not None
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/put', methods=['POST'])
def api_put():
    """API endpoint to put a key-value pair"""
    try:
        data = request.get_json()
        if not data or 'key' not in data or 'value' not in data:
            return jsonify({
                'success': False,
                'error': 'Missing key or value in request'
            }), 400
        
        key = data['key']
        value = data['value']
        
        # Try to parse value as JSON if it looks like JSON
        if isinstance(value, str):
            try:
                value = json.loads(value)
            except json.JSONDecodeError:
                pass  # Keep as string if not valid JSON
        
        success = kv_store.put(key, value)
        return jsonify({
            'success': success,
            'key': key,
            'value': value
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/delete/<key>', methods=['DELETE'])
def api_delete(key):
    """API endpoint to delete a key"""
    try:
        success = kv_store.delete(key)
        return jsonify({
            'success': True,
            'key': key,
            'deleted': success
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/batch_put', methods=['POST'])
def api_batch_put():
    """API endpoint for batch put operations"""
    try:
        data = request.get_json()
        if not data or 'items' not in data:
            return jsonify({
                'success': False,
                'error': 'Missing items in request'
            }), 400
        
        items = data['items']
        
        # Parse JSON values if they look like JSON
        processed_items = {}
        for key, value in items.items():
            if isinstance(value, str):
                try:
                    processed_items[key] = json.loads(value)
                except json.JSONDecodeError:
                    processed_items[key] = value
            else:
                processed_items[key] = value
        
        results = kv_store.batch_put(processed_items)
        return jsonify({
            'success': True,
            'results': results,
            'total': len(items),
            'successful': sum(results.values())
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/range')
def api_range():
    """API endpoint for range queries"""
    try:
        start_key = request.args.get('start')
        end_key = request.args.get('end')
        
        result = kv_store.get_range(start_key, end_key)
        return jsonify({
            'success': True,
            'data': result,
            'count': len(result),
            'start_key': start_key,
            'end_key': end_key
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/clear', methods=['POST'])
def api_clear():
    """API endpoint to clear all data"""
    try:
        kv_store.clear()
        return jsonify({
            'success': True,
            'message': 'All data cleared successfully'
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/flush', methods=['POST'])
def api_flush():
    """API endpoint to force flush memtable"""
    try:
        kv_store.force_flush()
        return jsonify({
            'success': True,
            'message': 'Memtable flushed successfully'
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/compact', methods=['POST'])
def api_compact():
    """API endpoint to force compaction"""
    try:
        kv_store.force_compaction()
        return jsonify({
            'success': True,
            'message': 'Compaction completed successfully'
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/data')
def data_page():
    """Data management page"""
    try:
        all_items = kv_store.get_all_items()
        return render_template('data.html', items=all_items)
    except Exception as e:
        return render_template('error.html', error=str(e))

@app.route('/operations')
def operations_page():
    """Operations page for CRUD operations"""
    return render_template('operations.html')

@app.route('/analytics')
def analytics_page():
    """Analytics and visualization page"""
    try:
        stats = kv_store.get_stats()
        return render_template('analytics.html', stats=stats)
    except Exception as e:
        return render_template('error.html', error=str(e))

@app.route('/demo')
def demo_page():
    """Demo page with sample data"""
    return render_template('demo.html')

@app.route('/api/demo/load_sample', methods=['POST'])
def api_load_sample_data():
    """Load sample data for demonstration"""
    try:
        sample_data = {
            'user_1': {'name': 'Alice Johnson', 'age': 28, 'role': 'Developer', 'email': 'alice@example.com'},
            'user_2': {'name': 'Bob Smith', 'age': 35, 'role': 'Manager', 'email': 'bob@example.com'},
            'user_3': {'name': 'Charlie Brown', 'age': 22, 'role': 'Intern', 'email': 'charlie@example.com'},
            'product_laptop': {'name': 'Gaming Laptop', 'price': 1299.99, 'category': 'Electronics', 'stock': 15},
            'product_mouse': {'name': 'Wireless Mouse', 'price': 29.99, 'category': 'Electronics', 'stock': 120},
            'product_keyboard': {'name': 'Mechanical Keyboard', 'price': 89.99, 'category': 'Electronics', 'stock': 45},
            'config_app_name': 'My Application',
            'config_version': '1.2.3',
            'config_debug': True,
            'config_max_users': 1000,
            'session_abc123': {'user_id': 'user_1', 'login_time': '2024-01-15T10:30:00', 'expires': '2024-01-15T18:30:00'},
            'session_def456': {'user_id': 'user_2', 'login_time': '2024-01-15T11:45:00', 'expires': '2024-01-15T19:45:00'},
            'cache_popular_products': ['product_laptop', 'product_mouse'],
            'metrics_daily_users': 2547,
            'metrics_total_sales': 45678.90,
            'log_error_001': {'timestamp': '2024-01-15T14:30:00', 'level': 'ERROR', 'message': 'Database connection failed'},
            'log_info_002': {'timestamp': '2024-01-15T14:35:00', 'level': 'INFO', 'message': 'Database connection restored'}
        }
        
        results = kv_store.batch_put(sample_data)
        successful = sum(results.values())
        
        return jsonify({
            'success': True,
            'message': f'Sample data loaded: {successful}/{len(sample_data)} items',
            'total': len(sample_data),
            'successful': successful,
            'results': results
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

# Error handlers
@app.errorhandler(404)
def page_not_found(e):
    return render_template('error.html', error='Page not found'), 404

@app.errorhandler(500)
def internal_server_error(e):
    return render_template('error.html', error='Internal server error'), 500

if __name__ == '__main__':
    # Ensure templates directory exists
    os.makedirs('templates', exist_ok=True)
    os.makedirs('static/css', exist_ok=True)
    os.makedirs('static/js', exist_ok=True)
    
    # Run Flask app
    print("Starting Key-Value Store Web Interface...")
    print("Access the application at: http://localhost:8888")
    app.run(debug=True, host='0.0.0.0', port=8888)
