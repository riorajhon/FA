#!/usr/bin/env python3
"""
OSM PBF Address Processor - Optimized Version
Processes OSM PBF files to extract address data and store in MongoDB

Requirements: pip install pymongo osmium
Usage: python osm_processor.py <osm_filename>
"""

import os
import sys
import json
import time
from typing import Optional
from pymongo import MongoClient, InsertOne
import osmium
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

class OSMAddressProcessor(osmium.SimpleHandler):
    """Optimized OSM handler for address extraction with progress tracking"""
    
    __slots__ = ['batch', 'batch_size', 'processed', 'saved_batches', 'collection', 'pending_ops', 'use_file_storage', 'start_time', 'last_report', 'country_code', 'country_name', 'file_size', 'bytes_processed', 'last_progress_report']
    
    def __init__(self, collection=None, use_file_storage=False, country_code=None, country_name=None, file_size=0):
        osmium.SimpleHandler.__init__(self)
        self.batch = []
        self.batch_size = 100
        self.processed = 0
        self.saved_batches = 0
        self.collection = collection
        self.pending_ops = []
        self.use_file_storage = use_file_storage
        self.start_time = time.time()
        self.last_report = 0
        self.country_code = country_code
        self.country_name = country_name
        self.file_size = file_size
        self.bytes_processed = 0
        self.last_progress_report = 0
        
    def node(self, n):
        if 'addr:housenumber' in n.tags:
            self._add_address(f'N{n.id}')
        self._update_progress()
            
    def way(self, w):
        if 'addr:housenumber' in w.tags:
            self._add_address(f'W{w.id}')
        self._update_progress()
            
    def relation(self, r):
        if 'addr:housenumber' in r.tags:
            self._add_address(f'R{r.id}')
        self._update_progress()
    
    def _update_progress(self):
        """Update file processing progress"""
        # Estimate bytes processed (rough approximation)
        current_time = time.time()
        if current_time - self.last_progress_report >= 1.0:  # Update every 1 second
            if self.file_size > 0:
                # Rough estimation based on processing time
                elapsed = current_time - self.start_time
                if elapsed > 0:
                    # Estimate progress based on time (not perfect but gives indication)
                    estimated_progress = min(95, (elapsed / (elapsed + 30)) * 100)  # Cap at 95% until done
                    
                    file_size_mb = self.file_size / (1024 * 1024)
                    processed_mb = (estimated_progress / 100) * file_size_mb
                    
                    # Use \r to overwrite the same line
                    print(f"\rProgress: {estimated_progress:.1f}% | File: {processed_mb:.1f}MB/{file_size_mb:.1f}MB | Addresses: {self.processed:,} | Batches: {self.saved_batches:,}", end='', flush=True)
            
            self.last_progress_report = current_time
    
    def _add_address(self, element_id):
        self.batch.append(element_id)
        self.processed += 1
        
        if len(self.batch) >= self.batch_size:
            self._save_batch()
    def _save_batch(self):
        """Save batch to MongoDB or file"""
        if not self.batch:
            return
            
        if self.use_file_storage:
            self._save_to_file()
        else:
            self._queue_batch()
            
        self.saved_batches += 1
        self.batch = []
    
    def _save_to_file(self):
        """Save batch to JSON file as fallback"""
        doc = {
            'ids': ','.join(self.batch), 
            'country_code': self.country_code,
            'country_name': self.country_name,
            'status': 'origin'
        }
        
        # Create output directory if it doesn't exist
        os.makedirs('output', exist_ok=True)
        
        # Append to JSON file
        with open('output/addresses.jsonl', 'a', encoding='utf-8') as f:
            f.write(json.dumps(doc) + '\n')
    
    def _queue_batch(self):
        """Queue batch for bulk insertion"""
        doc = {
            'ids': ','.join(self.batch), 
            'country_code': self.country_code,
            'country_name': self.country_name,
            'status': 'origin'
        }
        self.pending_ops.append(InsertOne(doc))
        
        # Dynamic flush size - smaller batches as we process more data
        if self.processed < 10000:
            flush_size = 1000  # Normal size for early processing
        elif self.processed < 50000:
            flush_size = 500   # Medium size for mid processing
        else:
            flush_size = 250   # Small size for end processing
        
        # Bulk insert when reaching dynamic threshold
        if len(self.pending_ops) >= flush_size:
            self._flush_pending()
    
    def _flush_pending(self):
        """Execute pending bulk operations"""
        if self.pending_ops and self.collection is not None:
            try:
                self.collection.bulk_write(self.pending_ops, ordered=False)
                self.pending_ops = []
            except Exception as e:
                logger.error(f"MongoDB error: {e}")
                logger.info("Switching to file storage...")
                self.use_file_storage = True
                # Save pending operations to file
                for op in self.pending_ops:
                    doc = op._doc
                    with open('output/addresses.jsonl', 'a', encoding='utf-8') as f:
                        f.write(json.dumps(doc) + '\n')
                self.pending_ops = []
    
    def finalize(self):
        """Save remaining data"""
        if self.batch:
            self._save_batch()
        self._flush_pending()
        
        # Final progress update showing 100%
        if self.file_size > 0:
            file_size_mb = self.file_size / (1024 * 1024)
            print(f"\rProgress: 100.0% | File: {file_size_mb:.1f}MB/{file_size_mb:.1f}MB | Addresses: {self.processed:,} | Batches: {self.saved_batches:,}", flush=True)

def format_file_size(size_bytes):
    """Format file size in human readable format"""
    if size_bytes == 0:
        return "0B"
    size_names = ["B", "KB", "MB", "GB"]
    i = 0
    while size_bytes >= 1024 and i < len(size_names) - 1:
        size_bytes /= 1024.0
        i += 1
    return f"{size_bytes:.1f}{size_names[i]}"

def get_file_size(file_path):
    """Get file size for progress estimation"""
    return os.path.getsize(file_path)

def find_osm_file(filename):
    """Find OSM file in osm_data directory"""
    if not filename.endswith('.osm.pbf'):
        filename += '-latest.osm.pbf'
    
    file_path = os.path.join('osm_data', filename)
    return file_path if os.path.exists(file_path) else None

def try_mongodb_connection(mongodb_uri):
    """Test MongoDB connection"""
    try:
        client = MongoClient(mongodb_uri, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        collection = client.osm_addresses.address_batches
        # Test write permission
        test_doc = {'test': True}
        result = collection.insert_one(test_doc)
        collection.delete_one({'_id': result.inserted_id})
        return client, collection
    except Exception as e:
        logger.warning(f"MongoDB connection failed: {e}")
        return None, None

def process_osm_file(filename, country_code, country_name, force_json=False, mongodb_uri="mongodb://admin:fjkfjrj!20020415@localhost:27017/?authSource=admin"):
    """Process OSM file with simple progress tracking and fallback storage"""
    file_path = find_osm_file(filename)
    if not file_path:
        raise FileNotFoundError(f"File '{filename}' not found in osm_data/")
    
    # Get file size for progress tracking
    file_size = get_file_size(file_path)
    file_size_formatted = format_file_size(file_size)
    
    # Try MongoDB connection (unless forced to use JSON)
    if force_json:
        client, collection = None, None
        use_file_storage = True
        print("Forced JSON output - saving to output/addresses.jsonl")
    else:
        client, collection = try_mongodb_connection(mongodb_uri)
        use_file_storage = collection is None
        
        if use_file_storage:
            print("MongoDB not available - saving to output/addresses.jsonl")
    
    if use_file_storage:
        os.makedirs('output', exist_ok=True)
        # Clear previous output file
        with open('output/addresses.jsonl', 'w') as f:
            pass
    # else:
    #     print("✅ Connected to MongoDB")
    
    processor = OSMAddressProcessor(collection, use_file_storage, country_code, country_name, file_size)
    
    # print("🚀 Starting processing...")
    
    try:
        processor.apply_file(file_path)
        processor.finalize()
        
        elapsed = time.time() - processor.start_time
        rate = processor.processed / elapsed if elapsed > 0 else 0
        
        # print(f"\n\n✅ Complete!")
        # print(f"📊 {processor.processed:,} addresses found")
        # print(f"�  {processor.saved_batches:,} batches saved")
        # print(f"� Filec processed: {file_size_formatted}")
        # print(f"⏱️  Processing time: {elapsed:.1f} seconds ({rate:.0f} addresses/sec)")
        
        if use_file_storage:
            print(f"Data saved to: output/addresses.jsonl")
        
        return {'addresses': processor.processed, 'batches': processor.saved_batches}
        
    finally:
        if client:
            client.close()

if __name__ == "__main__":
    if len(sys.argv) < 4 or len(sys.argv) > 5:
        print("Usage: python osm_processor.py <osm_filename> <country_code> <country_name> [json]")
        print("Example: python osm_processor.py ye YE Yemen")
        print("Example: python osm_processor.py us US \"United States\" json")
        print("Add 'json' as 4th parameter to force JSON output instead of MongoDB")
        sys.exit(1)
    
    filename = sys.argv[1]
    country_code = sys.argv[2]
    country_name = sys.argv[3]
    force_json = len(sys.argv) == 5 and sys.argv[4].lower() == 'json'
    
    try:
        stats = process_osm_file(filename, country_code, country_name, force_json)
        print(f"Processed {stats['addresses']} addresses in {stats['batches']} batches")
    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)