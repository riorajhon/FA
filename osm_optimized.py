#!/usr/bin/env python3
"""
OSM PBF Address Processor - Optimized for Small Bbox
Skips already validated addresses and uses better filtering

Requirements: pip install pymongo osmium
Usage: python osm_optimized.py <osm_filename> <country_code> <country_name>
"""

import os
import sys
import json
import time
from typing import Optional, Set
from pymongo import MongoClient, InsertOne
import osmium
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

class OSMAddressProcessor(osmium.SimpleHandler):
    """Optimized OSM handler that skips already validated addresses"""
    
    __slots__ = ['batch', 'batch_size', 'processed', 'saved_batches', 'collection', 
                 'pending_ops', 'use_file_storage', 'start_time', 'last_report', 
                 'country_code', 'country_name', 'file_size', 'last_progress_report',
                 'validated_osm_ids', 'skipped_count']
    
    def __init__(self, collection=None, use_file_storage=False, country_code=None, 
                 country_name=None, file_size=0, validated_osm_ids=None):
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
        self.last_progress_report = 0
        self.validated_osm_ids = validated_osm_ids or set()
        self.skipped_count = 0
    
    def check(self, t, osm_id):
        """Enhanced filtering for small bbox addresses"""
        # Skip if already validated
        if osm_id in self.validated_osm_ids:
            self.skipped_count += 1
            return False
        
        tags = t.tags
        
        # STRATEGY 1: Buildings with street (most reliable for small bbox)
        if 'building' in tags and 'addr:street' in tags:
            return True
        
        # STRATEGY 2: Places with street names (villages, hamlets, neighborhoods)
        if 'place' in tags and 'addr:street' in tags:
            place_type = tags['place']
            if place_type in ['neighbourhood', 'suburb', 'quarter', 'hamlet', 'isolated_dwelling']:
                return True
        
        # STRATEGY 3: Amenities with addresses (shops, restaurants, etc.)
        if 'amenity' in tags and 'addr:street' in tags:
            return True
        
        # STRATEGY 4: Shops with addresses
        if 'shop' in tags and 'addr:street' in tags:
            return True
        
        # STRATEGY 5: Tourism locations with addresses
        if 'tourism' in tags and 'addr:street' in tags:
            return True
        
        # STRATEGY 6: Leisure locations with addresses
        if 'leisure' in tags and 'addr:street' in tags:
            return True
        
        # STRATEGY 7: Office buildings with addresses
        if 'office' in tags and 'addr:street' in tags:
            return True
        
        return False
    
    def node(self, n):
        osm_id = f'N{n.id}'
        if self.check(n, osm_id):
            self._add_address(osm_id)
        self._update_progress()
            
    def way(self, w):
        osm_id = f'W{w.id}'
        if self.check(w, osm_id):
            self._add_address(osm_id)
        self._update_progress()
            
    def relation(self, r):
        osm_id = f'R{r.id}'
        if self.check(r, osm_id):
            self._add_address(osm_id)
        self._update_progress()
    
    def _update_progress(self):
        """Update file processing progress"""
        current_time = time.time()
        if current_time - self.last_progress_report >= 1.0:
            if self.file_size > 0:
                elapsed = current_time - self.start_time
                if elapsed > 0:
                    estimated_progress = min(95, (elapsed / (elapsed + 30)) * 100)
                    file_size_mb = self.file_size / (1024 * 1024)
                    processed_mb = (estimated_progress / 100) * file_size_mb
                    
                    print(f"\rProgress: {estimated_progress:.1f}% | File: {processed_mb:.1f}MB/{file_size_mb:.1f}MB | New: {self.processed:,} | Skipped: {self.skipped_count:,} | Batches: {self.saved_batches:,}", end='', flush=True)
            
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
        
        os.makedirs('output', exist_ok=True)
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
        
        if self.processed < 10000:
            flush_size = 1000
        elif self.processed < 50000:
            flush_size = 500
        else:
            flush_size = 250
        
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
        
        if self.file_size > 0:
            file_size_mb = self.file_size / (1024 * 1024)
            print(f"\rProgress: 100.0% | File: {file_size_mb:.1f}MB/{file_size_mb:.1f}MB | New: {self.processed:,} | Skipped: {self.skipped_count:,} | Batches: {self.saved_batches:,}", flush=True)

def load_validated_osm_ids(mongodb_uri, country_name):
    """Load already validated OSM IDs from database"""
    try:
        client = MongoClient(mongodb_uri, serverSelectionTimeoutMS=5000)
        db = client.osm_addresses
        
        # Get validated addresses for this country
        validated_ids = set()
        cursor = db.validated_addresses.find(
            {'country': country_name},
            {'osm_id': 1}
        )
        
        for doc in cursor:
            validated_ids.add(doc['osm_id'])
        
        client.close()
        logger.info(f"Loaded {len(validated_ids):,} already validated OSM IDs for {country_name}")
        return validated_ids
        
    except Exception as e:
        logger.warning(f"Could not load validated IDs: {e}")
        return set()

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
        test_doc = {'test': True}
        result = collection.insert_one(test_doc)
        collection.delete_one({'_id': result.inserted_id})
        return client, collection
    except Exception as e:
        logger.warning(f"MongoDB connection failed: {e}")
        return None, None

def process_osm_file(filename, country_code, country_name, force_json=False, 
                     mongodb_uri="mongodb://admin:fjkfjrj!20020415@localhost:27017/?authSource=admin"):
    """Process OSM file with optimized filtering and skip already validated addresses"""
    file_path = find_osm_file(filename)
    if not file_path:
        raise FileNotFoundError(f"File '{filename}' not found in osm_data/")
    
    file_size = get_file_size(file_path)
    
    # Load already validated OSM IDs
    print(f"🔍 Loading already validated addresses for {country_name}...")
    validated_osm_ids = load_validated_osm_ids(mongodb_uri, country_name)
    
    # Try MongoDB connection
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
        with open('output/addresses.jsonl', 'w') as f:
            pass
    
    processor = OSMAddressProcessor(collection, use_file_storage, country_code, 
                                   country_name, file_size, validated_osm_ids)
    
    try:
        processor.apply_file(file_path)
        processor.finalize()
        
        elapsed = time.time() - processor.start_time
        rate = processor.processed / elapsed if elapsed > 0 else 0
        
        print(f"\n\n✅ Complete!")
        print(f"📊 {processor.processed:,} new addresses found")
        print(f"⏭️  {processor.skipped_count:,} already validated (skipped)")
        print(f"📦 {processor.saved_batches:,} batches saved")
        print(f"⏱️  Processing time: {elapsed:.1f} seconds ({rate:.0f} addresses/sec)")
        
        if use_file_storage:
            print(f"💾 Data saved to: output/addresses.jsonl")
        
        return {'addresses': processor.processed, 'batches': processor.saved_batches, 'skipped': processor.skipped_count}
        
    finally:
        if client:
            client.close()

if __name__ == "__main__":
    if len(sys.argv) < 4 or len(sys.argv) > 5:
        print("Usage: python osm_optimized.py <osm_filename> <country_code> <country_name> [json]")
        print("Example: python osm_optimized.py ye YE Yemen")
        sys.exit(1)
    
    filename = sys.argv[1]
    country_code = sys.argv[2]
    country_name = sys.argv[3]
    force_json = len(sys.argv) == 5 and sys.argv[4].lower() == 'json'
    
    try:
        stats = process_osm_file(filename, country_code, country_name, force_json)
        print(f"✅ Processed {stats['addresses']} new addresses, skipped {stats['skipped']} already validated")
    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)
