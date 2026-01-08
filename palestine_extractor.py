#!/usr/bin/env python3
"""
Palestinian Territory Address Extractor
Extracts addresses specifically from Palestinian territories using precise geographic filtering
"""

import osmium
import json
import time
import os
from typing import Optional
from pymongo import MongoClient, InsertOne
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

class PalestineAddressProcessor(osmium.SimpleHandler):
    """OSM handler specifically for extracting Palestinian Territory addresses"""
    
    def __init__(self, collection=None, use_file_storage=False):
        osmium.SimpleHandler.__init__(self)
        self.batch = []
        self.batch_size = 100
        self.processed = 0
        self.saved_batches = 0
        self.collection = collection
        self.pending_ops = []
        self.use_file_storage = use_file_storage
        self.start_time = time.time()
        
        # Palestinian Territory regions - West Bank and Gaza Strip
        self.palestine_regions = [
            # Gaza Strip
            {'name': 'Gaza Strip', 'min_lon': 34.20, 'max_lon': 34.57, 'min_lat': 31.22, 'max_lat': 31.58},
            
            # West Bank major cities and areas
            # Ramallah area
            {'name': 'Ramallah', 'min_lon': 35.15, 'max_lon': 35.25, 'min_lat': 31.85, 'max_lat': 31.95},
            # Bethlehem area
            {'name': 'Bethlehem', 'min_lon': 35.15, 'max_lon': 35.25, 'min_lat': 31.65, 'max_lat': 31.75},
            # Hebron area
            {'name': 'Hebron', 'min_lon': 35.05, 'max_lon': 35.15, 'min_lat': 31.48, 'max_lat': 31.58},
            # Nablus area
            {'name': 'Nablus', 'min_lon': 35.20, 'max_lon': 35.30, 'min_lat': 32.20, 'max_lat': 32.25},
            # Jenin area
            {'name': 'Jenin', 'min_lon': 35.25, 'max_lon': 35.35, 'min_lat': 32.45, 'max_lat': 32.50},
            # Tulkarm area
            {'name': 'Tulkarm', 'min_lon': 35.00, 'max_lon': 35.10, 'min_lat': 32.30, 'max_lat': 32.35},
            # Qalqilya area
            {'name': 'Qalqilya', 'min_lon': 34.95, 'max_lon': 35.05, 'min_lat': 32.18, 'max_lat': 32.23},
            # Jericho area
            {'name': 'Jericho', 'min_lon': 35.40, 'max_lon': 35.50, 'min_lat': 31.85, 'max_lat': 31.90},
            
            # Additional West Bank areas (broader coverage)
            {'name': 'West Bank North', 'min_lon': 35.00, 'max_lon': 35.60, 'min_lat': 32.00, 'max_lat': 32.55},
            {'name': 'West Bank Central', 'min_lon': 35.00, 'max_lon': 35.60, 'min_lat': 31.70, 'max_lat': 32.00},
            {'name': 'West Bank South', 'min_lon': 35.00, 'max_lon': 35.60, 'min_lat': 31.30, 'max_lat': 31.70}
        ]
        
    def is_in_palestine(self, lon, lat):
        """
        Check if coordinates are within Palestinian Territory regions
        Covers both Gaza Strip and West Bank areas
        """
        for region in self.palestine_regions:
            if (region['min_lon'] <= lon <= region['max_lon'] and
                region['min_lat'] <= lat <= region['max_lat']):
                return True
        return False
        
    def has_address_info(self, tags):
        """
        Check if the building has meaningful address information
        """
        # Must have at least one address component
        address_tags = [
            'addr:street', 'addr:city', 'addr:town', 'addr:village',
            'addr:suburb', 'addr:district', 'addr:region', 'addr:postcode',
            'name', 'amenity', 'shop', 'office', 'tourism'
        ]
        
        return any(tag in tags for tag in address_tags)
        
    def way(self, w):
        """Process OSM ways to find Palestinian Territory buildings with address info"""
        if 'building' not in w.tags:
            return
            
        # Must have some address or name information
        if not self.has_address_info(w.tags):
            return
            
        # Check if way is in Palestinian Territory regions
        if not w.nodes:
            return
            
        # Check if ANY node is in our Palestinian Territory regions
        is_palestine_building = False
        for node in w.nodes:
            if hasattr(node, 'location') and node.location.valid():
                if self.is_in_palestine(node.location.lon, node.location.lat):
                    is_palestine_building = True
                    break
                    
        if is_palestine_building:
            # Additional validation: check building type and tags
            building_type = w.tags.get('building', '')
            
            # Skip generic or uncertain buildings unless they have specific info
            skip_buildings = ['yes', 'unclassified', 'other']
            if building_type in skip_buildings and not any(tag in w.tags for tag in ['name', 'amenity', 'shop', 'office']):
                return
                
            self._add_address(f'W{w.id}')
                        
        self._update_progress()
            
    def _update_progress(self):
        """Update processing progress"""
        current_time = time.time()
        if self.processed % 1000 == 0:
            elapsed = current_time - self.start_time
            rate = self.processed / elapsed if elapsed > 0 else 0
            print(f"\rProcessed: {self.processed:,} addresses | Rate: {rate:.0f}/sec | Batches: {self.saved_batches:,}", end='', flush=True)
    
    def _add_address(self, element_id):
        """Add address to batch"""
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
        """Save batch to JSON file"""
        doc = {
            'ids': ','.join(self.batch), 
            'country_code': 'PS',
            'country_name': 'Palestinian Territory',
            'status': 'origin'
        }
        
        import os
        os.makedirs('output', exist_ok=True)
        
        with open('output/palestine_addresses.jsonl', 'a', encoding='utf-8') as f:
            f.write(json.dumps(doc) + '\n')
    
    def _queue_batch(self):
        """Queue batch for bulk insertion"""
        doc = {
            'ids': ','.join(self.batch), 
            'country_code': 'PS',
            'country_name': 'Palestinian Territory',
            'status': 'origin'
        }
        self.pending_ops.append(InsertOne(doc))
        
        if len(self.pending_ops) >= 500:
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
                    with open('output/palestine_addresses.jsonl', 'a', encoding='utf-8') as f:
                        f.write(json.dumps(doc) + '\n')
                self.pending_ops = []
    
    def finalize(self):
        """Save remaining data"""
        if self.batch:
            self._save_batch()
        self._flush_pending()
        
        elapsed = time.time() - self.start_time
        rate = self.processed / elapsed if elapsed > 0 else 0
        print(f"\n\nComplete! Found {self.processed:,} Palestinian Territory addresses in {elapsed:.1f}s ({rate:.0f}/sec)")

def try_mongodb_connection():
    """Test MongoDB connection using environment variables"""
    mongodb_uri = os.getenv('MONGODB_URI')
    if not mongodb_uri:
        logger.error("MONGODB_URI not found in environment variables")
        return None, None
        
    try:
        client = MongoClient(mongodb_uri, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        
        db_name = os.getenv('DB_NAME', 'osm_addresses')
        collection_name = os.getenv('COLLECTION_ADDRESS_BATCHES', 'address_batches')
        collection = client[db_name][collection_name]
        
        return client, collection
    except Exception as e:
        logger.warning(f"MongoDB connection failed: {e}")
        return None, None

def extract_palestine_addresses(pbf_file="israel-and-palestine.osm.pbf"):
    """Extract Palestinian Territory addresses from combined PBF file"""
    
    # Try MongoDB connection
    client, collection = try_mongodb_connection()
    use_file_storage = collection is None
    
    if use_file_storage:
        print("MongoDB not available - saving to output/palestine_addresses.jsonl")
        import os
        os.makedirs('output', exist_ok=True)
        # Clear previous output file
        with open('output/palestine_addresses.jsonl', 'w') as f:
            pass
    
    print(f"🇵🇸 Starting Palestinian Territory address extraction from {pbf_file}...")
    
    processor = PalestineAddressProcessor(collection, use_file_storage)
    
    try:
        processor.apply_file(pbf_file, locations=True)
        processor.finalize()
        
        if use_file_storage:
            print(f"Palestinian Territory addresses saved to: output/palestine_addresses.jsonl")
        
        return {'addresses': processor.processed, 'batches': processor.saved_batches}
        
    finally:
        if client:
            client.close()

if __name__ == "__main__":
    import sys
    
    pbf_file = "israel-and-palestine.osm.pbf"
    if len(sys.argv) > 1:
        pbf_file = sys.argv[1]
    
    try:
        stats = extract_palestine_addresses(pbf_file)
        print(f"✅ Extracted {stats['addresses']} Palestinian Territory addresses in {stats['batches']} batches")
    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)