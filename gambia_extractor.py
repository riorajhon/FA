#!/usr/bin/env python3
"""
Gambia Address Extractor
Extracts addresses specifically from Gambia using more precise geographic filtering
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

class GambiaAddressProcessor(osmium.SimpleHandler):
    """OSM handler specifically for extracting Gambia addresses"""
    
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
        
        # Much more precise Gambia boundaries - only the core areas
        # Gambia is extremely narrow, these are the most certain Gambia-only areas
        self.strict_gambia_regions = [
            # Banjul area (capital)
            {'min_lon': -16.65, 'max_lon': -16.55, 'min_lat': 13.45, 'max_lat': 13.48},
            # Serekunda area 
            {'min_lon': -16.70, 'max_lon': -16.65, 'min_lat': 13.43, 'max_lat': 13.47},
            # Brikama area
            {'min_lon': -16.75, 'max_lon': -16.65, 'min_lat': 13.25, 'max_lat': 13.35},
            # Farafenni area (central)
            {'min_lon': -15.60, 'max_lon': -15.50, 'min_lat': 13.55, 'max_lat': 13.65},
            # Georgetown/Janjanbureh area
            {'min_lon': -14.77, 'max_lon': -14.67, 'min_lat': 13.52, 'max_lat': 13.58},
            # Basse area (eastern)
            {'min_lon': -14.22, 'max_lon': -14.12, 'min_lat': 13.30, 'max_lat': 13.35}
        ]
        
    def is_in_gambia(self, lon, lat):
        """
        Check if coordinates are within known Gambia urban areas only
        Using strict regional boundaries around major Gambia towns/cities
        """
        for region in self.strict_gambia_regions:
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
        """Process OSM ways to find Gambia buildings with address info"""
        if 'building' not in w.tags:
            return
        
        # if 'addr:housenumber' not in w.tags:
        #     return
        # Must have some address or name information
        if not self.has_address_info(w.tags):
            return
            
        # Check if way is in strict Gambia regions
        if not w.nodes:
            return
            
        # Check if ANY node is in our strict Gambia regions
        is_gambia_building = False
        for node in w.nodes:
            if hasattr(node, 'location') and node.location.valid():
                if self.is_in_gambia(node.location.lon, node.location.lat):
                    is_gambia_building = True
                    break
                    
        if is_gambia_building:
            # Additional validation: check building type and tags
            building_type = w.tags.get('building', '')
            
            # Skip generic or uncertain buildings
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
            'country_code': 'GM',
            'country_name': 'Gambia',
            'status': 'origin'
        }
        
        import os
        os.makedirs('output', exist_ok=True)
        
        with open('output/gambia_addresses.jsonl', 'a', encoding='utf-8') as f:
            f.write(json.dumps(doc) + '\n')
    
    def _queue_batch(self):
        """Queue batch for bulk insertion"""
        doc = {
            'ids': ','.join(self.batch), 
            'country_code': 'GM',
            'country_name': 'Gambia',
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
                    with open('output/gambia_addresses.jsonl', 'a', encoding='utf-8') as f:
                        f.write(json.dumps(doc) + '\n')
                self.pending_ops = []
    
    def finalize(self):
        """Save remaining data"""
        if self.batch:
            self._save_batch()
        self._flush_pending()
        
        elapsed = time.time() - self.start_time
        rate = self.processed / elapsed if elapsed > 0 else 0
        print(f"\n\nComplete! Found {self.processed:,} Gambia addresses in {elapsed:.1f}s ({rate:.0f}/sec)")

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

def extract_gambia_addresses(pbf_file="senegal-and-gambia.osm.pbf"):
    """Extract Gambia addresses from combined PBF file"""
    
    # Try MongoDB connection
    client, collection = try_mongodb_connection()
    use_file_storage = collection is None
    
    if use_file_storage:
        print("MongoDB not available - saving to output/gambia_addresses.jsonl")
        import os
        os.makedirs('output', exist_ok=True)
        # Clear previous output file
        with open('output/gambia_addresses.jsonl', 'w') as f:
            pass
    
    print(f"🇬🇲 Starting Gambia address extraction from {pbf_file}...")
    
    processor = GambiaAddressProcessor(collection, use_file_storage)
    
    try:
        processor.apply_file(pbf_file, locations=True)
        processor.finalize()
        
        if use_file_storage:
            print(f"Gambia addresses saved to: output/gambia_addresses.jsonl")
        
        return {'addresses': processor.processed, 'batches': processor.saved_batches}
        
    finally:
        if client:
            client.close()

if __name__ == "__main__":
    import sys
    
    pbf_file = "senegal-and-gambia.osm.pbf"
    if len(sys.argv) > 1:
        pbf_file = sys.argv[1]
    
    try:
        stats = extract_gambia_addresses(pbf_file)
        print(f"✅ Extracted {stats['addresses']} Gambia addresses in {stats['batches']} batches")
    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)