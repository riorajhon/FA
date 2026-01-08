#!/usr/bin/env python3
"""
Western Sahara Address Extractor
Extracts addresses specifically from Western Sahara using precise geographic filtering
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

class WesternSaharaAddressProcessor(osmium.SimpleHandler):
    """OSM handler specifically for extracting Western Sahara addresses"""
    
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
        
        # Western Sahara regions - VERY STRICT boundaries to exclude Morocco
        # Only the most certain Western Sahara areas
        self.western_sahara_regions = [
            # Laayoune (El Aaiún) - very tight boundaries around city center
            {'name': 'Laayoune Center', 'min_lon': -13.20, 'max_lon': -13.18, 'min_lat': 27.15, 'max_lat': 27.17},
            
            # Dakhla (Ad Dakhla) - very tight boundaries around city center
            {'name': 'Dakhla Center', 'min_lon': -15.95, 'max_lon': -15.93, 'min_lat': 23.71, 'max_lat': 23.73},
            
            # Boujdour (Bu Jaydur) - tight boundaries
            {'name': 'Boujdour Center', 'min_lon': -14.50, 'max_lon': -14.48, 'min_lat': 26.12, 'max_lat': 26.14},
            
            # Es Semara (As Smara) - tight boundaries
            {'name': 'Smara Center', 'min_lon': -11.68, 'max_lon': -11.66, 'min_lat': 26.73, 'max_lat': 26.75},
            
            # Aousserd (Awsard) - very remote, clearly in WS
            {'name': 'Aousserd', 'min_lon': -12.48, 'max_lon': -12.46, 'min_lat': 22.58, 'max_lat': 22.60},
            
            # Tifariti (SADR controlled area) - clearly in WS
            {'name': 'Tifariti', 'min_lon': -10.62, 'max_lon': -10.60, 'min_lat': 26.17, 'max_lat': 26.19},
            
            # Bir Lehlou (SADR controlled)
            {'name': 'Bir Lehlou', 'min_lon': -11.02, 'max_lon': -11.00, 'min_lat': 25.17, 'max_lat': 25.19},
            
            # Only the most remote desert areas that are clearly WS
            # Far eastern desert (SADR controlled, no Morocco presence)
            {'name': 'East Desert', 'min_lon': -10.50, 'max_lon': -9.00, 'min_lat': 22.00, 'max_lat': 25.50},
            
            # Southern coastal desert (clearly WS, south of Moroccan settlements)
            {'name': 'South Coast', 'min_lon': -16.00, 'max_lon': -15.80, 'min_lat': 23.60, 'max_lat': 23.80}
        ]
        
    def is_in_western_sahara(self, lon, lat):
        """
        Check if coordinates are within Western Sahara regions
        Uses very strict boundaries to exclude Moroccan territories
        """
        # First check if it's in any WS region
        in_ws_region = False
        for region in self.western_sahara_regions:
            if (region['min_lon'] <= lon <= region['max_lon'] and
                region['min_lat'] <= lat <= region['max_lat']):
                in_ws_region = True
                break
        
        if not in_ws_region:
            return False
        
        # Additional check: exclude known Moroccan areas even if they fall in broad regions
        # These are areas that are definitely Morocco, not Western Sahara
        moroccan_exclusions = [
            # Exclude northern Morocco areas that might overlap
            {'min_lon': -10.00, 'max_lon': -1.00, 'min_lat': 27.50, 'max_lat': 36.00},
            # Exclude central Morocco
            {'min_lon': -10.00, 'max_lon': -1.00, 'min_lat': 31.00, 'max_lat': 35.00},
            # Exclude areas too close to Moroccan cities
            {'min_lon': -8.00, 'max_lon': -4.00, 'min_lat': 29.00, 'max_lat': 32.00}
        ]
        
        for exclusion in moroccan_exclusions:
            if (exclusion['min_lon'] <= lon <= exclusion['max_lon'] and
                exclusion['min_lat'] <= lat <= exclusion['max_lat']):
                return False
        
        return True
        
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
        
        return any(tag_key in tags for tag_key in address_tags)
        
    def has_western_sahara_indicators(self, tags):
        """
        Check for Western Sahara specific indicators in tags
        Must be very specific to avoid Moroccan addresses
        """
        # Very specific Western Sahara indicators
        ws_indicators = [
            'western sahara', 'sahara occidental', 'sadr', 'sahrawi',
            'laayoune', 'el aaiun', 'العيون',  # Laayoune in Arabic
            'dakhla', 'ad dakhla', 'الداخلة',  # Dakhla in Arabic  
            'tifariti', 'bir lehlou', 'aousserd'
        ]
        
        # Moroccan indicators to exclude (if these appear, it's likely Morocco)
        moroccan_exclusions = [
            'morocco', 'maroc', 'المغرب', 'royaume du maroc',
            'casablanca', 'rabat', 'marrakech', 'fes', 'tangier',
            'agadir', 'meknes', 'oujda', 'kenitra', 'tetouan'
        ]
        
        # Check all tag values - iterate through tags properly
        try:
            for tag in tags:
                tag_value = tag.v.lower()  # Access tag value using .v attribute
                
                # If it contains Moroccan indicators, exclude it
                if any(exclusion in tag_value for exclusion in moroccan_exclusions):
                    return False
                
                # Check for WS indicators
                if any(indicator in tag_value for indicator in ws_indicators):
                    return True
        except Exception:
            # If tag access fails, skip this check
            pass
        
        return False
        
    def way(self, w):
        """Process OSM ways to find Western Sahara buildings with address info"""
        if 'building' not in w.tags:
            return
            
        # Must have some address or name information
        if not self.has_address_info(w.tags):
            return
            
        # Check if way is in Western Sahara regions
        if not w.nodes:
            return
            
        # Check if ANY node is in our Western Sahara regions
        is_ws_building = False
        for node in w.nodes:
            if hasattr(node, 'location') and node.location.valid():
                if self.is_in_western_sahara(node.location.lon, node.location.lat):
                    is_ws_building = True
                    break
                    
        # Additional check: look for Western Sahara indicators in tags
        if not is_ws_building:
            is_ws_building = self.has_western_sahara_indicators(w.tags)
                    
        if is_ws_building:
            # Additional validation: check building type and tags
            building_type = w.tags.get('building', '')
            
            # Skip generic or uncertain buildings unless they have specific info
            skip_buildings = ['yes', 'unclassified', 'other']
            if building_type in skip_buildings:
                # Check if it has meaningful tags
                has_meaningful_tags = any(tag_key in w.tags for tag_key in ['name', 'amenity', 'shop', 'office'])
                if not has_meaningful_tags:
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
            'country_code': 'EH',
            'country_name': 'Western Sahara',
            'status': 'origin'
        }
        
        import os
        os.makedirs('output', exist_ok=True)
        
        with open('output/western_sahara_addresses.jsonl', 'a', encoding='utf-8') as f:
            f.write(json.dumps(doc) + '\n')
    
    def _queue_batch(self):
        """Queue batch for bulk insertion"""
        doc = {
            'ids': ','.join(self.batch), 
            'country_code': 'EH',
            'country_name': 'Western Sahara',
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
                    with open('output/western_sahara_addresses.jsonl', 'a', encoding='utf-8') as f:
                        f.write(json.dumps(doc) + '\n')
                self.pending_ops = []
    
    def finalize(self):
        """Save remaining data"""
        if self.batch:
            self._save_batch()
        self._flush_pending()
        
        elapsed = time.time() - self.start_time
        rate = self.processed / elapsed if elapsed > 0 else 0
        print(f"\n\nComplete! Found {self.processed:,} Western Sahara addresses in {elapsed:.1f}s ({rate:.0f}/sec)")

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

def extract_western_sahara_addresses(pbf_file="eh-latest.osm.pbf"):
    """Extract Western Sahara addresses from Morocco/Western Sahara PBF file"""
    
    # Try MongoDB connection
    client, collection = try_mongodb_connection()
    use_file_storage = collection is None
    
    if use_file_storage:
        print("MongoDB not available - saving to output/western_sahara_addresses.jsonl")
        import os
        os.makedirs('output', exist_ok=True)
        # Clear previous output file
        with open('output/western_sahara_addresses.jsonl', 'w') as f:
            pass
    
    print(f"🏜️ Starting Western Sahara address extraction from {pbf_file}...")
    
    processor = WesternSaharaAddressProcessor(collection, use_file_storage)
    
    try:
        processor.apply_file(pbf_file, locations=True)
        processor.finalize()
        
        if use_file_storage:
            print(f"Western Sahara addresses saved to: output/western_sahara_addresses.jsonl")
        
        return {'addresses': processor.processed, 'batches': processor.saved_batches}
        
    finally:
        if client:
            client.close()

if __name__ == "__main__":
    import sys
    
    pbf_file = "eh-latest.osm.pbf"
    if len(sys.argv) > 1:
        pbf_file = sys.argv[1]
    
    try:
        stats = extract_western_sahara_addresses(pbf_file)
        print(f"✅ Extracted {stats['addresses']} Western Sahara addresses in {stats['batches']} batches")
    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)