#!/usr/bin/env python3
"""
Multi-OSM File Processor for US and Canada
Processes multiple OSM files from US and Canada folders
Stops after finding 2 batches (200 addresses) per file to distribute processing
"""

import os
import sys
import json
import time
import glob
import random
from typing import Optional, List
from pymongo import MongoClient, InsertOne
import osmium
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

class LimitedOSMAddressProcessor(osmium.SimpleHandler):
    """OSM handler with batch limit per file and random address skipping"""
    
    def __init__(self, collection=None, use_file_storage=False, country_code=None, country_name=None, max_batches=2, skip_probability=0.3):
        osmium.SimpleHandler.__init__(self)
        self.batch = []
        self.batch_size = 100  # 100 addresses per batch
        self.processed = 0
        self.saved_batches = 0
        self.max_batches = max_batches  # Stop after this many batches
        self.skip_probability = skip_probability  # Probability of skipping an address (0.0 to 1.0)
        self.collection = collection
        self.pending_ops = []
        self.use_file_storage = use_file_storage
        self.start_time = time.time()
        self.country_code = country_code
        self.country_name = country_name
        self.should_stop = False
        self.skipped_count = 0
        
    # def node(self, n):
    #     if self.should_stop:
    #         return
    #     if 'addr:housenumber' in n.tags:
    #         # Randomly skip some addresses for diversity
    #         if random.random() < self.skip_probability:
    #             self.skipped_count += 1
    #             return
    #         self._add_address(f'N{n.id}')
            
    def way(self, w):
        if self.should_stop:
            return
        if 'addr:housenumber' in w.tags:
            # Randomly skip some addresses for diversity
            if random.random() < self.skip_probability:
                self.skipped_count += 1
                return
            self._add_address(f'W{w.id}')
            
    # def relation(self, r):
    #     if self.should_stop:
    #         return
    #     if 'addr:housenumber' in r.tags:
    #         # Randomly skip some addresses for diversity
    #         if random.random() < self.skip_probability:
    #             self.skipped_count += 1
    #             return
    #         self._add_address(f'R{r.id}')
    
    def _add_address(self, element_id):
        if self.should_stop:
            return
            
        self.batch.append(element_id)
        self.processed += 1
        
        if len(self.batch) >= self.batch_size:
            self._save_batch()
            
            # Check if we've reached the batch limit
            if self.saved_batches >= self.max_batches:
                self.should_stop = True
                print(f"\nReached batch limit ({self.max_batches}) for this file. Moving to next file...")
    
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
            'country_code': self.country_code,
            'country_name': self.country_name,
            'status': 'origin'
        }
        
        os.makedirs('output', exist_ok=True)
        with open('output/multi_addresses.jsonl', 'a', encoding='utf-8') as f:
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
        
        if len(self.pending_ops) >= 10:  # Smaller flush size for faster processing
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
                    with open('output/multi_addresses.jsonl', 'a', encoding='utf-8') as f:
                        f.write(json.dumps(doc) + '\n')
                self.pending_ops = []
    
    def finalize(self):
        """Save remaining data"""
        if self.batch:
            self._save_batch()
        self._flush_pending()

class MultiOSMProcessor:
    """Processes multiple OSM files for US and Canada"""
    
    def __init__(self, force_json=False):
        self.force_json = force_json
        self.client = None
        self.collection = None
        self.use_file_storage = True
        
        if not force_json:
            self._try_mongodb_connection()
    
    def _try_mongodb_connection(self):
        """Try to connect to MongoDB"""
        try:
            mongodb_uri = os.getenv('MONGODB_URI')
            if mongodb_uri:
                self.client = MongoClient(mongodb_uri, serverSelectionTimeoutMS=5000)
                self.client.admin.command('ping')
                
                db_name = os.getenv('DB_NAME', 'osm_addresses')
                collection_name = os.getenv('COLLECTION_ADDRESS_BATCHES', 'address_batches')
                self.collection = self.client[db_name][collection_name]
                self.use_file_storage = False
                print("Connected to MongoDB")
        except Exception as e:
            logger.warning(f"MongoDB connection failed: {e}")
            self.use_file_storage = True
    
    def get_osm_files(self, country_folder: str) -> List[str]:
        """Get all OSM files from country folder"""
        folder_path = os.path.join('osm_data', country_folder)
        if not os.path.exists(folder_path):
            return []
        
        # Look for .pbf files (handles various naming patterns)
        patterns = [
            os.path.join(folder_path, '*.osm.pbf'),
            os.path.join(folder_path, '*.pbf'),
            os.path.join(folder_path, '*.osm*.pbf')
        ]
        
        files = []
        for pattern in patterns:
            files.extend(glob.glob(pattern))
        
        # Remove duplicates and sort
        files = list(set(files))
        return sorted(files)
    
    def process_single_file(self, file_path: str, country_code: str, country_name: str, max_batches: int = 2, skip_probability: float = 0.3):
        """Process a single OSM file with batch limit and random skipping"""
        if not os.path.exists(file_path):
            logger.error(f"File not found: {file_path}")
            return {'addresses': 0, 'batches': 0, 'skipped': 0}
        
        file_size = os.path.getsize(file_path)
        file_size_mb = file_size / (1024 * 1024)
        
        print(f"Processing {os.path.basename(file_path)} ({file_size_mb:.1f}MB) - Max {max_batches} batches, Skip {skip_probability*100:.0f}%")
        
        processor = LimitedOSMAddressProcessor(
            self.collection, 
            self.use_file_storage, 
            country_code, 
            country_name, 
            max_batches,
            skip_probability
        )
        
        try:
            processor.apply_file(file_path)
            processor.finalize()
            
            elapsed = time.time() - processor.start_time
            print(f"  → Found {processor.processed} addresses, skipped {processor.skipped_count} in {processor.saved_batches} batches ({elapsed:.1f}s)")
            
            return {
                'addresses': processor.processed, 
                'batches': processor.saved_batches,
                'skipped': processor.skipped_count
            }
            
        except Exception as e:
            logger.error(f"Error processing {file_path}: {e}")
            return {'addresses': 0, 'batches': 0, 'skipped': 0}
    
    def process_country(self, country_folder: str, country_code: str, country_name: str, max_batches_per_file: int = 2, skip_probability: float = 0.3):
        """Process all OSM files for a country with random address skipping"""
        print(f"\n🌍 Processing {country_name} ({country_code})")
        print(f"Random skip probability: {skip_probability*100:.0f}% (for address diversity)")
        
        if self.use_file_storage:
            print("Saving to output/multi_addresses.jsonl")
            # Clear output file for this country
            os.makedirs('output', exist_ok=True)
            if not os.path.exists('output/multi_addresses.jsonl'):
                with open('output/multi_addresses.jsonl', 'w') as f:
                    pass
        
        osm_files = self.get_osm_files(country_folder)
        if not osm_files:
            print(f"No OSM files found in osm_data/{country_folder}/")
            return {'total_addresses': 0, 'total_batches': 0, 'total_skipped': 0, 'files_processed': 0}
        
        print(f"Found {len(osm_files)} OSM files")
        
        total_addresses = 0
        total_batches = 0
        total_skipped = 0
        files_processed = 0
        
        for i, file_path in enumerate(osm_files, 1):
            print(f"\n[{i}/{len(osm_files)}] ", end='')
            
            result = self.process_single_file(file_path, country_code, country_name, max_batches_per_file, skip_probability)
            
            total_addresses += result['addresses']
            total_batches += result['batches']
            total_skipped += result['skipped']
            files_processed += 1
            
            # Show running totals
            print(f"  Running totals: {total_addresses:,} addresses, {total_skipped:,} skipped, {total_batches} batches")
        
        print(f"\n✅ {country_name} Complete!")
        print(f"📊 Total: {total_addresses:,} addresses, {total_skipped:,} skipped in {total_batches} batches from {files_processed} files")
        
        return {
            'total_addresses': total_addresses, 
            'total_batches': total_batches, 
            'total_skipped': total_skipped,
            'files_processed': files_processed
        }
    
    def close(self):
        """Close database connection"""
        if self.client:
            self.client.close()

def main():
    """Main function"""
    if len(sys.argv) < 2 or len(sys.argv) > 3:
        print("Usage: python multi_osm_processor.py <country> [json]")
        print("Countries: us, canada")
        print("Example: python multi_osm_processor.py us")
        print("Example: python multi_osm_processor.py canada json")
        sys.exit(1)
    
    country = sys.argv[1].lower()
    force_json = len(sys.argv) == 3 and sys.argv[2].lower() == 'json'
    
    # Country mapping
    country_config = {
        'us': {
            'folder': 'US',
            'code': 'US',
            'name': 'United States'
        },
        'canada': {
            'folder': 'Canada',
            'code': 'CA', 
            'name': 'Canada'
        }
    }
    
    if country not in country_config:
        print(f"Error: Unknown country '{country}'. Available: us, canada")
        sys.exit(1)
    
    config = country_config[country]
    processor = None
    
    try:
        processor = MultiOSMProcessor(force_json)
        
        # Process with 2 batches per file (200 addresses per file)
        result = processor.process_country(
            config['folder'], 
            config['code'], 
            config['name'], 
            max_batches_per_file=2
        )
        
        print(f"\n🎉 Final Results:")
        print(f"Files processed: {result['files_processed']}")
        print(f"Total addresses: {result['total_addresses']:,}")
        print(f"Total skipped: {result['total_skipped']:,}")
        print(f"Total batches: {result['total_batches']}")
        print(f"Expected max batches: {result['files_processed'] * 2}")
        
    except Exception as e:
        logger.error(f"Error: {e}")
        return 1
    
    finally:
        if processor:
            processor.close()
    
    return 0

if __name__ == "__main__":
    sys.exit(main())