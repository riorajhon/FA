#!/usr/bin/env python3
"""
Main OSM Processor
Automatically processes all countries from database with status "origin"
Finds OSM files and processes them using osm_processor.py

Requirements: pip install pymongo
Usage: python main_osm.py
"""

import os
import sys
import time
import requests
from typing import Dict, Optional
from pymongo import MongoClient
import logging

# Import the processing function and URLs
# from osm_processor import process_osm_file
from osm import process_osm_file
from basic.urls import GEOFABRIK_URLS

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

class MainOSMProcessor:
    """Main processor that handles all countries automatically"""
    
    def __init__(self, mongodb_uri="mongodb://admin:fjkfjrj!20020415@localhost:27017/?authSource=admin"):
        self.client = MongoClient(mongodb_uri)
        self.db = self.client.osm_addresses
        self.countries_collection = self.db.country_status
        self.osm_data_dir = "osm_data"
        
    def get_next_country(self) -> Optional[Dict]:
        """Get next country with status 'origin' from database"""
        country = self.countries_collection.find_one({'status': 'origin'})
        return country
    
    def update_country_status(self, country_id, status: str):
        """Update country status in database"""
        self.countries_collection.update_one(
            {'_id': country_id},
            {'$set': {'status': status}}
        )
    
    def download_osm_file(self, country_code: str) -> Optional[str]:
        """Download OSM file for country code"""
        if country_code not in GEOFABRIK_URLS:
            logger.warning(f"No download URL found for country code: {country_code}")
            return None
        
        url = GEOFABRIK_URLS[country_code]
        filename = f"{country_code.lower()}-latest.osm.pbf"
        file_path = os.path.join(self.osm_data_dir, filename)
        
        print(f"📥 Downloading OSM file for {country_code}...")
        print(f"🔗 URL: {url}")
        
        try:
            # Create osm_data directory if it doesn't exist
            os.makedirs(self.osm_data_dir, exist_ok=True)
            
            # Download with progress
            response = requests.get(url, stream=True)
            response.raise_for_status()
            
            total_size = int(response.headers.get('content-length', 0))
            downloaded = 0
            
            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        
                        if total_size > 0:
                            progress = (downloaded / total_size) * 100
                            print(f"\r📊 Download: {progress:.1f}% ({downloaded:,}/{total_size:,} bytes)", end='', flush=True)
            
            print(f"\n✅ Downloaded: {filename}")
            return filename
            
        except Exception as e:
            logger.error(f"Failed to download OSM file for {country_code}: {e}")
            # Clean up partial download
            if os.path.exists(file_path):
                os.remove(file_path)
            return None
    
    def find_osm_file(self, country_code: str) -> Optional[str]:
        """Find OSM file for country code, download if not found"""
        # Convert country code to lowercase for filename
        filename = f"{country_code.lower()}-latest.osm.pbf"
        file_path = os.path.join(self.osm_data_dir, filename)
        
        if os.path.exists(file_path):
            return filename
        
        logger.info(f"OSM file not found: {file_path}")
        print(f"📁 OSM file not found locally, attempting download...")
        return
        # Try to download the file
        # downloaded_filename = self.download_osm_file(country_code)
        # return downloaded_filename
    
    def process_country_osm(self, filename: str, country_code: str, country_name: str) -> bool:
        """Process country using osm_processor.py function"""
        try:
            logger.info(f"Processing {country_name} ({country_code}) with file {filename}")
            
            # Call the function directly instead of subprocess
            stats = process_osm_file(filename, country_code, country_name)
            
            logger.info(f"Successfully processed {country_name}: {stats['addresses']} addresses, {stats['batches']} batches")
            return True
                
        except Exception as e:
            logger.error(f"Failed to process {country_name}: {e}")
            return False
    
    def run_continuous_processing(self):
        """Continuously process countries until none left with status 'origin'"""
        print("🚀 Starting continuous OSM processing...")
        print("🔍 Looking for countries with status 'origin'...")
        
        processed_count = 0
        skipped_count = 0
        
        while True:
            # Get next country to process
            country = self.get_next_country()
            
            if not country:
                print(f"\n✅ All countries processed!")
                print(f"📊 Total processed: {processed_count}")
                print(f"⏭️  Total skipped: {skipped_count}")
                break
            
            country_id = country['_id']
            country_code = country['country_code']
            country_name = country['country_name']
            
            print(f"\n🔄 Processing: {country_name} ({country_code})")
            
            # Update status to 'processing'
            self.update_country_status(country_id, 'processing')
            
            # Find OSM file
            filename = self.find_osm_file(country_code)
            
            if not filename:
                print(f"⏭️  Skipping {country_name} - OSM file not found (keeping status 'origin')")
                skipped_count += 1
                continue
            
            print(f"📁 Found OSM file: {filename}")
            
            # Process the country
            success = self.process_country_osm(filename, country_code, country_name)
            
            if success:
                # Update status to 'completed'
                self.update_country_status(country_id, 'completed')
                processed_count += 1
                print(f"✅ Completed: {country_name}")
            else:
                # Update status to 'failed'
                self.update_country_status(country_id, 'failed')
                print(f"❌ Failed: {country_name}")
            
            # Small delay between countries
            time.sleep(2)
    
    def show_status(self):
        """Show current processing status"""
        pipeline = [
            {'$group': {
                '_id': '$status',
                'count': {'$sum': 1}
            }}
        ]
        
        status_counts = {}
        for result in self.countries_collection.aggregate(pipeline):
            status_counts[result['_id']] = result['count']
        
        print("📊 Current Status:")
        for status in ['origin', 'processing', 'completed', 'failed']:
            count = status_counts.get(status, 0)
            print(f"   {status}: {count}")
    
    def close(self):
        """Close database connection"""
        self.client.close()

if __name__ == "__main__":
    processor = MainOSMProcessor()
    
    try:
        # Show initial status
        processor.show_status()
        
        # Start processing
        processor.run_continuous_processing()
        
        # Show final status
        print("\n📊 Final Status:")
        processor.show_status()
        
    except KeyboardInterrupt:
        print("\n⏹️  Processing interrupted by user")
    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)
    finally:
        processor.close()