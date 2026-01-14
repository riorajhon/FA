#!/usr/bin/env python3
"""
Update Country Status to Origin
Reads countries from basic/countries_without_batches.json and updates their status to 'origin' in DB

Requirements: pip install pymongo
Usage: python update_country_status.py
"""

import os
import sys
import json
from pymongo import MongoClient
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

class CountryStatusUpdater:
    """Updates country status in database"""
    
    def __init__(self, mongodb_uri=None):
        if mongodb_uri is None:
            mongodb_uri = os.getenv('MONGODB_URI', 'mongodb://admin:fjkfjrj!20020415@localhost:27017/?authSource=admin')
        
        self.client = MongoClient(mongodb_uri)
        self.db = self.client.osm_addresses
        self.countries_collection = self.db.country_status
    
    def load_countries_from_json(self, json_file):
        """Load country names from JSON file"""
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                countries = json.load(f)
            logger.info(f"Loaded {len(countries)} countries from {json_file}")
            return countries
        except Exception as e:
            logger.error(f"Error loading JSON file: {e}")
            return []
    
    def update_country_status(self, country_name, new_status='origin'):
        """Update single country status"""
        try:
            result = self.countries_collection.update_one(
                {'country_name': country_name},
                {'$set': {'status': new_status}}
            )
            
            if result.matched_count > 0:
                logger.info(f"✅ Updated: {country_name} -> {new_status}")
                return True
            else:
                logger.warning(f"⚠️  Not found in DB: {country_name}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error updating {country_name}: {e}")
            return False
    
    def update_all_countries(self, countries, new_status='origin'):
        """Update status for all countries in list"""
        updated_count = 0
        not_found_count = 0
        
        print(f"🔄 Updating {len(countries)} countries to status '{new_status}'...\n")
        
        for country_name in countries:
            success = self.update_country_status(country_name, new_status)
            if success:
                updated_count += 1
            else:
                not_found_count += 1
        
        print(f"\n📊 Summary:")
        print(f"   ✅ Updated: {updated_count}")
        print(f"   ⚠️  Not found: {not_found_count}")
        print(f"   📝 Total: {len(countries)}")
        
        return updated_count, not_found_count
    
    def show_status_summary(self):
        """Show current status distribution"""
        pipeline = [
            {'$group': {
                '_id': '$status',
                'count': {'$sum': 1}
            }}
        ]
        
        status_counts = {}
        for result in self.countries_collection.aggregate(pipeline):
            status_counts[result['_id']] = result['count']
        
        print("\n📊 Current Status Distribution:")
        for status in ['origin', 'processing', 'completed', 'failed']:
            count = status_counts.get(status, 0)
            print(f"   {status}: {count}")
    
    def close(self):
        """Close database connection"""
        self.client.close()

if __name__ == "__main__":
    json_file = "basic/countries_without_batches.json"
    
    if not os.path.exists(json_file):
        print(f"❌ Error: File not found: {json_file}")
        sys.exit(1)
    
    updater = CountryStatusUpdater()
    
    try:
        # Show initial status
        print("📊 Before Update:")
        updater.show_status_summary()
        
        # Load countries from JSON
        countries = updater.load_countries_from_json(json_file)
        
        if not countries:
            print("❌ No countries to update")
            sys.exit(1)
        
        # Update all countries
        updated, not_found = updater.update_all_countries(countries, 'origin')
        
        # Show final status
        print("\n📊 After Update:")
        updater.show_status_summary()
        
    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)
    finally:
        updater.close()
