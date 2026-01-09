#!/usr/bin/env python3
"""
Delete and Update Addresses
Deletes validated_addresses and updates address_batches status for specified countries
"""

import os
import time
from typing import List
from pymongo import MongoClient
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

class AddressDeleterUpdater:
    """Deletes validated addresses and updates address_batches status for specified countries"""
    
    def __init__(self):
        self.client = self._connect_mongodb()
        self.db = self.client[os.getenv('DB_NAME', 'osm_addresses')]
        self.validated_addresses = self.db.validated_addresses
        self.address_batches = self.db.address_batches
        
        # Countries to process (from delete/.md)
        self.countries = [
            "Martinique",
            "French Guiana", 
            "French Polynesia",
            "Guadeloupe",
            "Reunion",
            "Mayotte",
            "New Caledonia",
            "Puerto Rico",
            "Guam",
            "U.S. Virgin Islands",
            "Hong Kong",
            "Macao"
        ]
        
    def _connect_mongodb(self):
        """Connect to MongoDB using environment variables"""
        mongodb_uri = os.getenv('MONGODB_URI')
        if not mongodb_uri:
            raise ValueError("MONGODB_URI not found in environment variables")
        
        client = MongoClient(mongodb_uri, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')  # Test connection
        logger.info("✅ Connected to MongoDB")
        return client
    
    def delete_validated_addresses(self, country: str) -> int:
        """Delete validated addresses for a specific country"""
        try:
            result = self.validated_addresses.delete_many({"country": country})
            deleted_count = result.deleted_count
            logger.info(f"🗑️  Deleted {deleted_count} validated addresses for {country}")
            return deleted_count
        except Exception as e:
            logger.error(f"Error deleting validated addresses for {country}: {e}")
            return 0
    
    def update_address_batches_status(self, country: str) -> int:
        """Update address_batches status to 'origin' where status != 'origin'"""
        try:
            result = self.address_batches.update_many(
                {
                    "country_name": country,
                    "status": {"$ne": "origin"}
                },
                {"$set": {"status": "origin"}}
            )
            updated_count = result.modified_count
            logger.info(f"🔄 Updated {updated_count} address_batches to 'origin' status for {country}")
            return updated_count
        except Exception as e:
            logger.error(f"Error updating address_batches for {country}: {e}")
            return 0
    
    def process_country(self, country: str) -> dict:
        """Process a single country - delete validated addresses and update batches"""
        logger.info(f"🔄 Processing {country}...")
        
        # Delete validated addresses
        deleted_count = self.delete_validated_addresses(country)
        
        # Update address_batches status
        updated_count = self.update_address_batches_status(country)
        
        return {
            "deleted_validated": deleted_count,
            "updated_batches": updated_count
        }
    
    def process_all_countries(self):
        """Process all countries in the list"""
        logger.info("🚀 Starting address deletion and update process...")
        
        total_stats = {
            "deleted_validated": 0,
            "updated_batches": 0,
            "processed_countries": 0
        }
        
        start_time = time.time()
        
        for i, country in enumerate(self.countries, 1):
            logger.info(f"📊 Progress: {i}/{len(self.countries)} - Processing {country}")
            
            try:
                stats = self.process_country(country)
                
                # Accumulate stats
                total_stats["deleted_validated"] += stats["deleted_validated"]
                total_stats["updated_batches"] += stats["updated_batches"]
                total_stats["processed_countries"] += 1
                
            except Exception as e:
                logger.error(f"Error processing {country}: {e}")
                continue
        
        elapsed = time.time() - start_time
        
        # Final summary
        logger.info("=" * 60)
        logger.info("✅ Address deletion and update process completed!")
        logger.info(f"📊 Total Statistics:")
        logger.info(f"   Countries processed: {total_stats['processed_countries']}")
        logger.info(f"   Validated addresses deleted: {total_stats['deleted_validated']:,}")
        logger.info(f"   Address batches updated: {total_stats['updated_batches']:,}")
        logger.info(f"⏱️  Total time: {elapsed:.1f} seconds")
        logger.info("=" * 60)
        
        return total_stats
    
    def close(self):
        """Close database connection"""
        if self.client:
            self.client.close()
            logger.info("MongoDB connection closed")

def main():
    """Main function to run the address deleter/updater"""
    processor = None
    
    try:
        processor = AddressDeleterUpdater()
        
        # Show countries that will be processed
        print("🏴 Countries to be processed:")
        for country in processor.countries:
            print(f"   - {country}")
        
        # Confirm before proceeding
        response = input("\n⚠️  This will DELETE validated addresses and UPDATE address batches. Continue? (y/N): ")
        if response.lower() != 'y':
            print("Operation cancelled.")
            return 0
        
        stats = processor.process_all_countries()
        
        print(f"\n🎉 Process completed successfully!")
        print(f"📈 Final Results:")
        print(f"   🗑️  Deleted: {stats['deleted_validated']:,} validated addresses")
        print(f"   🔄 Updated: {stats['updated_batches']:,} address batches to 'origin' status")
        print(f"   📊 Processed: {stats['processed_countries']} countries")
        
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        return 1
    
    finally:
        if processor:
            processor.close()
    
    return 0

if __name__ == "__main__":
    import sys
    sys.exit(main())