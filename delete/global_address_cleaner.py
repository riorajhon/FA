#!/usr/bin/env python3
"""
Global Address Cleaner
Loops through ALL addresses in the database and validates them using looks_like_address 
and validate_address_region functions, deleting invalid addresses in batches.

Requirements: pip install pymongo
Usage: python global_address_cleaner.py [batch_size]
Example: python global_address_cleaner.py 1000
"""

import os
import sys
import time
from typing import List, Dict
from pymongo import MongoClient, DeleteMany
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Import validation functions from basic module
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from basic.address_check import looks_like_address, validate_address_region

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

class GlobalAddressCleaner:
    """Validates ALL addresses from database and deletes invalid ones in batches"""
    
    def __init__(self, mongodb_uri=None, batch_size=1000):
        # Use environment variable or default
        if mongodb_uri is None:
            mongodb_uri = os.getenv('MONGODB_URI', 'mongodb://admin:fjkfjrj!20020415@localhost:27017/?authSource=admin')
        
        self.client = MongoClient(mongodb_uri)
        self.db = self.client.osm_addresses
        self.addresses_collection = self.db.validated_addresses
        self.batch_size = batch_size
        
        # Statistics
        self.stats = {
            'total_processed': 0,
            'valid_addresses': 0,
            'invalid_addresses': 0,
            'deleted_addresses': 0,
            'batches_processed': 0,
            'countries_processed': set(),
            'validation_errors': 0
        }
    
    def get_addresses_batch(self, skip: int, limit: int) -> List[Dict]:
        """Get a batch of addresses from database (all countries)"""
        try:
            # No country filter - get ALL addresses
            cursor = self.addresses_collection.find({}).skip(skip).limit(limit)
            addresses = list(cursor)
            return addresses
        except Exception as e:
            logger.error(f"Error fetching address batch: {e}")
            return []
    
    def get_total_address_count(self) -> int:
        """Get total count of ALL addresses in database"""
        try:
            count = self.addresses_collection.count_documents({})
            return count
        except Exception as e:
            logger.error(f"Error counting addresses: {e}")
            return 0
    
    def validate_address(self, address: str, country_name: str) -> bool:
        """Validate a single address using the validation functions"""
        try:
            # First check if it looks like an address
            if not looks_like_address(address):
                return False
            
            # Then validate the address region
            if not validate_address_region(address, country_name):
                return False
            
            return True
        except Exception as e:
            logger.warning(f"Error validating address '{address[:50]}...': {e}")
            self.stats['validation_errors'] += 1
            return False
    
    def process_addresses_batch(self, addresses: List[Dict]) -> List[str]:
        """Process a batch of addresses and return IDs of invalid ones"""
        invalid_ids = []
        
        for address_doc in addresses:
            address_text = address_doc.get('address', '')
            country_name = address_doc.get('country', '')
            address_id = address_doc.get('_id')
            
            if not address_text or not address_id or not country_name:
                # Missing required fields - consider invalid
                if address_id:
                    invalid_ids.append(address_id)
                    self.stats['invalid_addresses'] += 1
                continue
            
            self.stats['total_processed'] += 1
            self.stats['countries_processed'].add(country_name)
            
            # Validate the address
            is_valid = self.validate_address(address_text, country_name)
            
            if is_valid:
                self.stats['valid_addresses'] += 1
            else:
                self.stats['invalid_addresses'] += 1
                invalid_ids.append(address_id)
                logger.debug(f"Invalid address [{country_name}]: {address_text[:50]}...")
        
        return invalid_ids
    
    def delete_addresses_batch(self, invalid_ids: List[str]) -> int:
        """Delete a batch of invalid addresses from database"""
        if not invalid_ids:
            return 0
        
        try:
            # Use bulk delete operation
            result = self.addresses_collection.delete_many({'_id': {'$in': invalid_ids}})
            deleted_count = result.deleted_count
            self.stats['deleted_addresses'] += deleted_count
            logger.info(f"Deleted {deleted_count} invalid addresses")
            return deleted_count
        except Exception as e:
            logger.error(f"Error deleting addresses: {e}")
            return 0
    
    def clean_all_addresses(self):
        """Main function to clean ALL addresses in database"""
        print(f"🌍 Starting GLOBAL address validation and cleanup")
        print(f"📦 Batch size: {self.batch_size}")
        print("=" * 80)
        
        # Get total count of ALL addresses
        total_addresses = self.get_total_address_count()
        
        if total_addresses == 0:
            print(f"❌ No addresses found in database")
            return
        
        print(f"📊 Total addresses to validate: {total_addresses:,}")
        
        # Process addresses in batches using skip and limit
        skip = 0
        batch_number = 0
        start_time = time.time()
        
        while skip < total_addresses:
            batch_number += 1
            
            print(f"\n🔄 Processing batch {batch_number}: {skip + 1}-{min(skip + self.batch_size, total_addresses)}")
            
            # Get batch of addresses from database (ALL countries)
            batch_addresses = self.get_addresses_batch(skip, self.batch_size)
            
            if not batch_addresses:
                print(f"   ⚠️  No addresses returned for batch {batch_number}")
                break
            
            self.stats['batches_processed'] += 1
            
            # Validate addresses in this batch
            invalid_ids = self.process_addresses_batch(batch_addresses)
            
            # Delete invalid addresses
            if invalid_ids:
                deleted_count = self.delete_addresses_batch(invalid_ids)
                print(f"   ❌ Deleted {deleted_count} invalid addresses")
            else:
                print(f"   ✅ All addresses in batch are valid")
            
            # Show progress and performance
            processed_so_far = min(skip + self.batch_size, total_addresses)
            progress = (processed_so_far / total_addresses) * 100
            elapsed = time.time() - start_time
            rate = self.stats['total_processed'] / elapsed if elapsed > 0 else 0
            
            print(f"   📈 Progress: {progress:.1f}% ({processed_so_far:,}/{total_addresses:,})")
            print(f"   🚀 Rate: {rate:.0f} addresses/sec | Countries: {len(self.stats['countries_processed'])}")
            
            # Move to next batch
            skip += self.batch_size
            
            # Small delay to avoid overwhelming the database
            time.sleep(0.1)
        
        # Print final statistics
        self.print_final_stats(start_time)
    
    def print_final_stats(self, start_time: float):
        """Print final cleanup statistics"""
        total_time = time.time() - start_time
        
        print(f"\n{'='*80}")
        print(f"🏁 GLOBAL CLEANUP COMPLETE")
        print(f"{'='*80}")
        print(f"📊 Total Processed: {self.stats['total_processed']:,}")
        print(f"✅ Valid Addresses: {self.stats['valid_addresses']:,}")
        print(f"❌ Invalid Addresses: {self.stats['invalid_addresses']:,}")
        print(f"🗑️  Deleted Addresses: {self.stats['deleted_addresses']:,}")
        print(f"📦 Batches Processed: {self.stats['batches_processed']:,}")
        print(f"🌍 Countries Processed: {len(self.stats['countries_processed']):,}")
        
        if self.stats['validation_errors'] > 0:
            print(f"⚠️  Validation Errors: {self.stats['validation_errors']:,}")
        
        if self.stats['total_processed'] > 0:
            valid_percentage = (self.stats['valid_addresses'] / self.stats['total_processed']) * 100
            invalid_percentage = (self.stats['invalid_addresses'] / self.stats['total_processed']) * 100
            print(f"📈 Valid Rate: {valid_percentage:.1f}%")
            print(f"📉 Invalid Rate: {invalid_percentage:.1f}%")
        
        print(f"\n⏱️  Total Time: {total_time:.2f} seconds")
        if self.stats['total_processed'] > 0:
            rate = self.stats['total_processed'] / total_time
            print(f"🚀 Processing Rate: {rate:.0f} addresses/second")
        
        # Show top countries processed
        if self.stats['countries_processed']:
            countries_list = sorted(list(self.stats['countries_processed']))
            print(f"\n🌍 Countries Processed ({len(countries_list)}):")
            # Show first 10 countries
            for i, country in enumerate(countries_list[:10]):
                print(f"   {i+1}. {country}")
            if len(countries_list) > 10:
                print(f"   ... and {len(countries_list) - 10} more countries")
        
        print(f"{'='*80}\n")
    
    def get_database_info(self):
        """Get information about the database before starting"""
        try:
            # Get collection stats
            stats = self.db.command("collStats", "validated_addresses")
            size_mb = stats.get('size', 0) / (1024 * 1024)
            
            # Get unique countries
            countries = self.addresses_collection.distinct('country')
            
            print(f"📊 Database Information:")
            print(f"   Collection Size: {size_mb:.1f} MB")
            print(f"   Unique Countries: {len(countries)}")
            print(f"   Sample Countries: {', '.join(countries[:5])}")
            if len(countries) > 5:
                print(f"   ... and {len(countries) - 5} more")
            print()
            
        except Exception as e:
            logger.warning(f"Could not get database info: {e}")
    
    def close(self):
        """Close database connection"""
        self.client.close()

def main():
    """Main function"""
    if len(sys.argv) > 2:
        print("Usage: python global_address_cleaner.py [batch_size]")
        print("Example: python global_address_cleaner.py 1000")
        print("Example: python global_address_cleaner.py")
        sys.exit(1)
    
    batch_size = 1000  # Default batch size
    
    if len(sys.argv) == 2:
        try:
            batch_size = int(sys.argv[1])
            if batch_size <= 0:
                raise ValueError("Batch size must be positive")
        except ValueError as e:
            print(f"Error: Invalid batch size. {e}")
            sys.exit(1)
    
    # Create cleaner instance
    cleaner = GlobalAddressCleaner(batch_size=batch_size)
    
    try:
        # Show database info
        cleaner.get_database_info()
        
        # Confirm before starting
        print("⚠️  WARNING: This will validate and potentially delete invalid addresses from ALL countries!")
        response = input("Do you want to continue? (yes/no): ").lower().strip()
        
        if response not in ['yes', 'y']:
            print("❌ Operation cancelled by user")
            return
        
        # Start cleanup process
        start_time = time.time()
        cleaner.clean_all_addresses()
        
    except KeyboardInterrupt:
        print("\n⏹️  Process interrupted by user")
    except Exception as e:
        logger.error(f"Error during cleanup: {e}")
        sys.exit(1)
    finally:
        cleaner.close()

if __name__ == "__main__":
    main()