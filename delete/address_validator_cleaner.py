#!/usr/bin/env python3
"""
Address Validator Cleaner
Gets addresses from database by country name, validates them using looks_like_address 
and validate_address_region functions, and deletes invalid addresses in batches.

Requirements: pip install pymongo
Usage: python address_validator_cleaner.py <country_name> [batch_size]
Example: python address_validator_cleaner.py "United States" 1000
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

class AddressValidatorCleaner:
    """Validates addresses from database and deletes invalid ones in batches"""
    
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
            'batches_processed': 0
        }
    
    def get_addresses_batch(self, country_name: str, skip: int, limit: int) -> List[Dict]:
        """Get a batch of addresses for a specific country from database"""
        try:
            query = {'country': country_name}
            cursor = self.addresses_collection.find(query).skip(skip).limit(limit)
            addresses = list(cursor)
            return addresses
        except Exception as e:
            logger.error(f"Error fetching address batch for {country_name}: {e}")
            return []
    
    def get_total_address_count(self, country_name: str) -> int:
        """Get total count of addresses for a country"""
        try:
            query = {'country': country_name}
            count = self.addresses_collection.count_documents(query)
            return count
        except Exception as e:
            logger.error(f"Error counting addresses for {country_name}: {e}")
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
            return False
    
    def process_addresses_batch(self, addresses: List[Dict], country_name: str) -> List[str]:
        """Process a batch of addresses and return IDs of invalid ones"""
        invalid_ids = []
        
        for address_doc in addresses:
            address_text = address_doc.get('address', '')
            address_id = address_doc.get('_id')
            
            if not address_text or not address_id:
                continue
            
            self.stats['total_processed'] += 1
            
            # Validate the address
            is_valid = self.validate_address(address_text, country_name)
            
            if is_valid:
                self.stats['valid_addresses'] += 1
            else:
                self.stats['invalid_addresses'] += 1
                invalid_ids.append(address_id)
                logger.debug(f"Invalid address: {address_text[:50]}...")
        
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
    
    def clean_country_addresses(self, country_name: str):
        """Main function to clean addresses for a country using batch processing"""
        print(f"🧹 Starting address validation and cleanup for: {country_name}")
        print(f"📦 Batch size: {self.batch_size}")
        print("=" * 80)
        
        # Get total count of addresses for the country
        total_addresses = self.get_total_address_count(country_name)
        
        if total_addresses == 0:
            print(f"❌ No addresses found for {country_name}")
            return
        
        print(f"📊 Total addresses to validate: {total_addresses:,}")
        
        # Process addresses in batches using skip and limit
        skip = 0
        batch_number = 0
        
        while skip < total_addresses:
            batch_number += 1
            
            print(f"\n🔄 Processing batch {batch_number}: {skip + 1}-{min(skip + self.batch_size, total_addresses)}")
            
            # Get batch of addresses from database
            batch_addresses = self.get_addresses_batch(country_name, skip, self.batch_size)
            
            if not batch_addresses:
                print(f"   ⚠️  No addresses returned for batch {batch_number}")
                break
            
            self.stats['batches_processed'] += 1
            
            # Validate addresses in this batch
            invalid_ids = self.process_addresses_batch(batch_addresses, country_name)
            
            # Delete invalid addresses
            if invalid_ids:
                deleted_count = self.delete_addresses_batch(invalid_ids)
                print(f"   ❌ Deleted {deleted_count} invalid addresses")
            else:
                print(f"   ✅ All addresses in batch are valid")
            
            # Show progress
            processed_so_far = min(skip + self.batch_size, total_addresses)
            progress = (processed_so_far / total_addresses) * 100
            print(f"   📈 Progress: {progress:.1f}% ({processed_so_far:,}/{total_addresses:,})")
            
            # Move to next batch
            skip += self.batch_size
            
            # Small delay to avoid overwhelming the database
            time.sleep(0.1)
        
        # Print final statistics
        self.print_final_stats(country_name)
    
    def print_final_stats(self, country_name: str):
        """Print final cleanup statistics"""
        print(f"\n{'='*80}")
        print(f"🏁 CLEANUP COMPLETE FOR: {country_name}")
        print(f"{'='*80}")
        print(f"📊 Total Processed: {self.stats['total_processed']:,}")
        print(f"✅ Valid Addresses: {self.stats['valid_addresses']:,}")
        print(f"❌ Invalid Addresses: {self.stats['invalid_addresses']:,}")
        print(f"🗑️  Deleted Addresses: {self.stats['deleted_addresses']:,}")
        print(f"📦 Batches Processed: {self.stats['batches_processed']:,}")
        
        if self.stats['total_processed'] > 0:
            valid_percentage = (self.stats['valid_addresses'] / self.stats['total_processed']) * 100
            invalid_percentage = (self.stats['invalid_addresses'] / self.stats['total_processed']) * 100
            print(f"📈 Valid Rate: {valid_percentage:.1f}%")
            print(f"📉 Invalid Rate: {invalid_percentage:.1f}%")
        
        print(f"{'='*80}\n")
    
    def close(self):
        """Close database connection"""
        self.client.close()

def main():
    """Main function"""
    if len(sys.argv) < 2 or len(sys.argv) > 3:
        print("Usage: python address_validator_cleaner.py <country_name> [batch_size]")
        print("Example: python address_validator_cleaner.py \"United States\" 1000")
        print("Example: python address_validator_cleaner.py Germany")
        sys.exit(1)
    
    country_name = sys.argv[1]
    batch_size = 1000  # Default batch size
    
    if len(sys.argv) == 3:
        try:
            batch_size = int(sys.argv[2])
            if batch_size <= 0:
                raise ValueError("Batch size must be positive")
        except ValueError as e:
            print(f"Error: Invalid batch size. {e}")
            sys.exit(1)
    
    # Create cleaner instance
    cleaner = AddressValidatorCleaner(batch_size=batch_size)
    
    try:
        # Start cleanup process
        start_time = time.time()
        cleaner.clean_country_addresses(country_name)
        end_time = time.time()
        
        # Show total processing time
        total_time = end_time - start_time
        print(f"⏱️  Total processing time: {total_time:.2f} seconds")
        
        if cleaner.stats['total_processed'] > 0:
            rate = cleaner.stats['total_processed'] / total_time
            print(f"🚀 Processing rate: {rate:.0f} addresses/second")
        
    except KeyboardInterrupt:
        print("\n⏹️  Process interrupted by user")
    except Exception as e:
        logger.error(f"Error during cleanup: {e}")
        sys.exit(1)
    finally:
        cleaner.close()

if __name__ == "__main__":
    main()