#!/usr/bin/env python3
"""
Batch First Section Processor
Loops through all addresses in database and adds first_section field
Uses duplication/first_section.py/extract_first_section function

Requirements: pip install pymongo python-dotenv
Usage: python batch_first_section_processor.py
"""

import os
import sys
import time
from pymongo import MongoClient, UpdateOne
from dotenv import load_dotenv
import logging

# Import first section extraction function
from first_section import extract_first_section

# Load environment variables
load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

class BatchFirstSectionProcessor:
    """Processes addresses in batches to add first_section field"""
    
    def __init__(self):
        self.client = self._connect_mongodb()
        self.db = self.client[os.getenv('DB_NAME', 'osm_addresses')]
        self.collection = self.db.validated_addresses
        
        self.batch_size = 100  # Process 100 addresses at a time (user updated)
        self.update_batch_size = 50  # Update 50 at a time
        
        # Counters
        self.total_processed = 0
        self.total_updated = 0
        self.total_skipped = 0
        
    def _connect_mongodb(self):
        """Connect to MongoDB using environment variables"""
        mongodb_uri = os.getenv('MONGODB_URI')
        if not mongodb_uri:
            raise ValueError("MONGODB_URI not found in environment variables")
        
        client = MongoClient(mongodb_uri, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        return client
    
    def count_addresses_without_first_section(self):
        """Count addresses that don't have first_section field or have empty values"""
        query = {
            "$or": [
                {"first_section": {"$exists": False}},
                {"first_section": ""},
                {"first_section": None}
            ]
        }
        count = self.collection.count_documents(query)
        return count
    
    def count_total_addresses(self):
        """Count total addresses in collection"""
        return self.collection.count_documents({})
    
    def process_batch(self, skip=0):
        """Process a batch of addresses"""
        # Find addresses without first_section field or with empty values
        query = {
            "$or": [
                {"first_section": {"$exists": False}},
                {"first_section": ""},
                {"first_section": None}
            ]
        }
        cursor = self.collection.find(query).skip(skip).limit(self.batch_size)
        
        addresses = list(cursor)
        if not addresses:
            return 0
        
        print(f"Processing batch of {len(addresses)} addresses (skip: {skip:,})")
        
        # Process addresses in smaller update batches
        processed = 0
        for i in range(0, len(addresses), self.update_batch_size):
            batch = addresses[i:i + self.update_batch_size]
            self._update_batch(batch)
            processed += len(batch)
        
        return len(addresses)
    
    def _update_batch(self, addresses):
        """Update a batch of addresses with first_section field"""
        bulk_operations = []
        batch_processed = 0
        batch_updated = 0
        batch_skipped = 0
        
        for addr_doc in addresses:
            address_text = addr_doc.get('address', '')
            batch_processed += 1
            
            if not address_text or not address_text.strip():
                batch_skipped += 1
                continue
            
            # Extract first section
            first_section = extract_first_section(address_text)
            
            # Handle empty first_section with fallback
            if not first_section or not first_section.strip():
                # Use original address as fallback
                first_section = address_text.strip()
            
            # Prepare update operation using UpdateOne
            bulk_operations.append(
                UpdateOne(
                    {'_id': addr_doc['_id']},
                    {'$set': {'first_section': first_section}}
                )
            )
            batch_updated += 1
        
        # Execute bulk update
        if bulk_operations:
            try:
                result = self.collection.bulk_write(bulk_operations, ordered=False)
                self.total_updated += result.modified_count
                print(f"    Processed: {batch_processed} | Updated: {result.modified_count} | Skipped: {batch_skipped}")
            except Exception as e:
                logger.error(f"Error updating batch: {e}")
        
        self.total_processed += batch_processed
        self.total_skipped += batch_skipped
    
    def process_all_addresses(self):
        """Process all addresses in database"""
        print("🚀 Starting Batch First Section Processing")
        print("=" * 60)
        
        # Get initial counts
        total_addresses = self.count_total_addresses()
        addresses_without_first_section = self.count_addresses_without_first_section()
        
        print(f"📊 Database Statistics:")
        print(f"    Total addresses: {total_addresses:,}")
        print(f"    Without first_section: {addresses_without_first_section:,}")
        print(f"    Already processed: {total_addresses - addresses_without_first_section:,}")
        
        if addresses_without_first_section == 0:
            print("✅ All addresses already have first_section field!")
            return
        
        print(f"\n🔄 Processing {addresses_without_first_section:,} addresses in batches of {self.batch_size:,}")
        
        start_time = time.time()
        skip = 0
        
        while True:
            batch_processed = self.process_batch(skip)
            
            if batch_processed == 0:
                break
            
            skip += batch_processed
            
            # Calculate progress and ETA
            elapsed = time.time() - start_time
            rate = self.total_processed / elapsed if elapsed > 0 else 0
            remaining = addresses_without_first_section - self.total_processed
            eta = remaining / rate if rate > 0 else 0
            
            print(f"\n📈 Total Progress: {self.total_processed:,} processed | {self.total_updated:,} updated | {self.total_skipped:,} skipped")
            print(f"⏱️  Rate: {rate:.0f} addresses/sec | ETA: {eta/60:.1f} minutes")
            print("-" * 60)
            
            # Small delay to prevent overwhelming the database
            time.sleep(0.1)
        
        # Final statistics
        elapsed = time.time() - start_time
        final_count = self.count_addresses_without_first_section()
        
        print(f"\n✅ Processing Complete!")
        print(f"📊 Final Statistics:")
        print(f"    Total processed: {self.total_processed:,} addresses")
        print(f"    Total updated: {self.total_updated:,} addresses")
        print(f"    Total skipped: {self.total_skipped:,} addresses")
        print(f"    Time taken: {elapsed/60:.1f} minutes")
        print(f"    Average rate: {self.total_processed/elapsed:.0f} addresses/sec")
        print(f"    Remaining without first_section: {final_count:,}")
        
        if final_count == 0:
            print("🎉 All addresses now have first_section field!")
    
    def close(self):
        """Close database connection"""
        if self.client:
            self.client.close()

def main():
    """Main function"""
    processor = None
    
    try:
        processor = BatchFirstSectionProcessor()
        processor.process_all_addresses()
        
    except KeyboardInterrupt:
        print("\n⏹️  Processing interrupted by user")
    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)
    
    finally:
        if processor:
            processor.close()

if __name__ == "__main__":
    main()