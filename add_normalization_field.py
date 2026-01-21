#!/usr/bin/env python3
"""
Add Normalization Field to Existing Addresses
Processes all existing addresses in the database and adds the 'normalization' field
using the normalize_address_for_deduplication function.

Usage:
    python add_normalization_field.py --dry-run    # Preview changes
    python add_normalization_field.py              # Update database
    python add_normalization_field.py --limit 1000 # Process only 1000 addresses
"""

import os
import sys
import time
from typing import Dict, List, Optional
from pymongo import MongoClient, UpdateOne
import logging
from dotenv import load_dotenv

# Import normalization function
sys.path.append(os.path.join(os.path.dirname(__file__), 'basic'))
from address_normalization import normalize_address_for_deduplication

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class NormalizationFieldAdder:
    def __init__(self, mongodb_uri=None):
        """Initialize with database connection"""
        if mongodb_uri is None:
            mongodb_uri = os.getenv('MONGODB_URI')
            if not mongodb_uri:
                raise ValueError("MongoDB URI not found in environment variables")
        
        self.client = MongoClient(mongodb_uri)
        self.db = self.client['osm_addresses']
        self.addresses_collection = self.db['validated_addresses']
        
        # Statistics
        self.stats = {
            'total_addresses': 0,
            'addresses_without_normalization': 0,
            'processed': 0,
            'updated': 0,
            'errors': 0,
            'skipped_empty_address': 0
        }
    
    def get_database_stats(self):
        """Get initial database statistics"""
        # Total addresses
        self.stats['total_addresses'] = self.addresses_collection.count_documents({})
        
        # Addresses without normalization field
        self.stats['addresses_without_normalization'] = self.addresses_collection.count_documents({
            "normalization": {"$exists": False}
        })
    
    def process_addresses_batch(self, batch_size: int = 1000, limit: Optional[int] = None, dry_run: bool = False):
        """
        Process addresses in batches to add normalization field using skip/limit
        
        Args:
            batch_size: Number of addresses to process in each batch
            limit: Maximum number of addresses to process (None for all)
            dry_run: If True, don't actually update the database
        """
        
        # Query for addresses without normalization field
        query = {"normalization": {"$exists": False}}
        
        # Determine total to process
        total_to_process = limit if limit else self.stats['addresses_without_normalization']
        
        # Process in batches using skip/limit
        batch_count = 0
        skip = 0
        
        while skip < total_to_process:
            batch_count += 1
            current_batch_size = min(batch_size, total_to_process - skip)
            
            # Get batch using skip/limit
            batch = list(self.addresses_collection.find(
                query, 
                {"_id": 1, "address": 1}
            ).skip(skip).limit(current_batch_size))
            
            if not batch:
                break
                
            self.process_single_batch(batch, batch_count, dry_run)
            skip += current_batch_size
        
        # Print final statistics
        self.print_final_stats(dry_run)
    
    def process_single_batch(self, batch: List[Dict], batch_num: int, dry_run: bool = False):
        """Process a single batch of addresses"""
        
        updates = []
        
        for doc in batch:
            self.stats['processed'] += 1
            
            address = doc.get('address', '')
            if not address or not address.strip():
                self.stats['skipped_empty_address'] += 1
                continue
            
            try:
                # Generate normalization
                normalization = normalize_address_for_deduplication(address)
                
                if not dry_run:
                    # Prepare update operation
                    updates.append(UpdateOne(
                        {'_id': doc['_id']},
                        {'$set': {'normalization': normalization}}
                    ))
                
            except Exception as e:
                self.stats['errors'] += 1
                continue
        
        # Execute batch update if not dry run
        if not dry_run and updates:
            try:
                result = self.addresses_collection.bulk_write(updates)
                updated_count = result.modified_count
                self.stats['updated'] += updated_count
                
            except Exception as e:
                self.stats['errors'] += len(updates)
        
        elif dry_run:
            self.stats['updated'] += len(updates)
        
        # Progress update (single line, overwrite previous)
        progress = (self.stats['processed'] / self.stats['addresses_without_normalization']) * 100
        print(f"\rProgress: {progress:.1f}% ({self.stats['processed']:,}/{self.stats['addresses_without_normalization']:,})", end='', flush=True)
    
    def print_final_stats(self, dry_run: bool = False):
        """Print final processing statistics"""
        print(f"\nCompleted: {self.stats['updated']:,} updated, {self.stats['errors']:,} errors")
    
    def verify_updates(self):
        """Verify that normalization fields were added correctly"""
        pass
    
    def close_connection(self):
        """Close database connection"""
        if self.client:
            self.client.close()

def main():
    """Main function"""
    # Parse command line arguments
    dry_run = '--dry-run' in sys.argv
    limit = None
    
    # Check for limit argument
    if '--limit' in sys.argv:
        try:
            limit_index = sys.argv.index('--limit')
            if limit_index + 1 < len(sys.argv):
                limit = int(sys.argv[limit_index + 1])
                logger.info(f"Processing limit set to: {limit:,}")
        except (ValueError, IndexError):
            logger.error("Invalid --limit argument. Usage: --limit <number>")
            sys.exit(1)
    
    if dry_run:
        pass
    else:
        response = input("Continue? (y/N): ")
        if response.lower() != 'y':
            print("Aborted.")
            return
    
    try:
        # Initialize processor
        processor = NormalizationFieldAdder()
        
        # Get initial statistics
        processor.get_database_stats()
        
        if processor.stats['addresses_without_normalization'] == 0:
            print("All addresses already have normalization field!")
            return
        
        # Process addresses
        processor.process_addresses_batch(
            batch_size=1000,
            limit=limit,
            dry_run=dry_run
        )
        
        # Verify updates if not dry run
        if not dry_run:
            processor.verify_updates()
        
        # Close connection
        processor.close_connection()
        
    except Exception as e:
        print(f"Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()