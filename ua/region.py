#!/usr/bin/env python3
"""
Ukraine Region Processor
Updates country names for Ukrainian disputed regions in the database

Processes:
- Luhansk Oblast → Luhansk
- Republic of Crimea → Crimea  
- Donetsk Oblast → Donetsk

Usage: python ukraine_region_processor.py [--dry-run]
"""

import os
import sys
import re
from typing import Dict, List
from pymongo import MongoClient
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class UkraineRegionProcessor:
    def __init__(self, mongodb_uri=None):
        """Initialize the processor with database connection"""
        if mongodb_uri is None:
            mongodb_uri = os.getenv('MONGODB_URI')
            if not mongodb_uri:
                raise ValueError("MongoDB URI not found in environment variables")
        
        self.client = MongoClient(mongodb_uri)
        self.db = self.client['osm_addresses']
        self.addresses_collection = self.db['validated_addresses']
        
        # Define region mappings from .md file
        self.region_mappings = {
            "Luhansk Oblast": "Luhansk",
            "Republic of Crimea": "Crimea",
            "Donetsk Oblast": "Donetsk"
        }
        
        # Statistics
        self.stats = {
            'total_processed': 0,
            'total_updated': 0,
            'regions': {}
        }
        
        # Initialize region stats
        for region in self.region_mappings.keys():
            self.stats['regions'][region] = {
                'found': 0,
                'updated': 0
            }
    
    def find_addresses_with_region(self, region_name: str, dry_run: bool = False) -> List[Dict]:
        """
        Find addresses containing the specified region name
        
        Args:
            region_name: Name of the region to search for
            dry_run: If True, only count without returning documents
            
        Returns:
            List of matching address documents
        """
        try:
            # Create case-insensitive regex pattern
            pattern = re.compile(re.escape(region_name), re.IGNORECASE)
            
            # Query for addresses containing the region name
            query = {
                "address": {"$regex": pattern},
                "country": "Ukraine"  # Only process Ukrainian addresses
            }
            
            if dry_run:
                # Just count for dry run
                count = self.addresses_collection.count_documents(query)
                logger.info(f"Found {count} addresses containing '{region_name}'")
                return []
            else:
                # Return actual documents
                cursor = self.addresses_collection.find(query)
                addresses = list(cursor)
                logger.info(f"Found {len(addresses)} addresses containing '{region_name}'")
                return addresses
                
        except Exception as e:
            logger.error(f"Error finding addresses for region '{region_name}': {e}")
            return []
    
    def update_country_for_addresses(self, addresses: List[Dict], new_country: str, dry_run: bool = False) -> int:
        """
        Update country field for the given addresses
        
        Args:
            addresses: List of address documents to update
            new_country: New country name to set
            dry_run: If True, don't actually update
            
        Returns:
            Number of addresses updated
        """
        if not addresses:
            return 0
        
        if dry_run:
            logger.info(f"DRY RUN: Would update {len(addresses)} addresses to country '{new_country}'")
            return len(addresses)
        
        try:
            # Extract OSM IDs for bulk update
            osm_ids = [addr['osm_id'] for addr in addresses]
            
            # Perform bulk update
            result = self.addresses_collection.update_many(
                {"osm_id": {"$in": osm_ids}},
                {"$set": {"country": new_country}}
            )
            
            updated_count = result.modified_count
            logger.info(f"Updated {updated_count} addresses to country '{new_country}'")
            return updated_count
            
        except Exception as e:
            logger.error(f"Error updating addresses to country '{new_country}': {e}")
            return 0
    
    def process_single_region(self, region_name: str, new_country: str, dry_run: bool = False):
        """
        Process a single region mapping
        
        Args:
            region_name: Original region name to search for
            new_country: New country name to set
            dry_run: If True, don't make actual changes
        """
        logger.info(f"\n{'='*60}")
        logger.info(f"Processing: {region_name} → {new_country}")
        logger.info(f"{'='*60}")
        
        # Find addresses containing this region
        addresses = self.find_addresses_with_region(region_name, dry_run)
        
        # Update statistics
        self.stats['regions'][region_name]['found'] = len(addresses)
        
        if not addresses and not dry_run:
            logger.info(f"No addresses found for region '{region_name}'")
            return
        
        # Show sample addresses (first 3)
        if addresses and not dry_run:
            logger.info(f"Sample addresses found:")
            for i, addr in enumerate(addresses[:3], 1):
                logger.info(f"  {i}. {addr.get('address', 'N/A')[:100]}...")
            
            if len(addresses) > 3:
                logger.info(f"  ... and {len(addresses) - 3} more")
        
        # Update country field
        updated_count = self.update_country_for_addresses(addresses, new_country, dry_run)
        
        # Update statistics
        self.stats['regions'][region_name]['updated'] = updated_count
        self.stats['total_updated'] += updated_count
    
    def process_all_regions(self, dry_run: bool = False):
        """
        Process all Ukrainian regions
        
        Args:
            dry_run: If True, don't make actual changes
        """
        logger.info(f"🇺🇦 Starting Ukraine Region Processing")
        logger.info(f"Mode: {'DRY RUN' if dry_run else 'LIVE UPDATE'}")
        logger.info(f"Regions to process: {len(self.region_mappings)}")
        
        # Process each region mapping
        for region_name, new_country in self.region_mappings.items():
            try:
                self.process_single_region(region_name, new_country, dry_run)
                self.stats['total_processed'] += 1
                
            except Exception as e:
                logger.error(f"Error processing region '{region_name}': {e}")
                continue
        
        # Print final statistics
        self.print_final_stats(dry_run)
    
    def print_final_stats(self, dry_run: bool = False):
        """Print final processing statistics"""
        logger.info(f"\n{'='*60}")
        logger.info(f"FINAL STATISTICS {'(DRY RUN)' if dry_run else ''}")
        logger.info(f"{'='*60}")
        
        logger.info(f"Total regions processed: {self.stats['total_processed']}")
        logger.info(f"Total addresses {'would be ' if dry_run else ''}updated: {self.stats['total_updated']}")
        
        logger.info(f"\nRegion breakdown:")
        for region_name, stats in self.stats['regions'].items():
            new_country = self.region_mappings[region_name]
            logger.info(f"  {region_name} → {new_country}:")
            logger.info(f"    Found: {stats['found']} addresses")
            logger.info(f"    {'Would update' if dry_run else 'Updated'}: {stats['updated']} addresses")
    
    def verify_updates(self):
        """Verify that updates were applied correctly"""
        logger.info(f"\n🔍 Verifying updates...")
        
        for region_name, new_country in self.region_mappings.items():
            # Count addresses with the new country name that still contain the region
            pattern = re.compile(re.escape(region_name), re.IGNORECASE)
            count = self.addresses_collection.count_documents({
                "address": {"$regex": pattern},
                "country": new_country
            })
            
            logger.info(f"✅ {new_country}: {count} addresses verified")
    
    def close_connection(self):
        """Close database connection"""
        if self.client:
            self.client.close()

def main():
    """Main function"""
    # Parse command line arguments
    dry_run = '--dry-run' in sys.argv
    
    if dry_run:
        print("🧪 Running in DRY RUN mode - no changes will be made")
    else:
        print("⚠️  Running in LIVE mode - database will be modified")
        response = input("Continue? (y/N): ")
        if response.lower() != 'y':
            print("Aborted.")
            return
    
    try:
        # Initialize processor
        processor = UkraineRegionProcessor()
        
        # Process all regions
        processor.process_all_regions(dry_run=dry_run)
        
        # Verify updates if not dry run
        if not dry_run:
            processor.verify_updates()
        
        # Close connection
        processor.close_connection()
        
        print(f"\n✅ Processing complete!")
        
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()