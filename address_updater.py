#!/usr/bin/env python3
"""
Address Updater for Territories and Dependencies
Processes addresses for specific territories, trims them, validates them, and updates the database
Based on address_update.md specifications
"""

import os
import re
import time
from typing import List, Dict, Tuple
from pymongo import MongoClient, UpdateMany, DeleteMany
import logging
from dotenv import load_dotenv

# Import validation functions
from basic.address_check import looks_like_address, validate_address_region

# Load environment variables
load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

class AddressUpdater:
    """Updates addresses for specific territories and dependencies"""
    
    def __init__(self):
        self.client = self._connect_mongodb()
        self.db = self.client[os.getenv('DB_NAME', 'osm_addresses')]
        self.collection = self.db.validated_addresses
        
        # Define territories to process
        self.territories = [
            "Martinique",
            "French Guiana", 
            "French Polynesia",
            "Guadeloupe",
            "Réunion",
            "Mayotte",
            "New Caledonia",
            "Puerto Rico",
            "Guam",
            "United States Virgin Islands",
            "Hong Kong",
            "Macau"
        ]
        
        # Special processing rules
        self.special_rules = {
            "Réunion": {
                "address_replace": {"Réunion": "Reunion"},
                "country": "Reunion"
            },
            "United States Virgin Islands": {
                "address_replace": {"United States Virgin Islands": "U.S. Virgin Islands"},
                "country": "U.S. Virgin Islands"
            },
            "Macau": {
                "address_append": ", Macao",
                "country": "Macao"
            }
        }
    
    def _connect_mongodb(self):
        """Connect to MongoDB using environment variables"""
        # mongodb_uri = os.getenv('MONGODB_URI')
        mongodb_uri = "mongodb://admin:fjkfjrj!20020415@localhost:27017/?authSource=admin"
        if not mongodb_uri:
            raise ValueError("MONGODB_URI not found in environment variables")
        
        client = MongoClient(mongodb_uri, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')  # Test connection
        logger.info("✅ Connected to MongoDB")
        return client
    
    def find_addresses_for_territory(self, territory: str) -> List[Dict]:
        """Find addresses containing the territory name"""
        logger.info(f"🔍 Finding addresses for {territory}...")
        
        # Use case-insensitive regex to find addresses containing the territory
        query = {"address": {"$regex": territory, "$options": "i"}}
        addresses = list(self.collection.find(query))
        
        logger.info(f"Found {len(addresses)} addresses for {territory}")
        return addresses
    
    def trim_address_to_territory(self, address: str, territory: str) -> str:
        """Trim address from start to the territory name (inclusive)"""
        # Find the territory in the address (case-insensitive)
        pattern = re.compile(re.escape(territory), re.IGNORECASE)
        match = pattern.search(address)
        
        if match:
            # Trim from start to end of territory name
            end_pos = match.end()
            trimmed = address[:end_pos].strip()
            return trimmed
        
        return address  # Return original if territory not found
    
    def apply_special_rules(self, address: str, territory: str) -> Tuple[str, str]:
        """Apply special processing rules for specific territories"""
        if territory not in self.special_rules:
            return address, territory
        
        rules = self.special_rules[territory]
        processed_address = address
        
        # Apply address replacements
        if "address_replace" in rules:
            for old_text, new_text in rules["address_replace"].items():
                processed_address = processed_address.replace(old_text, new_text)
        
        # Apply address append
        if "address_append" in rules:
            processed_address += rules["address_append"]
        
        # Get new country name
        new_country = rules.get("country", territory)
        
        return processed_address, new_country
    
    def validate_address(self, address: str, country: str) -> bool:
        """Validate address using existing validation functions"""
        try:
            # Check if it looks like an address
            if not looks_like_address(address):
                return False
            
            # Validate address region
            if not validate_address_region(address, country):
                return False
            
            return True
        except Exception as e:
            logger.warning(f"Validation error for address '{address[:50]}...': {e}")
            return False
    
    def process_territory(self, territory: str) -> Dict:
        """Process all addresses for a specific territory"""
        logger.info(f"🔄 Processing territory: {territory}")
        
        # Find addresses for this territory
        addresses = self.find_addresses_for_territory(territory)
        
        if not addresses:
            logger.info(f"No addresses found for {territory}")
            return {"processed": 0, "updated": 0, "deleted": 0}
        
        updates_batch = []
        deletes_batch = []
        
        for addr_doc in addresses:
            original_address = addr_doc["address"]
            original_country = addr_doc.get("country", "")
            
            # Trim address to territory
            trimmed_address = self.trim_address_to_territory(original_address, territory)
            
            # Apply special rules
            processed_address, new_country = self.apply_special_rules(trimmed_address, territory)
            
            # Validate the processed address
            if self.validate_address(processed_address, new_country):
                # Prepare update operation
                updates_batch.append({
                    "_id": addr_doc["_id"],
                    "address": processed_address,
                    "country": new_country
                })
            else:
                # Prepare delete operation
                deletes_batch.append(addr_doc["_id"])
        
        # Execute batch operations
        stats = self._execute_batch_operations(updates_batch, deletes_batch)
        
        logger.info(f"✅ {territory}: {stats['updated']} updated, {stats['deleted']} deleted")
        return stats
    
    def _execute_batch_operations(self, updates_batch: List[Dict], deletes_batch: List[str]) -> Dict:
        """Execute batch update and delete operations"""
        stats = {"processed": len(updates_batch) + len(deletes_batch), "updated": 0, "deleted": 0}
        
        # Execute updates in batches
        if updates_batch:
            logger.info(f"Updating {len(updates_batch)} addresses...")
            
            # Group updates for bulk operation
            update_operations = []
            for update in updates_batch:
                update_operations.append(
                    UpdateMany(
                        {"_id": update["_id"]},
                        {"$set": {
                            "address": update["address"],
                            "country": update["country"]
                        }}
                    )
                )
            
            # Execute bulk updates in chunks
            chunk_size = 1000
            for i in range(0, len(update_operations), chunk_size):
                chunk = update_operations[i:i + chunk_size]
                try:
                    result = self.collection.bulk_write(chunk, ordered=False)
                    stats["updated"] += result.modified_count
                except Exception as e:
                    logger.error(f"Error updating batch: {e}")
        
        # Execute deletes in batches
        if deletes_batch:
            logger.info(f"Deleting {len(deletes_batch)} addresses...")
            
            # Delete in chunks
            chunk_size = 1000
            for i in range(0, len(deletes_batch), chunk_size):
                chunk = deletes_batch[i:i + chunk_size]
                try:
                    result = self.collection.delete_many({"_id": {"$in": chunk}})
                    stats["deleted"] += result.deleted_count
                except Exception as e:
                    logger.error(f"Error deleting batch: {e}")
        
        return stats
    
    def process_all_territories(self):
        """Process all territories in the list"""
        logger.info("🚀 Starting address update process for all territories")
        
        total_stats = {"processed": 0, "updated": 0, "deleted": 0}
        start_time = time.time()
        
        for i, territory in enumerate(self.territories, 1):
            logger.info(f"📊 Progress: {i}/{len(self.territories)} - Processing {territory}")
            
            try:
                stats = self.process_territory(territory)
                
                # Accumulate stats
                for key in total_stats:
                    total_stats[key] += stats[key]
                    
            except Exception as e:
                logger.error(f"Error processing {territory}: {e}")
                continue
        
        elapsed = time.time() - start_time
        
        # Final summary
        logger.info("=" * 60)
        logger.info("✅ Address update process completed!")
        logger.info(f"📊 Total Statistics:")
        logger.info(f"   Processed: {total_stats['processed']:,} addresses")
        logger.info(f"   Updated: {total_stats['updated']:,} addresses")
        logger.info(f"   Deleted: {total_stats['deleted']:,} addresses")
        logger.info(f"⏱️  Total time: {elapsed:.1f} seconds")
        logger.info("=" * 60)
        
        return total_stats
    
    def close(self):
        """Close database connection"""
        if self.client:
            self.client.close()
            logger.info("MongoDB connection closed")

def main():
    """Main function to run the address updater"""
    updater = None
    
    try:
        updater = AddressUpdater()
        stats = updater.process_all_territories()
        
        print(f"\n🎉 Process completed successfully!")
        print(f"📈 Final Results:")
        print(f"   ✅ Updated: {stats['updated']:,} addresses")
        print(f"   ❌ Deleted: {stats['deleted']:,} addresses")
        print(f"   📊 Total processed: {stats['processed']:,} addresses")
        
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        return 1
    
    finally:
        if updater:
            updater.close()
    
    return 0

if __name__ == "__main__":
    import sys
    sys.exit(main())