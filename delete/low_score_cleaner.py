#!/usr/bin/env python3
"""
Low Score Address Cleaner
Gets addresses with status=0, checks their Nominatim scores, and deletes those with score < 0.9
"""

import os
import time
from typing import List, Dict
from pymongo import MongoClient
from dotenv import load_dotenv

# Import scoring function
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from basic.address_score import check_with_nominatim

# Load environment variables
load_dotenv()

class LowScoreCleaner:
    """Cleans addresses with low Nominatim scores from the database"""
    
    def __init__(self):
        self.client = self._connect_mongodb()
        self.db = self.client[os.getenv('DB_NAME', 'osm_addresses')]
        self.collection = self.db.validated_addresses
        
        self.batch_size = 100
        self.score_threshold = 0.9
        
    def _connect_mongodb(self):
        """Connect to MongoDB using environment variables"""
        mongodb_uri = os.getenv('MONGODB_URI')
        if not mongodb_uri:
            raise ValueError("MONGODB_URI not found in environment variables")
        
        client = MongoClient(mongodb_uri, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        return client
    
    def get_addresses_with_status_0(self, limit: int = 1000) -> List[Dict]:
        """Get addresses with status=0 from database"""
        query = {"status": 0}
        addresses = list(self.collection.find(query).limit(limit))
        return addresses
    
    def check_address_scores(self, addresses: List[Dict]) -> Dict:
        """Check Nominatim scores for addresses and categorize them"""
        addresses_to_delete = []
        addresses_to_update = []
        
        for i, addr_doc in enumerate(addresses, 1):
            address = addr_doc.get("address", "")
            addr_id = addr_doc["_id"]
            
            try:
                score = check_with_nominatim(address)
                
                # check_with_nominatim returns only score now
                if isinstance(score, (int, float)) and score >= self.score_threshold:
                    addresses_to_update.append({"_id": addr_id, "score": score})
                else:
                    # Delete if score < threshold, or if it's a string (TIMEOUT, API_ERROR) or 0.0
                    addresses_to_delete.append({"_id": addr_id})
                        
            except Exception:
                # On error, delete the address
                addresses_to_delete.append({"_id": addr_id})
            
            time.sleep(0.5)  # API rate limiting
        
        return {
            "to_delete": addresses_to_delete,
            "to_update": addresses_to_update
        }
    
    def update_high_score_addresses(self, addresses_to_update: List[Dict]) -> int:
        """Update addresses with high scores to set score and status=1"""
        if not addresses_to_update:
            return 0
        
        updated_count = 0
        try:
            # Update each address individually for simplicity
            for addr in addresses_to_update:
                result = self.collection.update_one(
                    {"_id": addr["_id"]},
                    {"$set": {"score": addr["score"], "status": 1}}
                )
                updated_count += result.modified_count
            return updated_count
        except Exception:
            return updated_count
    def delete_low_score_addresses(self, addresses_to_delete: List[Dict]) -> int:
        """Delete addresses with low scores using batch operations"""
        if not addresses_to_delete:
            return 0
        
        ids_to_delete = [addr["_id"] for addr in addresses_to_delete]
        
        try:
            result = self.collection.delete_many({"_id": {"$in": ids_to_delete}})
            return result.deleted_count
        except Exception:
            return 0
    
    def clean_all_low_score_addresses(self):
        """Clean all addresses with low scores until no more status=0 addresses exist"""
        total_deleted = 0
        total_updated = 0
        total_processed = 0
        
        while True:
            addresses = self.get_addresses_with_status_0(self.batch_size)
            
            if not addresses:
                break
            
            # Check scores with progress tracking
            categorized = {"to_delete": [], "to_update": []}
            
            for i, addr_doc in enumerate(addresses, 1):
                address = addr_doc.get("address", "")
                addr_id = addr_doc["_id"]
                
                try:
                    score = check_with_nominatim(address)
                    
                    if isinstance(score, (int, float)) and score >= self.score_threshold:
                        categorized["to_update"].append({"_id": addr_id, "score": score})
                        total_updated += 1
                    else:
                        categorized["to_delete"].append({"_id": addr_id})
                        total_deleted += 1
                        
                except Exception:
                    categorized["to_delete"].append({"_id": addr_id})
                    total_deleted += 1
                
                # Show progress immediately after each score check
                progress = ((total_processed + i) / (total_processed + len(addresses))) * 100
                print(f"\rProgress: {progress:.1f}% | Deleted: {total_deleted} | Updated: {total_updated}", end='', flush=True)
                
                time.sleep(0.5)  # API rate limiting
            
            # Delete low score addresses
            self.delete_low_score_addresses(categorized["to_delete"])
            
            # Update high score addresses
            self.update_high_score_addresses(categorized["to_update"])
            
            total_processed += len(addresses)
            
            if len(addresses) < self.batch_size:
                break
        
        print()  # Final newline
        return {"deleted": total_deleted, "updated": total_updated}
    
    def close(self):
        """Close database connection"""
        if self.client:
            self.client.close()

def main():
    """Main function to run the low score cleaner"""
    cleaner = None
    
    try:
        cleaner = LowScoreCleaner()
        stats = cleaner.clean_all_low_score_addresses()
        
    except Exception as e:
        print(f"Error: {e}")
        return 1
    
    finally:
        if cleaner:
            cleaner.close()
    
    return 0

if __name__ == "__main__":
    import sys
    sys.exit(main())