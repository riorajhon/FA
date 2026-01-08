#!/usr/bin/env python3
"""
Get Country Names from MongoDB
Extracts unique country names from osm_addresses.error collection,
sorts them alphabetically, and saves to JSON file
"""

import json
import os
from pymongo import MongoClient
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

def get_country_names():
    """
    Extract unique country names from MongoDB and save to JSON
    Uses environment variables for MongoDB connection
    """
    
    mongodb_uri = os.getenv('MONGODB_URI')
    if not mongodb_uri:
        logger.error("MONGODB_URI not found in environment variables")
        return None
    
    try:
        # Connect to MongoDB
        logger.info("Connecting to MongoDB...")
        client = MongoClient(mongodb_uri, serverSelectionTimeoutMS=5000)
        
        # Test connection
        client.admin.command('ping')
        logger.info("✅ Connected to MongoDB")
        
        # Access the database and collection using environment variables
        db_name = os.getenv('DB_NAME', 'osm_addresses')
        collection_name = os.getenv('COLLECTION_ERROR', 'error')
        
        db = client[db_name]
        collection = db[collection_name]
        
        # Get total document count
        total_docs = collection.count_documents({})
        logger.info(f"Total documents in error collection: {total_docs:,}")
        
        if total_docs == 0:
            logger.warning("No documents found in osm_addresses.error collection")
            return
        
        # Get unique country names using aggregation
        logger.info("Extracting unique country names from seed field...")
        
        pipeline = [
            {
                "$group": {
                    "_id": "$seed",
                    "count": {"$sum": 1}
                }
            },
            {
                "$match": {
                    "_id": {"$ne": None}  # Exclude null seed values
                }
            },
            {
                "$sort": {"_id": 1}  # Sort alphabetically
            }
        ]
        
        results = list(collection.aggregate(pipeline))
        
        if not results:
            logger.warning("No seed values found in the collection")
            return
        
        # Extract country names from seed field and create sorted list
        country_names = []
        
        for result in results:
            seed_value = result["_id"]
            
            if seed_value:  # Skip empty/null seed values
                country_names.append(seed_value)
        
        # Sort alphabetically (already sorted by aggregation, but double-check)
        country_names.sort()
        
        logger.info(f"Found {len(country_names)} unique countries")
        
        # Save only country names as simple JSON array
        with open('country_names.json', 'w', encoding='utf-8') as f:
            json.dump(country_names, f, indent=2, ensure_ascii=False)
        
        logger.info("✅ File saved: country_names.json")
        
        # Print summary
        print(f"\n📊 Summary:")
        print(f"Total unique countries: {len(country_names)}")
        print(f"Total documents processed: {total_docs:,}")
        
        print(f"\n📝 Countries saved (first 10):")
        for i, country in enumerate(country_names[:10], 1):
            print(f"{i:2d}. {country}")
        
        if len(country_names) > 10:
            print(f"... and {len(country_names) - 10} more countries")
        
        return country_names
        
    except Exception as e:
        logger.error(f"Error: {e}")
        return None
        
    finally:
        if 'client' in locals():
            client.close()
            logger.info("MongoDB connection closed")

if __name__ == "__main__":
    import sys
    from datetime import datetime
    
    print("🌍 Country Names Extractor")
    print("=" * 50)
    
    result = get_country_names()
    
    if result:
        print(f"\n✅ Successfully extracted {len(result)} countries and saved to country_names.json")
    else:
        print("\n❌ Failed to extract country names")
        sys.exit(1)