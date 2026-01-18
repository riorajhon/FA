#!/usr/bin/env python3
"""
Batch Address Counter
Loops through country names and gets address counts from address_batches collection
Search criteria: country_name and status: "origin"
"""

import os
import json
from pymongo import MongoClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class BatchAddressCounter:
    """Counts addresses from address_batches collection by country"""
    
    def __init__(self):
        self.client = self._connect_mongodb()
        self.db = self.client[os.getenv('DB_NAME', 'osm_addresses')]
        self.address_batches_collection = self.db.address_batches
        
    def _connect_mongodb(self):
        """Connect to MongoDB using environment variables"""
        mongodb_uri = os.getenv('MONGODB_URI')
        if not mongodb_uri:
            raise ValueError("MONGODB_URI not found in environment variables")
        
        client = MongoClient(mongodb_uri, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        return client
    
    def load_country_names(self):
        """Load country names from JSON file"""
        with open('address_generator_final/batch.json', 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def get_address_batch_count(self, country_name: str) -> int:
        """Get count of addresses from address_batches with country_name and status='origin'"""
        query = {"country_name": country_name, "status": "origin"}
        count = self.address_batches_collection.count_documents(query)
        return count
    
    def count_addresses_by_country(self):
        """Count addresses for each country and save results"""
        countries = self.load_country_names()
        country_counts = {}
        
        print("Counting addresses by country from address_batches collection...")
        print("Search criteria: status='origin'")
        print("-" * 50)
        
        for country in countries:
            count = self.get_address_batch_count(country)
            country_counts[country] = count
            print(f"{country}: {count} addresses")
        
        return country_counts
    
    def save_results(self, country_counts):
        """Save results to JSON file"""
        output_file = 'address_generator_final/batch_address_counts.json'
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(country_counts, f, indent=2, ensure_ascii=False)
        
        total_addresses = sum(country_counts.values())
        countries_with_addresses = len([c for c in country_counts.values() if c > 0])
        
        print(f"\nSummary:")
        print(f"Total countries: {len(country_counts)}")
        print(f"Countries with addresses: {countries_with_addresses}")
        print(f"Total addresses: {total_addresses}")
        print(f"Results saved to: {output_file}")
    
    def close(self):
        """Close database connection"""
        if self.client:
            self.client.close()

def main():
    """Main function"""
    counter = None
    
    try:
        counter = BatchAddressCounter()
        country_counts = counter.count_addresses_by_country()
        counter.save_results(country_counts)
        
    except Exception as e:
        print(f"Error: {e}")
        return 1
    
    finally:
        if counter:
            counter.close()
    
    return 0

if __name__ == "__main__":
    import sys
    sys.exit(main())