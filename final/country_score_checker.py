#!/usr/bin/env python3
"""
Country Score Checker
Loops through country_names.json and finds countries with fewer than 15 addresses with score >= 0.9
"""

import os
import json
from pymongo import MongoClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class CountryScoreChecker:
    """Checks countries for address count with score >= 0.9"""
    
    def __init__(self):
        self.client = self._connect_mongodb()
        self.db = self.client[os.getenv('DB_NAME', 'osm_addresses')]
        self.collection = self.db.validated_addresses
        
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
        with open('basic/country_all.json', 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def get_high_score_count(self, country_name: str) -> int:
        """Get count of addresses with score >= 0.9 for a country"""
        query = {"country": country_name, "score": {"$gte": 1}}
        count = self.collection.count_documents(query)
        return count
    
    def find_countries_with_low_score_count(self):
        """Find countries with fewer than 15 addresses with score >= 0.9"""
        countries = self.load_country_names()
        low_count_countries = []
        
        for country in countries:
            count = self.get_high_score_count(country)
            if count < 15:
                country_data = {
                    "country": country,
                    "count": count
                }
                low_count_countries.append(country_data)
                print(f"{country}: {count} addresses with score >= 0.9")
        
        return low_count_countries
    
    def save_results(self, countries_data):
        """Save results to JSON file with country names and counts"""
        output_file = 'final/low_score_countries.json'
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(countries_data, f, indent=2, ensure_ascii=False)
        print(f"\nSaved {len(countries_data)} countries with counts to {output_file}")
    
    def close(self):
        """Close database connection"""
        if self.client:
            self.client.close()

def main():
    """Main function"""
    checker = None
    
    try:
        checker = CountryScoreChecker()
        low_count_countries = checker.find_countries_with_low_score_count()
        checker.save_results(low_count_countries)
        
        print(f"\nFound {len(low_count_countries)} countries with fewer than 15 addresses with score >= 0.9")
        
    except Exception as e:
        print(f"Error: {e}")
        return 1
    
    finally:
        if checker:
            checker.close()
    
    return 0

if __name__ == "__main__":
    import sys
    sys.exit(main())