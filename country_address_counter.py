#!/usr/bin/env python3
"""
Country Address Counter
Input: Country name
Output: Count of addresses with unique first sections that exist in the database
"""

import os
import sys
from pymongo import MongoClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class CountryAddressCounter:
    def __init__(self, mongodb_uri=None):
        """Initialize MongoDB connection"""
        if mongodb_uri is None:
            mongodb_uri = os.getenv('MONGODB_URI')
            if not mongodb_uri:
                raise ValueError("MongoDB URI not found in environment variables")
        
        self.client = MongoClient(mongodb_uri)
        self.db = self.client['osm_addresses']
        self.addresses_collection = self.db['validated_addresses']
    
    def extract_first_section(self, address):
        """Extract the first section of an address (before first comma)"""
        if not address:
            return None
        
        # Split by comma and take the first part, strip whitespace
        first_section = address.split(',')[0].strip()
        return first_section if first_section else None
    
    def count_unique_first_sections(self, country_name):
        """
        Count addresses with unique first sections for a given country using aggregation
        
        Args:
            country_name (str): Name of the country
            
        Returns:
            dict: Contains total count, unique first sections count, and details
        """
        try:
            # Aggregation pipeline for efficient counting
            pipeline = [
                # Match documents for the specified country
                {"$match": {"country": country_name}},
                
                # Add a field for first_section by splitting address on comma
                {"$addFields": {
                    "first_section": {
                        "$trim": {
                            "input": {
                                "$arrayElemAt": [
                                    {"$split": ["$address", ","]}, 
                                    0
                                ]
                            }
                        }
                    }
                }},
                
                # Group to count total and collect unique first sections
                {"$group": {
                    "_id": None,
                    "total_addresses": {"$sum": 1},
                    "addresses_with_first_section": {
                        "$sum": {
                            "$cond": [
                                {"$and": [
                                    {"$ne": ["$first_section", ""]},
                                    {"$ne": ["$first_section", None]}
                                ]},
                                1,
                                0
                            ]
                        }
                    },
                    "unique_first_sections": {
                        "$addToSet": {
                            "$cond": [
                                {"$and": [
                                    {"$ne": ["$first_section", ""]},
                                    {"$ne": ["$first_section", None]}
                                ]},
                                "$first_section",
                                "$$REMOVE"
                            ]
                        }
                    }
                }},
                
                # Project final result
                {"$project": {
                    "_id": 0,
                    "total_addresses": 1,
                    "addresses_with_first_section": 1,
                    "unique_first_sections_count": {"$size": "$unique_first_sections"},
                    "unique_first_sections": 1
                }}
            ]
            
            # Execute aggregation
            result_cursor = self.addresses_collection.aggregate(pipeline)
            result_list = list(result_cursor)
            
            if result_list:
                result_data = result_list[0]
                result = {
                    'country': country_name,
                    'total_addresses': result_data.get('total_addresses', 0),
                    'addresses_with_first_section': result_data.get('addresses_with_first_section', 0),
                    'unique_first_sections_count': result_data.get('unique_first_sections_count', 0),
                    'unique_first_sections': sorted(result_data.get('unique_first_sections', []))
                }
            else:
                # No addresses found for this country
                result = {
                    'country': country_name,
                    'total_addresses': 0,
                    'addresses_with_first_section': 0,
                    'unique_first_sections_count': 0,
                    'unique_first_sections': []
                }
            
            return result
            
        except Exception as e:
            print(f"Error counting addresses for country '{country_name}': {e}")
            return None
    
    def print_results(self, result):
        """Print formatted results"""
        if not result:
            print("No results to display")
            return
        
        print(f"\n=== Address Count Results for '{result['country']}' ===")
        print(f"Total addresses in database: {result['total_addresses']}")
        print(f"Addresses with first section: {result['addresses_with_first_section']}")
        print(f"Unique first sections count: {result['unique_first_sections_count']}")
        
        if result['unique_first_sections']:
            print(f"\nFirst 10 unique first sections:")
            for i, section in enumerate(result['unique_first_sections'][:10], 1):
                print(f"  {i}. {section}")
            
            if len(result['unique_first_sections']) > 10:
                print(f"  ... and {len(result['unique_first_sections']) - 10} more")
    
    def close_connection(self):
        """Close MongoDB connection"""
        if self.client:
            self.client.close()

def main():
    """Main function to run the country address counter"""
    if len(sys.argv) != 2:
        print("Usage: python country_address_counter.py <country_name>")
        print("Example: python country_address_counter.py 'United States'")
        sys.exit(1)
    
    country_name = sys.argv[1]
    
    try:
        # Initialize counter
        counter = CountryAddressCounter()
        
        # Count unique first sections
        print(f"Counting addresses for country: '{country_name}'...")
        result = counter.count_unique_first_sections(country_name)
        
        # Print results
        counter.print_results(result)
        
        # Close connection
        counter.close_connection()
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()