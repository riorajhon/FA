#!/usr/bin/env python3
"""
Simple Country Address Checker
Quick function to check address counts by country
"""

import os
from pymongo import MongoClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def get_country_address_stats(country_name, mongodb_uri=None):
    """
    Get address statistics for a country using aggregation pipeline
    
    Args:
        country_name (str): Name of the country
        mongodb_uri (str): MongoDB connection URI (optional)
    
    Returns:
        dict: Address statistics
    """
    if mongodb_uri is None:
        mongodb_uri = os.getenv('MONGODB_URI')
        if not mongodb_uri:
            raise ValueError("MongoDB URI not found in environment variables")
    
    client = MongoClient(mongodb_uri)
    db = client['osm_addresses']
    addresses_collection = db['validated_addresses']
    
    try:
        # Aggregation pipeline to get counts efficiently
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
            
            # Filter out empty first sections
            {"$match": {"first_section": {"$ne": ""}}},
            
            # Group to get counts
            {"$group": {
                "_id": None,
                "total_addresses": {"$sum": 1},
                "unique_first_sections": {"$addToSet": "$first_section"}
            }},
            
            # Project final result
            {"$project": {
                "_id": 0,
                "total_addresses": 1,
                "unique_first_sections_count": {"$size": "$unique_first_sections"},
                "unique_first_sections": 1
            }}
        ]
        
        # Execute aggregation
        result_cursor = addresses_collection.aggregate(pipeline)
        result_list = list(result_cursor)
        
        if result_list:
            result_data = result_list[0]
            result = {
                'country': country_name,
                'total_addresses': result_data.get('total_addresses', 0),
                'unique_first_sections': result_data.get('unique_first_sections_count', 0),
                'first_sections_list': sorted(result_data.get('unique_first_sections', []))
            }
        else:
            # No addresses found for this country
            result = {
                'country': country_name,
                'total_addresses': 0,
                'unique_first_sections': 0,
                'first_sections_list': []
            }
        
        return result
        
    finally:
        client.close()

def check_country(country_name):
    """
    Simple function to check and print country address stats
    
    Args:
        country_name (str): Name of the country
    """
    try:
        stats = get_country_address_stats(country_name)
        print(f"Country: {stats['country']}")
        print(f"Total addresses: {stats['total_addresses']}")
        print(f"Unique first sections: {stats['unique_first_sections']}")
        
        # Show sample of first sections if available
        if stats['first_sections_list']:
            print(f"Sample first sections (first 5):")
            for i, section in enumerate(stats['first_sections_list'][:5], 1):
                print(f"  {i}. {section}")
            
            if len(stats['first_sections_list']) > 5:
                print(f"  ... and {len(stats['first_sections_list']) - 5} more")
        
        return stats
    except Exception as e:
        print(f"Error checking country '{country_name}': {e}")
        return None

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) != 2:
        print("Usage: python country_checker.py <country_name>")
        print("Example: python country_checker.py 'Slovakia'")
        sys.exit(1)
    
    country_name = sys.argv[1]
    check_country(country_name)