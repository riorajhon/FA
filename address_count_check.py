#!/usr/bin/env python3
"""
Address Count Checker
Loops through country_all.json and checks validated_addresses collection
Saves countries with < 15 unique streets to a JSON file

Requirements: pip install pymongo
Usage: python address_count_check.py
"""

import json
import sys
from typing import List, Dict
from pymongo import MongoClient
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

class AddressCountChecker:
    """Checks address counts and identifies countries needing more addresses"""
    
    def __init__(self, mongodb_uri="mongodb://admin:fjkfjrj!20020415@localhost:27017/?authSource=admin"):
        self.client = MongoClient(mongodb_uri)
        self.db = self.client.osm_addresses
        self.addresses_collection = self.db.validated_addresses
        self.output_file = "countries_need_more_addresses.json"
        
    def load_all_countries(self) -> List[str]:
        """Load all countries from country_all.json"""
        try:
            with open('basic/country_all.json', 'r', encoding='utf-8') as f:
                countries = json.load(f)
            logger.info(f"Loaded {len(countries)} countries from country_all.json")
            return countries
        except Exception as e:
            logger.error(f"Error loading country_all.json: {e}")
            return []
    
    def get_unique_street_count(self, country_name: str) -> int:
        """Get count of unique streets for a country"""
        try:
            pipeline = [
                # Match country
                {'$match': {'country': country_name}},
                # Filter out null/empty streets
                {'$match': {'street': {'$ne': None, '$ne': ''}}},
                # Group by unique street names
                {'$group': {'_id': '$street'}},
                # Count unique streets
                {'$count': 'unique_streets'}
            ]
            
            result = list(self.addresses_collection.aggregate(pipeline))
            return result[0]['unique_streets'] if result else 0
            
        except Exception as e:
            logger.warning(f"Error getting street count for {country_name}: {e}")
            return 0
    
    def load_existing_output(self) -> List[str]:
        """Load existing countries from output file"""
        try:
            with open(self.output_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            return []
        except Exception as e:
            logger.warning(f"Error loading existing output file: {e}")
            return []
    
    def save_countries_to_file(self, countries: List[str]):
        """Save countries list to JSON file"""
        try:
            with open(self.output_file, 'w', encoding='utf-8') as f:
                json.dump(countries, f, indent=2, ensure_ascii=False)
            logger.info(f"Saved {len(countries)} countries to {self.output_file}")
        except Exception as e:
            logger.error(f"Error saving to file: {e}")
    
    def check_all_countries(self):
        """Check all countries and identify those needing more addresses"""
        print("🔍 Checking address counts for all countries...")
        
        # Load countries and existing output
        all_countries = self.load_all_countries()
        if not all_countries:
            print("❌ No countries loaded")
            return
        
        countries_need_more = self.load_existing_output()
        print(f"📊 Loaded {len(countries_need_more)} countries from existing output file")
        
        print(f"🔄 Checking {len(all_countries)} countries...")
        
        stats = {
            'checked': 0,
            'need_more': 0,
            'sufficient': 0,
            'newly_added': 0
        }
        
        for i, country_name in enumerate(all_countries, 1):
            # Progress update
            progress_pct = (i / len(all_countries)) * 100
            print(f"\rProgress: {progress_pct:.1f}% | Checked: {i}/{len(all_countries)} | Need more: {stats['need_more']}", end='', flush=True)
            
            # Get unique street count
            street_count = self.get_unique_street_count(country_name)
            stats['checked'] += 1
            
            if street_count < 15:
                stats['need_more'] += 1
                
                # Add to list if not already present
                if country_name not in countries_need_more:
                    countries_need_more.append(country_name)
                    stats['newly_added'] += 1
            else:
                stats['sufficient'] += 1
        
        # Final progress update
        print(f"\rProgress: 100.0% | Checked: {len(all_countries)}/{len(all_countries)} | Need more: {stats['need_more']}", flush=True)
        
        # Save updated list
        self.save_countries_to_file(countries_need_more)
        
        print(f"\n\n✅ Analysis Complete!")
        print(f"📊 Countries checked: {stats['checked']}")
        print(f"🔴 Need more addresses (< 15 streets): {stats['need_more']}")
        print(f"🟢 Sufficient addresses (≥ 15 streets): {stats['sufficient']}")
        print(f"🆕 Newly added to list: {stats['newly_added']}")
        print(f"📁 Output saved to: {self.output_file}")
        
        # Show some examples
        if countries_need_more:
            print(f"\n📋 Sample countries needing more addresses:")
            for country in countries_need_more[:10]:
                street_count = self.get_unique_street_count(country)
                print(f"   - {country}: {street_count} unique streets")
            
            if len(countries_need_more) > 10:
                print(f"   ... and {len(countries_need_more) - 10} more")
    
    def close(self):
        """Close database connection"""
        self.client.close()

if __name__ == "__main__":
    checker = AddressCountChecker()
    
    try:
        checker.check_all_countries()
    except KeyboardInterrupt:
        print("\n⏹️  Analysis interrupted by user")
    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)
    finally:
        checker.close()