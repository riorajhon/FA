#!/usr/bin/env python3
"""
Country Status Processor
Processes countries from country.json, finds their codes in geonames_countries.json,
and saves them to database with status "origin"

Requirements: pip install pymongo
Usage: python country_status.py
"""

import json
import sys
from typing import Dict, List, Optional
from pymongo import MongoClient
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

class CountryStatusProcessor:
    """Processes countries and saves them with status to database"""
    
    def __init__(self, mongodb_uri="mongodb://admin:fjkfjrj!20020415@localhost:27017/?authSource=admin"):
        self.client = MongoClient(mongodb_uri)
        self.db = self.client.osm_addresses
        self.countries_collection = self.db.country_status
        
    def load_country_names(self) -> List[str]:
        """Load country names from country.json"""
        try:
            with open('basic/country.json', 'r', encoding='utf-8') as f:
                countries = json.load(f)
            logger.info(f"Loaded {len(countries)} countries from country.json")
            return countries
        except Exception as e:
            logger.error(f"Error loading country.json: {e}")
            return []
    
    def load_geonames_countries(self) -> Dict:
        """Load geonames countries data"""
        try:
            with open('basic/geonames_countries.json', 'r', encoding='utf-8') as f:
                geonames_data = json.load(f)
            logger.info(f"Loaded {len(geonames_data)} countries from geonames_countries.json")
            return geonames_data
        except Exception as e:
            logger.error(f"Error loading geonames_countries.json: {e}")
            return {}
    
    def find_country_code(self, country_name: str, geonames_data: Dict) -> Optional[str]:
        """Find country code for a country name in geonames data"""
        country_name_lower = country_name.lower().strip()
        
        # Direct name matching
        for code, data in geonames_data.items():
            geonames_name = data.get('name', '').lower().strip()
            if geonames_name == country_name_lower:
                return code
        
        # Handle special cases and variations
        name_mappings = {
            'united states': 'US',
            'united kingdom': 'GB',
            'south korea': 'KR',
            'north korea': 'KP',
            'democratic republic of the congo': 'CD',
            'Congo, Democratic Republic of the': 'CD',
            'republic of the congo': 'CG',
            'ivory coast': 'CI',
            'the netherlands': 'NL',
            'czechia': 'CZ',
            'north macedonia': 'MK',
            'eswatini': 'SZ',
            'timor leste': 'TL',
            'palestinian territory': 'PS',
            'bonaire, saint eustatius and saba': 'BQ',
            'british virgin islands': 'VG',
            'u.s. virgin islands': 'VI',
            'turks and caicos islands': 'TC',
            'saint vincent and the grenadines': 'VC',
            'trinidad and tobago': 'TT',
            'western sahara': 'EH'
        }
        
        if country_name_lower in name_mappings:
            return name_mappings[country_name_lower]
        
        # Partial matching for complex names
        for code, data in geonames_data.items():
            geonames_name = data.get('name', '').lower()
            
            # Check if country name contains geonames name or vice versa
            if (country_name_lower in geonames_name or 
                geonames_name in country_name_lower):
                return code
        
        return None
    
    def save_country_status(self, country_name: str, country_code: Optional[str] = None):
        """Save country with status to database, even without country code"""
        document = {
            'country_name': country_name,
            'status': 'origin'
        }
        
        # Add country code only if found
        if country_code:
            document['country_code'] = country_code
        
        # Check if already exists (by country name only)
        existing = self.countries_collection.find_one({
            'country_name': country_name
        })
        
        if not existing:
            self.countries_collection.insert_one(document)
            return True
        return False
    
    def process_countries(self):
        """Process all countries and save to database"""
        print("🔄 Loading country data...")
        
        # Load data
        country_names = self.load_country_names()
        geonames_data = self.load_geonames_countries()
        
        if not country_names or not geonames_data:
            print("❌ Failed to load required data files")
            return
        
        print(f"📊 Processing {len(country_names)} countries...")
        
        stats = {
            'processed': 0,
            'saved': 0,
            'not_found': 0,
            'already_exists': 0
        }
        
        not_found_countries = []
        
        for i, country_name in enumerate(country_names, 1):
            stats['processed'] += 1
            
            # Update progress
            progress_pct = (i / len(country_names)) * 100
            print(f"\r📊 Progress: {progress_pct:.1f}% | Processed: {i}/{len(country_names)} | Saved: {stats['saved']}", end='', flush=True)
            
            # Find country code
            country_code = self.find_country_code(country_name, geonames_data)
            
            # Save to database regardless of whether country code was found
            if self.save_country_status(country_name, country_code):
                stats['saved'] += 1
            else:
                stats['already_exists'] += 1
            
            # Track countries without codes for reporting
            if not country_code:
                stats['not_found'] += 1
                not_found_countries.append(country_name)
        
        # Final progress update
        print(f"\r📊 Progress: 100.0% | Processed: {len(country_names)}/{len(country_names)} | Saved: {stats['saved']}", flush=True)
        
        print(f"\n\n✅ Complete!")
        print(f"📊 Countries processed: {stats['processed']}")
        print(f"💾 Countries saved: {stats['saved']}")
        print(f"🔄 Already existed: {stats['already_exists']}")
        print(f"❌ Saved without country code: {stats['not_found']}")
        
        if not_found_countries:
            print(f"\n⚠️  Countries saved without country codes:")
            for country in not_found_countries:
                print(f"   - {country}")
    
    def close(self):
        """Close database connection"""
        self.client.close()

if __name__ == "__main__":
    processor = CountryStatusProcessor()
    
    try:
        processor.process_countries()
    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)
    finally:
        processor.close()