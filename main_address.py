#!/usr/bin/env python3
"""
Main Address Processor
Loops through countries in country.json and processes addresses using address_validator.py
If no batches found for a country, saves country name to JSON file

Requirements: pip install pymongo requests
Usage: python main_address.py
"""

import json
import sys
import time
import logging
import os

# Import the address validator
from address_validator import AddressValidator

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

class MainAddressProcessor:
    """Main processor that handles address validation for all countries"""
    
    def __init__(self):
        self.validator = AddressValidator()
        self.countries_without_batches = []
        
    def load_countries(self):
        """Load countries from country.json"""
        try:
            with open('final/country_names_only.json', 'r', encoding='utf-8') as f:
                countries = json.load(f)
            logger.info(f"Loaded {len(countries)} countries from country.json")
            return countries
        except Exception as e:
            logger.error(f"Error loading country.json: {e}")
            return []
    
    def save_countries_without_batches(self):
        """Save countries without batches to JSON file"""
        if self.countries_without_batches:
            output_file = 'countries_without_batches.json'
            try:
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(self.countries_without_batches, f, indent=2, ensure_ascii=False)
                print(f"💾 Saved {len(self.countries_without_batches)} countries without batches to {output_file}")
            except Exception as e:
                logger.error(f"Error saving countries without batches: {e}")
    
    def process_all_countries(self):
        """Process addresses for all countries with limit 1"""
        print("🚀 Starting address validation for all countries...")
        
        # Load countries
        countries = self.load_countries()
        if not countries:
            print("❌ No countries loaded")
            return
        
        print(f"📊 Processing {len(countries)} countries with limit 1 each...")
        
        stats = {
            'processed': 0,
            'successful': 0,
            'failed': 0,
            'no_batches': 0
        }
        
        for i, country_name in enumerate(countries, 1):
            print(f"\n🔄 Processing {i}/{len(countries)}: {country_name}")
            
            try:
                # First check if country has batches
                batches = self.validator.get_address_batches(country_name, limit=1)
                
                if not batches:
                    # No batches found for this country
                    print(f"📭 No batches found for: {country_name}")
                    self.countries_without_batches.append(country_name)
                    stats['no_batches'] += 1
                else:
                    # Process country with limit 2
                    self.validator.process_country(country_name, limit=3)
                    stats['successful'] += 1
                    print(f"✅ Completed: {country_name}")
                
            except Exception as e:
                logger.error(f"Failed to process {country_name}: {e}")
                stats['failed'] += 1
                print(f"❌ Failed: {country_name}")
            
            stats['processed'] += 1
            
            # Small delay between countries
            time.sleep(1)
        
        # Save countries without batches to JSON file
        self.save_countries_without_batches()
        
        print(f"\n\n✅ All countries processed!")
        print(f"📊 Total processed: {stats['processed']}")
        print(f"✅ Successful: {stats['successful']}")
        print(f"❌ Failed: {stats['failed']}")
        print(f"📭 No batches: {stats['no_batches']}")
        
        if self.countries_without_batches:
            print(f"\n📋 Countries without batches ({len(self.countries_without_batches)}):")
            for country in self.countries_without_batches[:10]:  # Show first 10
                print(f"  - {country}")
            if len(self.countries_without_batches) > 10:
                print(f"  ... and {len(self.countries_without_batches) - 10} more")
    
    def close(self):
        """Close validator connection"""
        self.validator.close()

if __name__ == "__main__":
    processor = MainAddressProcessor()
    
    try:
        processor.process_all_countries()
    except KeyboardInterrupt:
        print("\n⏹️  Processing interrupted by user")
    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)
    finally:
        processor.close()