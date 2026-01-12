#!/usr/bin/env python3
"""
Unique First Section Checker
Loops through countries in country_all.json and checks unique first_section count
Reports countries with less than 15 unique first_sections

Requirements: pip install pymongo python-dotenv
Usage: python unique_first_section_checker.py
"""

import os
import json
import sys
from pymongo import MongoClient
from dotenv import load_dotenv
import logging
from datetime import datetime

# Load environment variables
load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

class UniqueFirstSectionChecker:
    """Checks unique first_section counts per country"""
    
    def __init__(self):
        self.client = self._connect_mongodb()
        self.db = self.client[os.getenv('DB_NAME', 'osm_addresses')]
        self.collection = self.db.validated_addresses
        
        # Load countries list
        self.countries = self._load_countries()
        
        # Results storage
        self.results = []
        self.low_count_countries = []
        
    def _connect_mongodb(self):
        """Connect to MongoDB using environment variables"""
        mongodb_uri = os.getenv('MONGODB_URI')
        if not mongodb_uri:
            raise ValueError("MONGODB_URI not found in environment variables")
        
        client = MongoClient(mongodb_uri, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        return client
    
    def _load_countries(self):
        """Load countries from country_all.json"""
        try:
            with open('basic/country_all.json', 'r', encoding='utf-8') as f:
                countries = json.load(f)
            print(f"📋 Loaded {len(countries)} countries from country_all.json")
            return countries
        except FileNotFoundError:
            logger.error("country_all.json not found in basic/ directory")
            sys.exit(1)
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing country_all.json: {e}")
            sys.exit(1)
    
    def get_unique_first_sections_for_country(self, country):
        """Get unique first_section count for a specific country"""
        try:
            # MongoDB aggregation pipeline to get unique first_sections
            pipeline = [
                {
                    "$match": {
                        "country": country,
                        "first_section": {"$exists": True, "$ne": "", "$ne": None}
                    }
                },
                {
                    "$group": {
                        "_id": "$first_section"
                    }
                },
                {
                    "$count": "unique_count"
                }
            ]
            
            result = list(self.collection.aggregate(pipeline))
            unique_count = result[0]['unique_count'] if result else 0
            
            return unique_count
            
        except Exception as e:
            logger.error(f"Error processing country {country}: {e}")
            return 0
    
    def get_total_addresses_for_country(self, country):
        """Get total address count for a country"""
        try:
            return self.collection.count_documents({"country": country})
        except Exception as e:
            logger.error(f"Error counting addresses for {country}: {e}")
            return 0
    
    def check_all_countries(self):
        """Check unique first_section counts for all countries"""
        print("🔍 Starting Unique First Section Analysis")
        print("=" * 70)
        
        total_countries = len(self.countries)
        processed = 0
        
        for country in self.countries:
            processed += 1
            
            # Get counts
            unique_first_sections = self.get_unique_first_sections_for_country(country)
            total_addresses = self.get_total_addresses_for_country(country)
            
            # Store result
            result = {
                'country': country,
                'unique_first_sections': unique_first_sections,
                'total_addresses': total_addresses,
                'is_low_count': unique_first_sections < 15
            }
            self.results.append(result)
            
            # Track low count countries
            if unique_first_sections < 15:
                self.low_count_countries.append(result)
            
            # Progress indicator
            if processed % 10 == 0 or processed == total_countries:
                print(f"📊 Progress: {processed}/{total_countries} countries processed")
            
            # Show current result
            status = "⚠️ LOW" if unique_first_sections < 15 else "✅ OK"
            print(f"  {status} {country}: {unique_first_sections} unique first_sections ({total_addresses:,} total addresses)")
        
        print("\n" + "=" * 70)
    
    def generate_report(self):
        """Generate and display the final report"""
        print("📋 UNIQUE FIRST SECTION ANALYSIS REPORT")
        print("=" * 70)
        print(f"📅 Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"🌍 Total countries analyzed: {len(self.countries)}")
        print(f"⚠️  Countries with < 15 unique first_sections: {len(self.low_count_countries)}")
        print(f"✅ Countries with >= 15 unique first_sections: {len(self.countries) - len(self.low_count_countries)}")
        
        if self.low_count_countries:
            print("\n🚨 COUNTRIES WITH LOW UNIQUE FIRST_SECTION COUNT (<15):")
            print("-" * 70)
            print(f"{'Country':<25} {'Unique':<8} {'Total':<10} {'Ratio':<8}")
            print("-" * 70)
            
            # Sort by unique count (lowest first)
            sorted_low = sorted(self.low_count_countries, key=lambda x: x['unique_first_sections'])
            
            for result in sorted_low:
                ratio = result['unique_first_sections'] / result['total_addresses'] if result['total_addresses'] > 0 else 0
                print(f"{result['country']:<25} {result['unique_first_sections']:<8} {result['total_addresses']:<10,} {ratio:<8.3f}")
        
        # Summary statistics
        if self.results:
            unique_counts = [r['unique_first_sections'] for r in self.results if r['unique_first_sections'] > 0]
            if unique_counts:
                avg_unique = sum(unique_counts) / len(unique_counts)
                min_unique = min(unique_counts)
                max_unique = max(unique_counts)
                
                print(f"\n📈 STATISTICS:")
                print(f"    Average unique first_sections: {avg_unique:.1f}")
                print(f"    Minimum unique first_sections: {min_unique}")
                print(f"    Maximum unique first_sections: {max_unique}")
        
        print("\n" + "=" * 70)
    
    def save_report_to_file(self, filename="unique_first_section_report.json"):
        """Save detailed report to JSON file"""
        report_data = {
            'generated_at': datetime.now().isoformat(),
            'total_countries': len(self.countries),
            'low_count_threshold': 15,
            'countries_with_low_count': len(self.low_count_countries),
            'countries_with_adequate_count': len(self.countries) - len(self.low_count_countries),
            'low_count_countries': self.low_count_countries,
            'all_results': self.results
        }
        
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(report_data, f, indent=2, ensure_ascii=False)
            print(f"💾 Detailed report saved to: {filename}")
        except Exception as e:
            logger.error(f"Error saving report: {e}")
    
    def close(self):
        """Close database connection"""
        if self.client:
            self.client.close()

def main():
    """Main function"""
    checker = None
    
    try:
        checker = UniqueFirstSectionChecker()
        checker.check_all_countries()
        checker.generate_report()
        checker.save_report_to_file()
        
    except KeyboardInterrupt:
        print("\n⏹️  Analysis interrupted by user")
    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)
    
    finally:
        if checker:
            checker.close()

if __name__ == "__main__":
    main()