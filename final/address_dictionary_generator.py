#!/usr/bin/env python3
"""
Address Dictionary Generator
Creates an address dictionary with exactly 15 addresses per country using unique first_section logic
"""

import json
import os
import time
from typing import List, Dict, Set
from pymongo import MongoClient
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

class AddressDictionaryGenerator:
    """Generates address dictionary with 15 addresses per country using unique first_section"""
    
    def __init__(self):
        self.client = self._connect_mongodb()
        self.db = self.client[os.getenv('DB_NAME', 'osm_addresses')]
        self.collection = self.db.validated_addresses
        
        self.target_count = 15
        self.address_dictionary = {}
        self.report = {
            "processed_countries": 0,
            "successful_countries": 0,
            "skipped_countries": 0,
            "countries": {}
        }
    
    def _connect_mongodb(self):
        """Connect to MongoDB using environment variables"""
        mongodb_uri = os.getenv('MONGODB_URI')
        if not mongodb_uri:
            raise ValueError("MONGODB_URI not found in environment variables")
        
        client = MongoClient(mongodb_uri, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')  # Test connection
        logger.info("✅ Connected to MongoDB")
        return client
    
    def load_country_names(self) -> List[str]:
        """Load country names from final/country_names.json"""
        try:
            with open('final/country_names.json', 'r', encoding='utf-8') as f:
                countries = json.load(f)
            logger.info(f"📋 Loaded {len(countries)} countries from final/country_names.json")
            return countries
        except Exception as e:
            logger.error(f"Error loading final/country_names.json: {e}")
            return []
    
    def get_unique_first_section_addresses(self, country: str) -> List[Dict]:
        """Get addresses with unique first_section values, sorted by score (highest first)"""
        pipeline = [
            # Match addresses for this country that have first_section field
            {"$match": {
                "country": country,
                "first_section": {"$exists": True, "$ne": None, "$ne": ""},
                "score": {"$exists": True}
            }},
            
            # Sort by score (highest first), then by _id for consistency
            {"$sort": {"score": -1, "_id": 1}},
            
            # Group by first_section to get unique values, keeping the highest score
            {"$group": {
                "_id": "$first_section",
                "address": {"$first": "$address"},
                "doc_id": {"$first": "$_id"},
                "score": {"$first": "$score"},
                "first_section": {"$first": "$first_section"}
            }},
            
            # Sort again by score to maintain order after grouping
            {"$sort": {"score": -1, "doc_id": 1}},
            
            # Limit to target count + buffer to check if we have enough
            {"$limit": self.target_count + 5}
        ]
        
        results = list(self.collection.aggregate(pipeline))
        return [{
            "_id": r["doc_id"], 
            "address": r["address"], 
            "score": r["score"],
            "first_section": r["first_section"]
        } for r in results]
    
    def select_addresses_for_country(self, country: str) -> Dict:
        """Select exactly 15 addresses for a country using unique first_section logic"""
        logger.info(f"🔍 Processing {country}...")
        
        # Get addresses with unique first_section values, sorted by score
        unique_addresses = self.get_unique_first_section_addresses(country)
        
        if len(unique_addresses) < self.target_count:
            # Not enough unique first_section addresses
            return {
                "addresses": [],
                "count": len(unique_addresses),
                "unique_first_sections": len(unique_addresses),
                "average_score": 0,
                "score_range": {"min": 0, "max": 0}
            }
        
        # Select exactly 15 addresses
        selected_addresses = unique_addresses[:self.target_count]
        
        # Calculate statistics
        scores = [addr["score"] for addr in selected_addresses]
        average_score = sum(scores) / len(scores) if scores else 0
        score_range = {"min": min(scores), "max": max(scores)} if scores else {"min": 0, "max": 0}
        
        return {
            "addresses": [addr["address"] for addr in selected_addresses],
            "count": len(selected_addresses),
            "unique_first_sections": len(selected_addresses),
            "average_score": round(average_score, 3),
            "score_range": score_range
        }
    
    def process_all_countries(self):
        """Process all countries and generate address dictionary"""
        logger.info("🚀 Starting address dictionary generation...")
        
        countries = self.load_country_names()
        if not countries:
            logger.error("No countries loaded. Exiting.")
            return
        
        start_time = time.time()
        
        for i, country in enumerate(countries, 1):
            self.report["processed_countries"] += 1
            
            # Progress update
            progress_pct = (i / len(countries)) * 100
            print(f"\r📊 Progress: {progress_pct:.1f}% | {i}/{len(countries)} | Processing: {country[:30]:<30}", end='', flush=True)
            
            try:
                result = self.select_addresses_for_country(country)
                
                if result["count"] == self.target_count:
                    # Success: exactly 15 addresses with unique first_section found
                    self.address_dictionary[country] = result["addresses"]
                    self.report["successful_countries"] += 1
                    
                    self.report["countries"][country] = {
                        "status": "success",
                        "total_found": result["count"],
                        "unique_first_sections": result["unique_first_sections"],
                        "average_score": result["average_score"],
                        "score_range": result["score_range"]
                    }
                    
                    logger.info(f"✅ {country}: {result['count']} addresses selected (avg score: {result['average_score']})")
                    
                else:
                    # Skip: insufficient unique first_section addresses
                    self.report["skipped_countries"] += 1
                    
                    self.report["countries"][country] = {
                        "status": "skipped",
                        "reason": "insufficient_unique_first_sections",
                        "unique_first_sections_found": result["count"],
                        "required": self.target_count,
                        "average_score": result["average_score"],
                        "score_range": result["score_range"]
                    }
                    
                    logger.warning(f"⚠️  {country}: Only {result['count']}/{self.target_count} unique first_sections found - SKIPPED")
                    
            except Exception as e:
                logger.error(f"❌ Error processing {country}: {e}")
                self.report["skipped_countries"] += 1
                
                self.report["countries"][country] = {
                    "status": "error",
                    "reason": str(e),
                    "unique_first_sections_found": 0,
                    "required": self.target_count
                }
        
        elapsed = time.time() - start_time
        
        # Final progress update
        print(f"\r📊 Progress: 100.0% | {len(countries)}/{len(countries)} | Complete!{' ' * 30}", flush=True)
        
        logger.info("=" * 60)
        logger.info("✅ Address dictionary generation completed!")
        logger.info(f"📊 Summary:")
        logger.info(f"   Total countries processed: {self.report['processed_countries']}")
        logger.info(f"   Successful countries: {self.report['successful_countries']}")
        logger.info(f"   Skipped countries: {self.report['skipped_countries']}")
        logger.info(f"   Success rate: {(self.report['successful_countries']/self.report['processed_countries']*100):.1f}%")
        logger.info(f"⏱️  Total time: {elapsed:.1f} seconds")
        logger.info("=" * 60)
    
    def save_files(self):
        """Save address dictionary and report to JSON files"""
        # Save address dictionary
        with open('final/address_dictionary.json', 'w', encoding='utf-8') as f:
            json.dump(self.address_dictionary, f, indent=2, ensure_ascii=False)
        
        # Save report
        with open('final/address_report.json', 'w', encoding='utf-8') as f:
            json.dump(self.report, f, indent=2, ensure_ascii=False)
        
        logger.info("💾 Files saved:")
        logger.info(f"   📖 final/address_dictionary.json ({len(self.address_dictionary)} countries)")
        logger.info(f"   📊 final/address_report.json")
    
    def close(self):
        """Close database connection"""
        if self.client:
            self.client.close()
            logger.info("MongoDB connection closed")

def main():
    """Main function"""
    generator = None
    
    try:
        generator = AddressDictionaryGenerator()
        generator.process_all_countries()
        generator.save_files()
        
        print(f"\n🎉 Address dictionary generation completed!")
        print(f"📈 Results:")
        print(f"   ✅ Successful countries: {generator.report['successful_countries']}")
        print(f"   ⚠️  Skipped countries: {generator.report['skipped_countries']}")
        print(f"   📖 Dictionary size: {len(generator.address_dictionary)} countries")
        print(f"   📊 Total addresses: {len(generator.address_dictionary) * 15:,}")
        print(f"   🎯 Method: Unique first_section selection with score sorting")
        
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        return 1
    
    finally:
        if generator:
            generator.close()
    
    return 0

if __name__ == "__main__":
    import sys
    sys.exit(main())