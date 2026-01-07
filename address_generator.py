#!/usr/bin/env python3
"""
Address Generator
Loops through basic/country.json, gets addresses from 'address' collection,
sends to Nominatim and validates like address_validator.py

Requirements: pip install pymongo requests
Usage: python address_generator.py
"""

import json
import sys
import time
import requests
from typing import Dict, List, Optional
from pymongo import MongoClient
import logging

# Import validation functions
sys.path.append('basic')
from address_check import looks_like_address, validate_address_region, compute_bounding_box_areas_meters

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

class AddressGenerator:
    """Generates validated addresses from existing address data using Nominatim"""
    
    def __init__(self, mongodb_uri="mongodb://admin:fjkfjrj!20020415@localhost:27017/?authSource=admin", source_db_name="address"):
        self.client = MongoClient(mongodb_uri)
        self.target_db = self.client.osm_addresses  # Target database
        self.source_db = self.client[source_db_name]  # Source database
        self.source_collection = self.source_db.address  # Source collection
        self.target_collection = self.target_db.validated_addresses  # Target collection
        self.nominatim_base_url = "https://nominatim.openstreetmap.org"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'OSM Address Generator/1.0',
            'Accept-Language': 'en'
        })
        
    def load_countries(self) -> List[str]:
        """Load countries from country.json"""
        try:
            with open('basic/country.json', 'r', encoding='utf-8') as f:
                countries = json.load(f)
            logger.info(f"Loaded {len(countries)} countries from country.json")
            return countries
        except Exception as e:
            logger.error(f"Error loading country.json: {e}")
            return []
    
    def get_addresses_for_country(self, country_name: str, limit: int = 20) -> List[Dict]:
        """Get addresses from source collection for a country with status 0, update to status 1"""
        try:
            # Find addresses with status 0
            addresses = list(self.source_collection.find(
                {'country_name': country_name, 'status': 0}
            ).limit(limit))
            
            # Update their status to 1
            if addresses:
                address_ids = [addr['_id'] for addr in addresses]
                self.source_collection.update_many(
                    {'_id': {'$in': address_ids}},
                    {'$set': {'status': 1}}
                )
                # logger.info(f"Updated {len(addresses)} addresses to status 1 for {country_name}")
            
            return addresses
        except Exception as e:
            logger.warning(f"Error getting addresses for {country_name}: {e}")
            return []
    def query_nominatim_by_address(self, full_address: str, max_retries: int = 1) -> Optional[Dict]:
        """Query Nominatim API by full address with retry logic"""
        try:
            url = f"{self.nominatim_base_url}/search"
            params = {
                'q': full_address,
                'format': 'json',
                'addressdetails': 1,
                'limit': 1,
                'accept-language': 'en'
            }
            
            # Retry logic
            for attempt in range(max_retries):
                try:
                    response = self.session.get(url, params=params, timeout=10)
                    response.raise_for_status()
                    
                    results = response.json()
                    return results[0] if results else None
                    
                except (requests.exceptions.RequestException, requests.exceptions.Timeout) as e:
                    if attempt < max_retries - 1:
                        wait_time = (attempt + 1) * 2
                        # logger.warning(f"Nominatim request failed, retrying in {wait_time}s... (attempt {attempt + 1}/{max_retries})")
                        time.sleep(wait_time)
                        continue
                    else:
                        # logger.warning(f"Nominatim query failed after {max_retries} attempts: {e}")
                        return None
            
        except Exception as e:
            logger.warning(f"Nominatim query failed for '{full_address}': {e}")
            return None
    
    def extract_address_components(self, nominatim_result: Dict) -> Dict:
        """Extract city and street from Nominatim result"""
        address = nominatim_result.get('address', {})
        
        # Extract city (try multiple fields)
        city_fields = ['city', 'town', 'village', 'municipality', 'suburb', 'district']
        city = None
        for field in city_fields:
            if field in address:
                city = address[field]
                break
        
        # Extract street (try multiple fields)
        street_fields = ['road', 'street', 'pedestrian', 'path', 'footway']
        street = None
        for field in street_fields:
            if field in address:
                street = address[field]
                break
        
        return {
            'city': city,
            'street': street,
            'country': address.get('country'),
            'display_name': nominatim_result.get('display_name')
        }
    
    def calculate_score(self, bbox_list: List[str]) -> float:
        """Calculate score based on bounding box size"""
        try:
            # Convert string bbox to float list for computation
            if len(bbox_list) == 4:
                bbox_floats = [float(x) for x in bbox_list]
                bbox_area = compute_bounding_box_areas_meters(bbox_floats)
            else:
                return 0.3
            
            if bbox_area < 100:
                return 1.0
            elif bbox_area < 1000:
                return 0.9
            elif bbox_area < 10000:
                return 0.8
            elif bbox_area < 100000:
                return 0.7
            else:
                return 0.3
                
        except Exception as e:
            logger.warning(f"Bbox calculation failed: {e}")
            return 0.3
    
    def save_address(self, address_data: Dict) -> bool:
        """Save validated address to target collection using upsert, return True if saved, False if duplicate"""
        try:
            # Use upsert with address field to handle duplicate addresses
            result = self.target_collection.update_one(
                {'address': address_data['address']},  # Filter by address text
                {'$set': address_data},                # Update/insert data
                upsert=True                            # Create if doesn't exist
            )
            # Return True if a new document was inserted (upserted_id exists)
            return result.upserted_id is not None
        except Exception as e:
            logger.error(f"Error saving address {address_data.get('osm_id', 'unknown')}: {e}")
            return False
    
    def process_country_addresses(self, country_name: str) -> Dict:
        """Process addresses for a single country"""
        print(f"\n🔄 Processing: {country_name}")
        
        # First, get addresses with status 0 and update them to status 1
        addresses = self.get_addresses_for_country(country_name, limit=40)
        
        if not addresses:
            print(f"   No addresses with status 1 found for {country_name}")
            return {'processed': 0, 'saved': 0}
        
        print(f"   Found {len(addresses)} addresses to process")
        
        stats = {'processed': 0, 'saved': 0}
        
        for i, addr_doc in enumerate(addresses, 1):
            try:
                # Get full address from document (adjust field name as needed)
                full_address = addr_doc.get('fulladdress')
                
                if not full_address:
                    continue

                stats['processed'] += 1
                
                # Query Nominatim
                result = self.query_nominatim_by_address(full_address)

                time.sleep(1)

                if not result:
                    continue

                display_name = result.get('display_name', '')
                
                # Clean special characters from display_name
                special_chars = ['`', ':', '%', '$', '@', '*', '^', '[', ']', '{', '}', '_', '«', '»']
                for char in special_chars:
                    display_name = display_name.replace(char, ' ')
                
                # Clean up multiple spaces
                display_name = ' '.join(display_name.split())
                
                # Validate address format
                if not looks_like_address(display_name):
                    continue
                
                # Validate region
                nominatim_country = result.get('address', {}).get('country', country_name)
                if not validate_address_region(display_name, nominatim_country):
                    continue
                
                # Check place_rank - only save if > 20
                place_rank = result.get('place_rank', 0)
                if place_rank <= 20:
                    continue
                
                # Extract address components
                components = self.extract_address_components(result)
                
                # Calculate score from bounding box
                bbox = result.get('boundingbox', [])
                score = self.calculate_score(bbox) if len(bbox) == 4 else 0.3
                
                if score < 0.9:
                    continue
                    
                # Create osm_id from osm_type and osm_id
                osm_type = result.get('osm_type', '')
                osm_id_num = result.get('osm_id', '')
                
                if osm_type and osm_id_num:
                    # Format like N1234567, W9876543, R5555555
                    osm_id = f"{osm_type[0].upper()}{osm_id_num}"
                else:
                    # Generate unique ID if no OSM data
                    osm_id = f"gen_{addr_doc.get('_id', 'unknown')}"
                
                # Save address
                address_data = {
                    'osm_id': osm_id,
                    'address': display_name,
                    'country': components['country'],
                    'city': components['city'],
                    'street': components['street'],
                    'score': score,
                    'status': 0
                }
                
                # Try to save address, only increment counter if successful
                if self.save_address(address_data):
                    stats['saved'] += 1
                
            except Exception as e:
                # Log error and continue to next address
                logger.warning(f"Error processing address {addr_doc.get('_id', 'unknown')}: {e}")
                continue
            
            # Update progress after each address (successful or not)
            progress_pct = (i / len(addresses)) * 100
            print(f"\r   Progress: {progress_pct:.1f}% | Processed: {i}/{len(addresses)} | Saved: {stats['saved']}", end='', flush=True)
        
        # Final progress update
        print(f"\r   Progress: 100.0% | Processed: {len(addresses)}/{len(addresses)} | Saved: {stats['saved']}", flush=True)
        
        return stats
    
    def process_all_countries(self):
        """Process addresses for all countries"""
        print("🚀 Starting address generation for all countries...")
        
        # Load countries
        countries = self.load_countries()
        if not countries:
            print("❌ No countries loaded")
            return
        
        print(f"📊 Processing {len(countries)} countries...")
        
        total_stats = {
            'countries_processed': 0,
            'total_processed': 0,
            'total_saved': 0
        }
        
        for i, country_name in enumerate(countries, 1):
            print(f"\n📍 Country {i}/{len(countries)}: {country_name}")
            
            try:
                stats = self.process_country_addresses(country_name)
                total_stats['countries_processed'] += 1
                total_stats['total_processed'] += stats['processed']
                total_stats['total_saved'] += stats['saved']
                
            except Exception as e:
                logger.error(f"Error processing {country_name}: {e}")
            
            # Small delay between countries
            time.sleep(2)
        
        print(f"\n\n✅ All countries processed!")
        print(f"📊 Countries processed: {total_stats['countries_processed']}")
        print(f"🔄 Total addresses processed: {total_stats['total_processed']}")
        print(f"💾 Total addresses saved: {total_stats['total_saved']}")
    
    def close(self):
        """Close database connection"""
        self.client.close()

if __name__ == "__main__":
    # Allow specifying source database name as command line argument
    source_db_name = sys.argv[1] if len(sys.argv) > 1 else "address"
    
    print(f"🔄 Using source database: {source_db_name}")
    generator = AddressGenerator(source_db_name=source_db_name)
    
    try:
        generator.process_all_countries()
    except KeyboardInterrupt:
        print("\n⏹️  Processing interrupted by user")
    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)
    finally:
        generator.close()