#!/usr/bin/env python3
"""
Address Validator - Processes OSM IDs from database using Nominatim
Fetches address batches, validates addresses, and saves results

Requirements: pip install pymongo requests
Usage: python address_validator.py <country_name> <limit>
"""

import os
import sys
import json
import time
import requests
import re
from typing import Dict, List, Optional, Tuple
from pymongo import MongoClient
import logging
from dotenv import load_dotenv

# Import first_section extractor
sys.path.append(os.path.join(os.path.dirname(__file__), 'duplication'))
from first_section import extract_first_section

# Import address normalization
sys.path.append(os.path.join(os.path.dirname(__file__), 'basic'))
from address_normalization import normalize_address_for_deduplication

# Load environment variables
load_dotenv()

# Import validation functions from basic module
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '.'))
from basic.address_check import looks_like_address, validate_address_region, compute_bounding_box_areas_meters
from basic.address_score import check_with_nominatim

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

class AddressValidator:
    """Validates OSM addresses using Nominatim API"""
    
    def __init__(self, mongodb_uri=None):
        # Use environment variable or default
        if mongodb_uri is None:
            mongodb_uri = os.getenv('MONGODB_URI', 'mongodb://admin:fjkfjrj!20020415@localhost:27017/?authSource=admin')
        
        self.client = MongoClient(mongodb_uri)
        self.db = self.client.osm_addresses
        self.batches_collection = self.db.address_batches2
        self.addresses_collection = self.db.validated_addresses
        self.nominatim_base_url = "https://nominatim.openstreetmap.org"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Check Address/1.0',
            'Accept-Language': 'en'
        })
        
        # Initialize JSON file for special addresses
        self.json_addresses = []
        self.json_filename = "addresses_score1_low.json"
        
        # Initialize JSON file for empty OSM results
        self.empty_osm_ids = []
        self.empty_osm_filename = "empty_osm_results.json"
        
        self.territories = [
            "Martinique",
            "French Guiana", 
            "French Polynesia",
            "Guadeloupe",
            "Reunion",
            "Mayotte",
            "New Caledonia",
            "Puerto Rico",
            "Guam",
            "U.S. Virgin Islands",
            "Hong Kong",
            "Macao"
        ]
        
        # Special processing rules
        self.special_rules = {
            "Reunion": {
                "address_replace": {"Réunion": "Reunion"},
                "country": "Reunion"
            },
            "U.S. Virgin Islands": {
                "address_replace": {"United States Virgin Islands": "U.S. Virgin Islands"},
                "country": "U.S. Virgin Islands"
            },
            "Macao": {
                "address_append": ", Macao",
                "country": "Macao"
            }
        }
        
    def get_address_batches(self, country_name: str, limit: int) -> List[Dict]:
        """Get address batches from database"""
        query = {
            'country_name': country_name,
            'status': 'origin'
        }
        
        batches = list(self.batches_collection.find(query).limit(limit))
        logger.info(f"Found {len(batches)} batches for {country_name}")
        return batches
    
    def update_batch_status(self, batch_id, status: str):
        """Update batch status in database"""
        self.batches_collection.update_one(
            {'_id': batch_id},
            {'$set': {'status': status}}
        )
    
    def query_nominatim(self, osm_id: str, max_retries: int = 3) -> Optional[Dict]:
        """Query Nominatim API for OSM ID with retry logic"""
        try:
            # Extract type and ID (N123 -> type=N, id=123)
            osm_type = osm_id[0].lower()
            osm_number = osm_id[1:]
            
            # Map OSM types
            type_mapping = {'n': 'node', 'w': 'way', 'r': 'relation'}
            if osm_type not in type_mapping:
                return None
            
            url = f"{self.nominatim_base_url}/lookup"
            params = {
                'osm_ids': f"{osm_type.upper()}{osm_number}",
                'format': 'json',
                'addressdetails': 1,
                'extratags': 1,
                'accept-language': 'en'
            }
            
            # Retry logic
            for attempt in range(max_retries):
                try:
                    response = self.session.get(url, params=params, timeout=10)
                    response.raise_for_status()
                    
                    results = response.json()
                    
                    # Check if request was successful but results are empty
                    if response.status_code == 200 and not results:
                        # Save empty result to JSON
                        self.save_empty_osm_id(osm_id, osm_type.upper(), osm_number)
                        return None
                    
                    return results[0] if results else None
                    
                except (requests.exceptions.RequestException, requests.exceptions.Timeout) as e:
                    if attempt < max_retries - 1:
                        wait_time = (attempt + 1) * 2  # 2, 4, 6 seconds
                        logger.warning(f"Nominatim request failed for {osm_id}, retrying in {wait_time}s... (attempt {attempt + 1}/{max_retries})")
                        time.sleep(wait_time)
                        continue
                    else:
                        logger.warning(f"Nominatim query failed for {osm_id} after {max_retries} attempts: {e}")
                        return None
            
        except Exception as e:
            logger.warning(f"Nominatim query failed for {osm_id}: {e}")
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
            bbox_area = compute_bounding_box_areas_meters(bbox_list)
            
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
    
    def save_address_to_json(self, address_data: Dict):
        """Save address to JSON file for addresses with score_1=1 and score<1"""
        try:
            self.json_addresses.append(address_data)
            print(f"📝 Saved to JSON: {address_data['address']} (score_1={address_data['score_1']}, score={address_data['score']})")
        except Exception as e:
            logger.error(f"Error saving address to JSON: {e}")
    
    def save_empty_osm_id(self, osm_id: str, osm_type: str, osm_number: str):
        """Save OSM ID with empty result to JSON file"""
        try:
            self.empty_osm_ids.append(osm_id)
            print(f"⚠️  Empty result: {osm_id}")
        except Exception as e:
            logger.error(f"Error saving empty OSM ID: {e}")
    
    def save_json_file(self):
        """Save collected addresses to JSON file, combining with existing data"""
        if self.json_addresses:
            try:
                existing_data = []
                
                # Load existing data if file exists
                if os.path.exists(self.json_filename):
                    try:
                        with open(self.json_filename, 'r', encoding='utf-8') as f:
                            existing_data = json.load(f)
                        logger.info(f"Loaded {len(existing_data)} existing addresses from {self.json_filename}")
                    except Exception as e:
                        logger.warning(f"Could not load existing JSON file: {e}")
                
                # Combine existing and new data, avoiding duplicates by osm_id
                existing_osm_ids = {addr.get('osm_id') for addr in existing_data if addr.get('osm_id')}
                new_addresses = [addr for addr in self.json_addresses if addr.get('osm_id') not in existing_osm_ids]
                
                combined_data = existing_data + new_addresses
                
                # Save combined data
                with open(self.json_filename, 'w', encoding='utf-8') as f:
                    json.dump(combined_data, f, indent=2, ensure_ascii=False)
                
                logger.info(f"Saved {len(combined_data)} total addresses to {self.json_filename} ({len(existing_data)} existing + {len(new_addresses)} new)")
            except Exception as e:
                logger.error(f"Error saving JSON file: {e}")
    
    def save_empty_osm_file(self):
        """Save empty OSM IDs to JSON file, combining with existing data"""
        if self.empty_osm_ids:
            try:
                existing_data = []
                
                # Load existing data if file exists
                if os.path.exists(self.empty_osm_filename):
                    try:
                        with open(self.empty_osm_filename, 'r', encoding='utf-8') as f:
                            existing_data = json.load(f)
                        logger.info(f"Loaded {len(existing_data)} existing empty OSM IDs from {self.empty_osm_filename}")
                    except Exception as e:
                        logger.warning(f"Could not load existing empty OSM file: {e}")
                
                # Combine existing and new data, avoiding duplicates
                existing_set = set(existing_data)
                new_empty_ids = [osm_id for osm_id in self.empty_osm_ids if osm_id not in existing_set]
                
                combined_data = existing_data + new_empty_ids
                
                # Save combined data
                with open(self.empty_osm_filename, 'w', encoding='utf-8') as f:
                    json.dump(combined_data, f, indent=2, ensure_ascii=False)
                
                logger.info(f"Saved {len(combined_data)} total empty OSM IDs to {self.empty_osm_filename} ({len(existing_data)} existing + {len(new_empty_ids)} new)")
            except Exception as e:
                logger.error(f"Error saving empty OSM file: {e}")
    
    def save_address(self, address_data: Dict, score):
        """Save validated address to database using upsert to handle duplicates"""
        try:
            # Use upsert with address field to handle duplicate addresses
            self.addresses_collection.update_one(
                {'address': address_data['address']},  # Filter by address text
                {'$set': address_data},                # Update/insert data
                upsert=True                            # Create if doesn't exist
            )
            print(f"\n{address_data['address']} : {score}")
        except Exception as e:
            logger.error(f"Error saving address {address_data.get('osm_id', 'unknown')}: {e}")
            raise
    
    def trim_address_to_territory(self, address: str, territory: str) -> str:
        """Trim address from start to the territory name (inclusive)"""
        # Find the territory in the address (case-insensitive)
        
        territory1 = territory
        
        if territory == "Reunion":
            territory1 = "Réunion"
        if territory == "U.S. Virgin IslandsUnited":
            territory1 = "United States Virgin Islands"
        if territory == "Macao":
            territory1 = "Macau"
            
        pattern = re.compile(re.escape(territory1), re.IGNORECASE)
        match = pattern.search(address)
       
        if match:
            # Trim from start to end of territory name
            end_pos = match.end()
            trimmed = address[:end_pos].strip()
            return trimmed
        
        return address  # Return original if territory not found
    
    def apply_special_rules(self, address: str, territory: str) -> Tuple[str, str]:
        """Apply special processing rules for specific territories"""
        if territory not in self.special_rules:
            return address, territory
        
        rules = self.special_rules[territory]
        processed_address = address
        
        # Apply address replacements
        if "address_replace" in rules:
            for old_text, new_text in rules["address_replace"].items():
                processed_address = processed_address.replace(old_text, new_text)
        
        # Apply address append
        if "address_append" in rules:
            processed_address += rules["address_append"]
        
        # Get new country name
        new_country = rules.get("country", territory)
        
        return processed_address, new_country
    def process_territory_display_name(self, country_name: str, nominatim_country: str, display_name: str) -> str:
        """
        Process display name for territories by removing nominatim country and adding territory name
        
        Args:
            country_name: Target territory name (e.g., "Reunion")
            nominatim_country: Country from Nominatim (e.g., "France") 
            display_name: Original display name
            
        Returns:
            Modified display name with territory name instead of nominatim country
        """
        
        # Territory variations to remove (only one variation per territory)
        territory_variations = {
            "Reunion": "Réunion,",
            "U.S. Virgin Islands": "United States Virgin Islands,",
            "Hong Kong": "Hong Kong,",
            "Martinique": "Martinique,",
            "French Guiana": "French Guiana,", 
            "French Polynesia": "French Polynesia,",
            "Guadeloupe": "Guadeloupe,",
            "Mayotte": "Mayotte,",
            "New Caledonia": "New Caledonia,",
            "Puerto Rico": "Puerto Rico,",
            "Guam": "Guam,",
            "Macao": "Macao"
        }
        
        # Check if this is a territory we need to process
        if country_name not in territory_variations:
            return display_name
        
        # Remove nominatim_country from display_name
        if nominatim_country and nominatim_country in display_name:
            display_name = display_name.replace(nominatim_country, "").strip()
            # Clean up any double commas or spaces
            display_name = display_name.replace(",,", ",").strip(", ")
        
        # Remove existing territory variation
        variation_to_remove = territory_variations[country_name]
        if variation_to_remove in display_name:
            display_name = display_name.replace(variation_to_remove, "").strip(", ")
            
        # print("----------------")    
        # print(variation_to_remove, nominatim_country)
        # print("----------------")    
        
        # Add the country_name at the end
        if display_name:
            display_name = f"{display_name}, {country_name}"
        else:
            display_name = country_name
        
        return display_name
            
    def process_batch(self, batch: Dict, country_name: str) -> Dict:
        """Process a single batch of OSM IDs"""
        batch_id = batch['_id']
        ids_string = batch['ids']
        
        # Update status to 'checking'
        self.update_batch_status(batch_id, 'checking')
        
        # Split IDs and process
        osm_ids = ids_string.split(',')
        stats = {
            'total_ids': len(osm_ids),
            'saved': 0
        }
        
        processed_count = 0
        
        for osm_id in osm_ids:
            osm_id = osm_id.strip()
            if not osm_id:
                continue
            
            processed_count += 1
            
            # Update progress every 10 IDs
            if processed_count % 10 == 0 or processed_count == len(osm_ids):
                progress_pct = (processed_count / len(osm_ids)) * 100
                print(f"\r📊 Progress: {progress_pct:.1f}% | Processed: {processed_count}/{len(osm_ids)} | Saved: {stats['saved']}", end='', flush=True)
            
            # Query Nominatim
            result = self.query_nominatim(osm_id)
            if not result:
                continue
            
            display_name = result.get('display_name', '')
            
            # Clean special characters from display_name
            special_chars = ['`', ':', '%', '$', '@', '*', '^', '[', ']', '{', '}', '_', '«', '»']
            for char in special_chars:
                display_name = display_name.replace(char, ' ')
                
            match country_name:
                case "Aruba":
                    display_name = display_name.replace(', Netherlands', ' ')
                case "Curacao":
                    display_name = display_name.replace(', Netherlands', ' ')
                    display_name = display_name.replace('Curacao,', ' ')
                    display_name = f"{display_name}, {country_name}"
                case "Cabo Verde":
                    display_name = display_name.replace(', Cape Verde', ', Cabo Verde')
                case "Palestinian Territory":
                    display_name = display_name.replace('Palestinian Territories', 'Palestinian Territory')
                case "Republic of the Congo":
                    display_name = display_name.replace('Congo-Brazzaville', 'Republic of the Congo')
                case "Timor Leste":
                    display_name = display_name.replace('East Timor', 'Timor Leste')
                case "Maldives":
                    display_name = display_name.replace('é', 'e')
                case "Montserrat":
                    display_name = display_name.replace("MSR", '')
                # case "Montserrat":
                #     postcode = result.get('address', {}).get('postcode', '')
                #     display_name = display_name.replace(postcode, '')
                case _:
                    nominatim_country = country_name
            
            if country_name in self.territories:
                nominatim_country = result.get('address', {}).get('country', country_name)
                display_name = self.process_territory_display_name(country_name, nominatim_country, display_name)
                # display_name = self.trim_address_to_territory(display_name, country_name)
                # display_name, nominatim_country = self.apply_special_rules(display_name, country_name)
                
            nominatim_country = result.get('address', {}).get('country', country_name)    
            # Process territories display names
            # display_name = self.process_territory_display_name(country_name, nominatim_country, display_name)
                
            display_name = ' '.join(display_name.split())

            if not looks_like_address(display_name):
                print("failed looks_like_address")
                continue
            # print(display_name)
            # print(nominatim_country)
            if not validate_address_region(display_name, nominatim_country):
                print("failed validate_address_region")
                continue
            
            # Check place_rank - only save if > 20
            place_rank = result.get('place_rank', 0)
            if place_rank <= 20:
                continue
            
            # Extract address components
            components = self.extract_address_components(result)
            
            # Calculate score from bounding box
            bbox = result.get('boundingbox', [])
            score_1 = self.calculate_score(bbox) if len(bbox) == 4 else 0.3
            score = check_with_nominatim(display_name)
            
            # Check if we should save to JSON (score_1 = 1 AND score < 1)
            should_save_to_json = (score_1 == 1.0 and score == 0)
            
            # Save to JSON if conditions are met
            if should_save_to_json:
                self.save_address_to_json({
                    'osm_id': osm_id,
                    'country': nominatim_country,
                    'city': components['city'],
                    'street': components['street'],
                    'score': score,
                    'score_1': score_1,
                    'status': 1,
                    'address': display_name  # Add this field to satisfy the existing index
                })
                
            if score < 0.9:
                print(f"{ score } failed with score < 0.9")
                continue

            # Save address
            address_data = {
                'osm_id': osm_id,
                'country': nominatim_country,
                'city': components['city'],
                'street': components['street'],
                'score': score,
                'status': 1,
                'address': display_name,  # Add this field to satisfy the existing index
                'first_section': extract_first_section(display_name),  # Add first_section field
                'normalization': normalize_address_for_deduplication(display_name)  # Add normalization field
            }
            
            self.save_address(address_data, score)
            stats['saved'] += 1
            
            # Rate limiting - be nice to Nominatim
            time.sleep(1)
        
        # Final progress update
        print(f"\r📊 Progress: 100.0% | Processed: {len(osm_ids)}/{len(osm_ids)} | Saved: {stats['saved']}", flush=True)
        
        # Update status to 'checked' when finished
        self.update_batch_status(batch_id, 'checked')
        
        return stats
            # Validate address format
    
    def process_country(self, country_name: str, limit: int):
        """Process all batches for a country"""
        # Get batches
        batches = self.get_address_batches(country_name, limit)
        if not batches:
            return
        
        total_saved = 0
        
        for i, batch in enumerate(batches, 1):
            try:
                stats = self.process_batch(batch, country_name)
                total_saved += stats['saved']
            except Exception as e:
                logger.error(f"Error processing batch {batch['_id']}: {e}")
        
        # Save JSON file with special addresses
        self.save_json_file()
        
        # Save JSON file with empty OSM results
        self.save_empty_osm_file()
        
        print(f"Complete! Addresses saved: {total_saved}")
        if self.json_addresses:
            print(f"Special addresses (score_1=1, score<1) saved to {self.json_filename}: {len(self.json_addresses)}")
        if self.empty_osm_ids:
            print(f"Empty OSM results saved to {self.empty_osm_filename}: {len(self.empty_osm_ids)}")
    
    def close(self):
        """Close database connection"""
        self.client.close()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        # print("Usage: python address_validator.py <country_name> <limit>")
        # print("Example: python address_validator.py Yemen 10")
        sys.exit(1)
    
    country_name = sys.argv[1]
    try:
        limit = int(sys.argv[2])
    except ValueError:
        print("Error: limit must be a number")
        sys.exit(1)
    
    validator = AddressValidator()
    
    try:
        validator.process_country(country_name, limit)
    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)
    finally:
        validator.close()