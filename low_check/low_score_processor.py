#!/usr/bin/env python3
"""
Low Score Address Processor
Processes addresses from addresses_score1_low.json using dual Nominatim queries
and advanced validation to recover valid addresses with improved scores.

Algorithm:
1. Loop through addresses_score1_low.json
2. Skip addresses where score > 0
3. Make dual Nominatim queries (OSM ID + address)
4. Process and clean display names
5. Validate with multiple checks
6. Save valid addresses to database

Usage: python low_score_processor.py [limit]
"""

import os
import sys
import json
import time
import requests
import re
from typing import Dict, List, Optional, Set
from pymongo import MongoClient
import logging
from dotenv import load_dotenv

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'duplication'))

from address_check import looks_like_address, validate_address_region
from address_score import check_with_nominatim
from first_section import extract_first_section
from address_normalization import normalize_address_for_deduplication

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class LowScoreProcessor:
    def __init__(self, mongodb_uri=None):
        """Initialize the processor with database connection"""
        if mongodb_uri is None:
            mongodb_uri = os.getenv('MONGODB_URI')
            if not mongodb_uri:
                raise ValueError("MongoDB URI not found in environment variables")
        
        self.client = MongoClient(mongodb_uri)
        self.db = self.client['osm_addresses']
        self.addresses_collection = self.db['validated_addresses']
        
        # Rate limiting
        self.request_delay = 1.5  # Seconds between requests
        self.last_request_time = 0
        
        # Statistics
        self.stats = {
            'processed': 0,
            'skipped_score_gt_0': 0,
            'osm_query_failed': 0,
            'address_query_failed': 0,
            'validation_failed': 0,
            'saved': 0,
            'errors': 0
        }
    
    def rate_limit(self):
        """Implement rate limiting for API requests"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        if time_since_last < self.request_delay:
            sleep_time = self.request_delay - time_since_last
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
    
    def query_nominatim_osm_id(self, osm_id: str) -> Optional[Dict]:
        """
        Query Nominatim using OSM ID with English language preference
        
        Args:
            osm_id: OSM identifier (e.g., "N264052543")
            
        Returns:
            Dict with display_name and name, or None if failed
        """
        try:
            self.rate_limit()
            
            url = "https://nominatim.openstreetmap.org/lookup"
            params = {
                "osm_ids": osm_id,
                "format": "json",
                "accept-language": "en"
            }
            headers = {"User-Agent": "LowScoreProcessor/1.0"}
            
            response = requests.get(url, params=params, headers=headers, timeout=10)
            response.raise_for_status()
            
            results = response.json()
            if results and len(results) > 0:
                result = results[0]
                return {
                    'display_name': result.get('display_name', ''),
                    'name': result.get('name', '')
                }
            
            return None
            
        except Exception as e:
            logger.error(f"OSM ID query failed for {osm_id}: {e}")
            return None
    
    def query_nominatim_address(self, address: str) -> Optional[Dict]:
        """
        Query Nominatim using address with no language preference
        
        Args:
            address: Full address string
            
        Returns:
            Dict with name2, number_part1, and country info, or None if failed
        """
        try:
            self.rate_limit()
            
            url = "https://nominatim.openstreetmap.org/search"
            params = {
                "q": address,
                "format": "json",
                "limit": 1
            }
            headers = {"User-Agent": "LowScoreProcessor/1.0"}
            
            response = requests.get(url, params=params, headers=headers, timeout=10)
            response.raise_for_status()
            
            results = response.json()
            if results and len(results) > 0:
                result = results[0]
                display_name = result.get('display_name', '')
                
                # Extract numbers from display_name
                number_part1 = set(re.findall(r"[0-9]+", display_name.lower()))
                
                # Extract country from address components or display_name
                address_parts = result.get('address', {})
                nominatim_country = address_parts.get('country', '')
                
                # If no country in address components, try to extract from display_name
                if not nominatim_country and display_name:
                    # Get the last part after the last comma as potential country
                    parts = display_name.split(',')
                    if len(parts) > 1:
                        nominatim_country = parts[-1].strip()
                
                return {
                    'name2': result.get('name', ''),
                    'number_part1': number_part1,
                    'display_name': display_name,
                    'nominatim_country': nominatim_country
                }
            
            return None
            
        except Exception as e:
            logger.error(f"Address query failed for {address}: {e}")
            return None
    
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
    
    def clean_numbers_in_display_name(self, display_name: str, allowed_numbers: Set[str]) -> str:
        """
        Remove numbers from display_name that are not in allowed_numbers set
        This includes both standalone numbers and numbers within words (like "5th")
        
        Args:
            display_name: The display name to clean
            allowed_numbers: Set of allowed number strings
            
        Returns:
            Cleaned display name
        """
        if not display_name or not allowed_numbers:
            return display_name
        
        # Find all numbers in the display name (both standalone and within words)
        all_numbers_in_display = re.findall(r"[0-9]+", display_name)
        
        cleaned = display_name
        
        # Process each number found
        for number in all_numbers_in_display:
            if number not in allowed_numbers:
                # First try to remove standalone numbers (word boundaries)
                standalone_pattern = r'\b' + re.escape(number) + r'\b'
                if re.search(standalone_pattern, cleaned):
                    cleaned = re.sub(standalone_pattern, '', cleaned)
                else:
                    # If not standalone, look for numbers within words (like "5th", "1st", "2nd")
                    # Common patterns: 5th, 1st, 2nd, 3rd, etc.
                    word_patterns = [
                        r'\b' + re.escape(number) + r'th\b',  # 5th, 6th, 7th, etc.
                        r'\b' + re.escape(number) + r'st\b',  # 1st, 21st, 31st, etc.
                        r'\b' + re.escape(number) + r'nd\b',  # 2nd, 22nd, 32nd, etc.
                        r'\b' + re.escape(number) + r'rd\b',  # 3rd, 23rd, 33rd, etc.
                    ]
                    
                    # Try each pattern
                    for pattern in word_patterns:
                        if re.search(pattern, cleaned, re.IGNORECASE):
                            cleaned = re.sub(pattern, '', cleaned, flags=re.IGNORECASE)
                            break
                    else:
                        # If no ordinal pattern found, remove any word containing this number
                        # This handles cases like "District5", "Area123", etc.
                        word_with_number_pattern = r'\b\w*' + re.escape(number) + r'\w*\b'
                        cleaned = re.sub(word_with_number_pattern, '', cleaned)
        
        # Clean up extra spaces and commas
        cleaned = re.sub(r'\s+', ' ', cleaned)  # Multiple spaces to single
        cleaned = re.sub(r',\s*,', ',', cleaned)  # Double commas
        cleaned = re.sub(r'^\s*,\s*', '', cleaned)  # Leading comma
        cleaned = re.sub(r'\s*,\s*$', '', cleaned)  # Trailing comma
        cleaned = re.sub(r'\s*,\s+', ', ', cleaned)  # Normalize comma spacing
        
        return cleaned.strip()
    
    def process_single_address(self, address_data: Dict) -> bool:
        """
        Process a single address through the complete pipeline
        
        Args:
            address_data: Address data from JSON file
            
        Returns:
            True if address was successfully processed and saved, False otherwise
        """
        osm_id = address_data.get('osm_id', '')
        original_address = address_data.get('address', '')
        country = address_data.get('country', '')
        
        logger.info(f"Processing {osm_id}: {original_address[:100]}...")
        
        # Step 1: Query OSM ID with English language
        osm_result = self.query_nominatim_osm_id(osm_id)
        if not osm_result:
            logger.warning(f"OSM ID query failed for {osm_id}")
            self.stats['osm_query_failed'] += 1
            return False
        
        # Step 2: Query address with no language preference
        address_result = self.query_nominatim_address(original_address)
        if not address_result:
            logger.warning(f"Address query failed for {osm_id}")
            self.stats['address_query_failed'] += 1
            return False
        
        # Step 3: Process display name
        display_name = osm_result['display_name']
        name = osm_result['name']
        name2 = address_result['name2']
        number_part1 = address_result['number_part1']
        nominatim_country = address_result.get('nominatim_country', '')
        
        # Apply territory processing
        processed_display_name = self.process_territory_display_name(country, nominatim_country, display_name)
        
        # Replace name with name2 if both exist
        if name and name2:
            processed_display_name = processed_display_name.replace(name, name2)
        
        # Clean numbers not in number_part1
        final_display_name = self.clean_numbers_in_display_name(
            processed_display_name, number_part1
        )
        print(number_part1)
        print(final_display_name)
        logger.info(f"Processed address: {final_display_name}")
        
        # Step 4: Validation pipeline
        if not looks_like_address(final_display_name):
            logger.info(f"Failed looks_like_address check for {osm_id}")
            self.stats['validation_failed'] += 1
            return False
        
        if not validate_address_region(final_display_name, country):
            logger.info(f"Failed validate_address_region check for {osm_id}")
            self.stats['validation_failed'] += 1
            return False
        
        # Step 5: Final Nominatim validation
        nominatim_score = check_with_nominatim(final_display_name)
        if nominatim_score != 1.0:
            logger.info(f"Failed check_with_nominatim (score: {nominatim_score}) for {osm_id}")
            self.stats['validation_failed'] += 1
            return False
        
        # Step 6: Save to database
        try:
            # Extract components for database (reuse original data where possible)
            save_data = {
                'osm_id': osm_id,
                'country': country,
                'city': address_data.get('city', ''),
                'street': address_data.get('street', ''),
                'score': nominatim_score,
                'status': 1,
                'address': final_display_name,
                'first_section': extract_first_section(final_display_name),
                'normalization': normalize_address_for_deduplication(display_name)
            }
            
            # Update existing document or insert new one
            self.addresses_collection.update_one(
                {'osm_id': osm_id},
                {'$set': save_data},
                upsert=True
            )
            
            logger.info(f"✅ Saved address for {osm_id} with score {nominatim_score}")
            self.stats['saved'] += 1
            return True
            
        except Exception as e:
            logger.error(f"Database save failed for {osm_id}: {e}")
            self.stats['errors'] += 1
            return False
    
    def load_addresses_from_json(self, file_path: str) -> List[Dict]:
        """Load addresses from JSON file"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                addresses = json.load(f)
            logger.info(f"Loaded {len(addresses)} addresses from {file_path}")
            return addresses
        except Exception as e:
            logger.error(f"Failed to load addresses from {file_path}: {e}")
            return []
    
    def process_addresses(self, limit: Optional[int] = None):
        """
        Main processing function
        
        Args:
            limit: Maximum number of addresses to process (None for all)
        """
        # Load addresses from JSON file
        json_file = os.path.join(os.path.dirname(__file__), 'addresses_score1_low.json')
        addresses = self.load_addresses_from_json(json_file)
        
        if not addresses:
            logger.error("No addresses to process")
            return
        
        # Filter addresses with score > 0 (skip already processed)
        addresses_to_process = [
            addr for addr in addresses 
            if addr.get('score', 0) <= 0
        ]
        
        logger.info(f"Found {len(addresses_to_process)} addresses with score <= 0 to process")
        
        # Apply limit if specified
        if limit:
            addresses_to_process = addresses_to_process[:limit]
            logger.info(f"Limited to {len(addresses_to_process)} addresses")
        
        # Process each address
        for i, address_data in enumerate(addresses_to_process, 1):
            try:
                logger.info(f"\n--- Processing {i}/{len(addresses_to_process)} ---")
                
                # Skip if score > 0
                if address_data.get('score', 0) > 0:
                    logger.info(f"Skipping address with score > 0")
                    self.stats['skipped_score_gt_0'] += 1
                    continue
                
                # Process the address
                success = self.process_single_address(address_data)
                self.stats['processed'] += 1
                
                # Print progress every 10 addresses
                if i % 10 == 0:
                    self.print_stats()
                
            except Exception as e:
                logger.error(f"Error processing address {i}: {e}")
                self.stats['errors'] += 1
                continue
        
        # Final statistics
        logger.info("\n" + "="*50)
        logger.info("PROCESSING COMPLETE")
        self.print_stats()
    
    def print_stats(self):
        """Print current processing statistics"""
        logger.info("Statistics:")
        for key, value in self.stats.items():
            logger.info(f"  {key}: {value}")
        
        if self.stats['processed'] > 0:
            success_rate = (self.stats['saved'] / self.stats['processed']) * 100
            logger.info(f"  Success rate: {success_rate:.1f}%")
    
    def close_connection(self):
        """Close database connection"""
        if self.client:
            self.client.close()

def main():
    """Main function"""
    # Parse command line arguments
    limit = None
    if len(sys.argv) > 1:
        try:
            limit = int(sys.argv[1])
            print(f"Processing limit set to: {limit}")
        except ValueError:
            print("Invalid limit argument. Using no limit.")
    
    try:
        # Initialize processor
        processor = LowScoreProcessor()
        
        # Process addresses
        processor.process_addresses(limit=limit)
        
        # Close connection
        processor.close_connection()
        
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()