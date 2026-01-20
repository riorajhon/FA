#!/usr/bin/env python3
"""
Test Low Score Processor
Simple test script to validate the processing algorithm on a few addresses

Usage: python test_processor.py
"""

import os
import sys
import json
import time
import requests
import re
from typing import Dict, List, Optional, Set

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from address_check import looks_like_address, validate_address_region
from address_score import check_with_nominatim
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'duplication'))
from first_section import extract_first_section

class TestProcessor:
    def __init__(self):
        self.request_delay = 1.5
        self.last_request_time = 0
    
    def rate_limit(self):
        """Implement rate limiting for API requests"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        if time_since_last < self.request_delay:
            sleep_time = self.request_delay - time_since_last
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
    
    def query_nominatim_osm_id(self, osm_id: str) -> Optional[Dict]:
        """Query Nominatim using OSM ID with English language preference"""
        try:
            self.rate_limit()
            
            url = "https://nominatim.openstreetmap.org/lookup"
            params = {
                "osm_ids": osm_id,
                "format": "json",
                "accept-language": "en"
            }
            headers = {"User-Agent": "TestProcessor/1.0"}
            
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
            print(f"OSM ID query failed for {osm_id}: {e}")
            return None
    
    def query_nominatim_address(self, address: str) -> Optional[Dict]:
        """Query Nominatim using address with no language preference"""
        try:
            self.rate_limit()
            
            url = "https://nominatim.openstreetmap.org/search"
            params = {
                "q": address,
                "format": "json",
                "limit": 1
            }
            headers = {"User-Agent": "TestProcessor/1.0"}
            
            response = requests.get(url, params=params, headers=headers, timeout=10)
            response.raise_for_status()
            
            results = response.json()
            if results and len(results) > 0:
                result = results[0]
                display_name = result.get('display_name', '')
                
                # Extract numbers from display_name
                number_part1 = set(re.findall(r"[0-9]+", display_name.lower()))
                
                return {
                    'name2': result.get('name', ''),
                    'number_part1': number_part1,
                    'display_name': display_name
                }
            
            return None
            
        except Exception as e:
            print(f"Address query failed for {address}: {e}")
            return None
    
    def process_territory_display_name(self, display_name: str) -> str:
        """Process territory display name"""
        if not display_name:
            return ""
        
        processed = display_name
        
        # Remove common administrative terms
        admin_terms = [
            ", Administrative Region",
            ", Region", 
            ", District",
            ", Prefecture",
            ", Province",
            ", State",
            ", County"
        ]
        
        for term in admin_terms:
            processed = processed.replace(term, "")
        
        return processed.strip()
    
    def clean_numbers_in_display_name(self, display_name: str, allowed_numbers: Set[str]) -> str:
        """
        Remove numbers from display_name that are not in allowed_numbers set
        This includes both standalone numbers and numbers within words (like "5th")
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
    
    def test_single_address(self, address_data: Dict):
        """Test processing a single address"""
        osm_id = address_data.get('osm_id', '')
        original_address = address_data.get('address', '')
        country = address_data.get('country', '')
        
        print(f"\n{'='*80}")
        print(f"Testing OSM ID: {osm_id}")
        print(f"Original Address: {original_address}")
        print(f"Country: {country}")
        print(f"{'='*80}")
        
        # Step 1: Query OSM ID
        print("\n1. Querying OSM ID with English language...")
        osm_result = self.query_nominatim_osm_id(osm_id)
        if not osm_result:
            print("❌ OSM ID query failed")
            return False
        
        print(f"✅ OSM Result:")
        print(f"   Display Name: {osm_result['display_name']}")
        print(f"   Name: {osm_result['name']}")
        
        # Step 2: Query address
        print("\n2. Querying address with no language preference...")
        address_result = self.query_nominatim_address(original_address)
        if not address_result:
            print("❌ Address query failed")
            return False
        
        print(f"✅ Address Result:")
        print(f"   Name2: {address_result['name2']}")
        print(f"   Numbers: {address_result['number_part1']}")
        print(f"   Display Name: {address_result['display_name']}")
        
        # Step 3: Process display name
        print("\n3. Processing display name...")
        display_name = osm_result['display_name']
        name = osm_result['name']
        name2 = address_result['name2']
        number_part1 = address_result['number_part1']
        
        print(f"   Original: {display_name}")
        
        # Apply territory processing
        processed = self.process_territory_display_name(display_name)
        print(f"   After territory processing: {processed}")
        
        # Replace name with name2
        if name and name2:
            processed = processed.replace(name, name2)
            print(f"   After name replacement ({name} → {name2}): {processed}")
        
        # Clean numbers
        final_display_name = self.clean_numbers_in_display_name(processed, number_part1)
        print(f"   Final processed: {final_display_name}")
        
        # Step 4: Validation
        print("\n4. Running validation checks...")
        
        looks_valid = looks_like_address(final_display_name)
        print(f"   looks_like_address: {looks_valid}")
        
        region_valid = validate_address_region(final_display_name, country)
        print(f"   validate_address_region: {region_valid}")
        
        if looks_valid and region_valid:
            nominatim_score = check_with_nominatim(final_display_name)
            print(f"   check_with_nominatim: {nominatim_score}")
            
            if nominatim_score == 1.0:
                print("\n✅ ALL VALIDATIONS PASSED!")
                
                # Show first section
                first_section = extract_first_section(final_display_name)
                print(f"   First section: {first_section}")
                
                return True
            else:
                print(f"\n❌ Nominatim validation failed (score: {nominatim_score})")
        else:
            print("\n❌ Basic validation failed")
        
        return False

def main():
    """Test the processor with a few sample addresses"""
    processor = TestProcessor()
    
    # Load sample addresses
    json_file = os.path.join(os.path.dirname(__file__), 'addresses_score1_low.json')
    
    try:
        with open(json_file, 'r', encoding='utf-8') as f:
            addresses = json.load(f)
        
        print(f"Loaded {len(addresses)} addresses from JSON file")
        
        # Test first 3 addresses with score <= 0
        test_addresses = [addr for addr in addresses if addr.get('score', 0) <= 0][:3]
        
        print(f"Testing {len(test_addresses)} addresses...")
        
        success_count = 0
        for i, address_data in enumerate(test_addresses, 1):
            print(f"\n\n🔍 TEST {i}/{len(test_addresses)}")
            success = processor.test_single_address(address_data)
            if success:
                success_count += 1
        
        print(f"\n\n📊 SUMMARY: {success_count}/{len(test_addresses)} addresses passed all validations")
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()