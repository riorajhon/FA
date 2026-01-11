#!/usr/bin/env python3
"""
Address Checker
Takes a country name as input, finds addresses from address dictionary,
and checks their scores using Nominatim
"""

import json
import sys
import time
from basic.address_score import check_with_nominatim

def load_address_dictionary():
    """Load address dictionary from JSON file"""
    try:
        with open('final/address_dictionary.json', 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print("Error: address_dictionary.json not found in final/")
        return None
    except Exception as e:
        print(f"Error loading address dictionary: {e}")
        return None

def check_country_addresses(country_name, address_dict):
    """Check scores for all addresses of a given country"""
    if country_name not in address_dict:
        print(f"Country '{country_name}' not found in address dictionary")
        return
    
    addresses = address_dict[country_name]
    print(f"Checking {len(addresses)} addresses for {country_name}...")
    print("=" * 60)
    
    for i, address in enumerate(addresses, 1):
        print(f"Address {i}: {address[:80]}{'...' if len(address) > 80 else ''}")
        
        # Check score with Nominatim
        result = check_with_nominatim(address)
        
        # Print only the score as requested
        if isinstance(result, dict):
            print(f"Score: {result['score']}")
        elif result == "TIMEOUT":
            print("Score: TIMEOUT")
        elif result == "API_ERROR":
            print("Score: API_ERROR")
        elif result == 0.0:
            print("Score: 0.0")
        else:
            print(f"Score: {result}")
        
        print("-" * 60)
        
        # Small delay to be respectful to Nominatim API
        time.sleep(1)

def main():
    """Main function"""
    if len(sys.argv) != 2:
        print("Usage: python address_check.py <country_name>")
        print("Example: python address_check.py Germany")
        print("Example: python address_check.py \"United States\"")
        sys.exit(1)
    
    country_name = sys.argv[1]
    
    # Load address dictionary
    address_dict = load_address_dictionary()
    if not address_dict:
        sys.exit(1)
    
    # Check addresses for the specified country
    check_country_addresses(country_name, address_dict)

if __name__ == "__main__":
    main()