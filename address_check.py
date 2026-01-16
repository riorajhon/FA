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
        print(f"\nAvailable countries ({len(address_dict)}):")
        for name in sorted(address_dict.keys())[:10]:
            print(f"  - {name}")
        if len(address_dict) > 10:
            print(f"  ... and {len(address_dict) - 10} more")
        return
    
    addresses = address_dict[country_name]
    print(f"\n{'='*80}")
    print(f"Country: {country_name}")
    print(f"Total Addresses: {len(addresses)}")
    print(f"{'='*80}\n")
    
    stats = {'total': len(addresses), 'score_1_0': 0, 'score_0_9': 0, 'score_below_0_9': 0, 'errors': 0}
    
    for i, address in enumerate(addresses, 1):
        print(f"[{i}/{len(addresses)}] {address[:70]}{'...' if len(address) > 70 else ''}")
        
        # Check score with Nominatim
        result = check_with_nominatim(address)
        
        # Print score with emoji indicator
        if isinstance(result, dict):
            score = result.get('score', 0.0)
            if score >= 1.0:
                print(f"  ✅ Score: {score}")
                stats['score_1_0'] += 1
            elif score >= 0.9:
                print(f"  ✅ Score: {score}")
                stats['score_0_9'] += 1
            else:
                print(f"  ⚠️  Score: {score}")
                stats['score_below_0_9'] += 1
        elif result == "TIMEOUT":
            print("  ⏱️  Score: TIMEOUT")
            stats['errors'] += 1
        elif result == "API_ERROR":
            print("  ❌ Score: API_ERROR")
            stats['errors'] += 1
        else:
            score = result if result is not None else 0.0
            if score >= 1.0:
                print(f"  ✅ Score: {score}")
                stats['score_1_0'] += 1
            elif score >= 0.9:
                print(f"  ✅ Score: {score}")
                stats['score_0_9'] += 1
            else:
                print(f"  ⚠️  Score: {score}")
                stats['score_below_0_9'] += 1
        
        print()
        
        # Small delay to be respectful to Nominatim API
        time.sleep(1)
    
    # Print summary
    print(f"{'='*80}")
    print(f"Summary for {country_name}:")
    print(f"  Total: {stats['total']}")
    print(f"  Score 1.0: {stats['score_1_0']} ({stats['score_1_0']/stats['total']*100:.1f}%)")
    print(f"  Score 0.9-0.99: {stats['score_0_9']} ({stats['score_0_9']/stats['total']*100:.1f}%)")
    print(f"  Score <0.9: {stats['score_below_0_9']} ({stats['score_below_0_9']/stats['total']*100:.1f}%)")
    if stats['errors'] > 0:
        print(f"  Errors: {stats['errors']}")
    print(f"{'='*80}\n")

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