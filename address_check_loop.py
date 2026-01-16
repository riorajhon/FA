#!/usr/bin/env python3
"""
Address Checker - Loop All Countries
Loops through all addresses in address_dictionary.json and checks their scores
Saves results to JSON file

Usage: python address_check_loop.py [country_name] [output_file]
- No argument: Check all countries, save to output/address_check_results.json
- With country name: Check only that country
- With output file: Specify custom output file path

Examples:
  python address_check_loop.py
  python address_check_loop.py Albania
  python address_check_loop.py Albania output/albania_results.json
"""

import json
import sys
import time
import os
from datetime import datetime
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

def check_address_score(address):
    """Check score for a single address"""
    try:
        result = check_with_nominatim(address)
        
        if isinstance(result, dict):
            return result.get('score', 0.0)
        elif result == "TIMEOUT":
            return "TIMEOUT"
        elif result == "API_ERROR":
            return "API_ERROR"
        else:
            return result if result is not None else 0.0
    except Exception as e:
        print(f"Error checking address: {e}")
        return "ERROR"

def check_country_addresses(country_name, addresses):
    """Check scores for all addresses of a given country"""
    print(f"\n{'='*80}")
    print(f"Country: {country_name}")
    print(f"Total Addresses: {len(addresses)}")
    print(f"{'='*80}\n")
    
    results = {
        'country': country_name,
        'total': len(addresses),
        'checked': 0,
        'score_1_0': 0,
        'score_0_9': 0,
        'score_0_8': 0,
        'score_below_0_8': 0,
        'timeout': 0,
        'error': 0,
        'addresses': []
    }
    
    for i, address in enumerate(addresses, 1):
        # Show progress
        print(f"[{i}/{len(addresses)}] Checking: {address[:70]}{'...' if len(address) > 70 else ''}")
        
        # Check score
        score = check_address_score(address)
        
        # Categorize result
        save_to_json = False  # Flag to determine if we should save this address
        
        if score == "TIMEOUT":
            results['timeout'] += 1
            print(f"  ⏱️  Score: TIMEOUT")
            save_to_json = True
            address_result = {
                'address': address,
                'score': score,
                'category': 'timeout'
            }
        elif score in ["API_ERROR", "ERROR"]:
            results['error'] += 1
            print(f"  ❌ Score: {score}")
            save_to_json = True
            address_result = {
                'address': address,
                'score': score,
                'category': 'error'
            }
        else:
            results['checked'] += 1
            if score >= 1.0:
                results['score_1_0'] += 1
                print(f"  ✅ Score: {score}")
                # Don't save to JSON (score >= 0.9)
            elif score >= 0.9:
                results['score_0_9'] += 1
                print(f"  ✅ Score: {score}")
                # Don't save to JSON (score >= 0.9)
            elif score >= 0.8:
                results['score_0_8'] += 1
                print(f"  ⚠️  Score: {score}")
                save_to_json = True
                address_result = {
                    'address': address,
                    'score': score,
                    'category': 'score_0.8'
                }
            else:
                results['score_below_0_8'] += 1
                print(f"  ❌ Score: {score}")
                save_to_json = True
                address_result = {
                    'address': address,
                    'score': score,
                    'category': 'score_below_0.8'
                }
        
        # Only save addresses with score < 0.9 (or errors/timeouts)
        if save_to_json:
            results['addresses'].append(address_result)
        
        # Rate limiting - be nice to Nominatim
        time.sleep(1)
    
    # Print summary for this country
    print(f"\n{'-'*80}")
    print(f"Summary for {country_name}:")
    print(f"  Total: {results['total']}")
    print(f"  Checked: {results['checked']}")
    print(f"  Score 1.0: {results['score_1_0']} ({results['score_1_0']/results['total']*100:.1f}%)")
    print(f"  Score 0.9-0.99: {results['score_0_9']} ({results['score_0_9']/results['total']*100:.1f}%)")
    print(f"  Score 0.8-0.89: {results['score_0_8']} ({results['score_0_8']/results['total']*100:.1f}%)")
    print(f"  Score <0.8: {results['score_below_0_8']} ({results['score_below_0_8']/results['total']*100:.1f}%)")
    if results['timeout'] > 0:
        print(f"  Timeout: {results['timeout']}")
    if results['error'] > 0:
        print(f"  Error: {results['error']}")
    print(f"  📝 Low score addresses saved to JSON: {len(results['addresses'])}")
    print(f"{'-'*80}\n")
    
    return results

def check_all_countries(address_dict):
    """Check all countries in the address dictionary"""
    total_countries = len(address_dict)
    print(f"🌍 Starting to check {total_countries} countries...")
    
    overall_stats = {
        'total_countries': total_countries,
        'total_addresses': 0,
        'checked': 0,
        'score_1_0': 0,
        'score_0_9': 0,
        'score_0_8': 0,
        'score_below_0_8': 0,
        'timeout': 0,
        'error': 0
    }
    
    all_results = []
    
    for idx, (country_name, addresses) in enumerate(address_dict.items(), 1):
        print(f"\n{'#'*80}")
        print(f"Processing Country {idx}/{total_countries}")
        print(f"{'#'*80}")
        
        results = check_country_addresses(country_name, addresses)
        all_results.append(results)
        
        # Aggregate stats
        overall_stats['total_addresses'] += results['total']
        overall_stats['checked'] += results['checked']
        overall_stats['score_1_0'] += results['score_1_0']
        overall_stats['score_0_9'] += results['score_0_9']
        overall_stats['score_0_8'] += results['score_0_8']
        overall_stats['score_below_0_8'] += results['score_below_0_8']
        overall_stats['timeout'] += results['timeout']
        overall_stats['error'] += results['error']
    
    # Print overall summary
    print(f"\n{'='*80}")
    print(f"OVERALL SUMMARY")
    print(f"{'='*80}")
    print(f"Total Countries: {overall_stats['total_countries']}")
    print(f"Total Addresses: {overall_stats['total_addresses']}")
    print(f"Successfully Checked: {overall_stats['checked']}")
    print(f"\nScore Distribution:")
    print(f"  Score 1.0: {overall_stats['score_1_0']} ({overall_stats['score_1_0']/overall_stats['total_addresses']*100:.1f}%)")
    print(f"  Score 0.9-0.99: {overall_stats['score_0_9']} ({overall_stats['score_0_9']/overall_stats['total_addresses']*100:.1f}%)")
    print(f"  Score 0.8-0.89: {overall_stats['score_0_8']} ({overall_stats['score_0_8']/overall_stats['total_addresses']*100:.1f}%)")
    print(f"  Score <0.8: {overall_stats['score_below_0_8']} ({overall_stats['score_below_0_8']/overall_stats['total_addresses']*100:.1f}%)")
    if overall_stats['timeout'] > 0:
        print(f"\nTimeout: {overall_stats['timeout']}")
    if overall_stats['error'] > 0:
        print(f"Error: {overall_stats['error']}")
    print(f"{'='*80}\n")
    
    return all_results, overall_stats

def save_results_to_json(results, overall_stats, output_file):
    """Save results to JSON file"""
    try:
        # Create output directory if it doesn't exist
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        output_data = {
            'timestamp': datetime.now().isoformat(),
            'summary': overall_stats,
            'countries': results
        }
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False)
        
        print(f"✅ Results saved to: {output_file}")
        return True
    except Exception as e:
        print(f"❌ Error saving results: {e}")
        return False

def main():
    """Main function"""
    # Load address dictionary
    address_dict = load_address_dictionary()
    if not address_dict:
        sys.exit(1)
    
    # Determine output file
    default_output = "output/address_check_results.json"
    output_file = default_output
    
    # Check if specific country requested
    if len(sys.argv) >= 2:
        country_name = sys.argv[1]
        
        # Check if output file specified
        if len(sys.argv) >= 3:
            output_file = sys.argv[2]
        else:
            output_file = f"output/{country_name.lower().replace(' ', '_')}_results.json"
        
        if country_name not in address_dict:
            print(f"❌ Country '{country_name}' not found in address dictionary")
            print(f"\nAvailable countries ({len(address_dict)}):")
            for name in sorted(address_dict.keys()):
                print(f"  - {name}")
            sys.exit(1)
        
        # Check single country
        results = check_country_addresses(country_name, address_dict[country_name])
        
        # Calculate overall stats for single country
        overall_stats = {
            'total_countries': 1,
            'total_addresses': results['total'],
            'checked': results['checked'],
            'score_1_0': results['score_1_0'],
            'score_0_9': results['score_0_9'],
            'score_0_8': results['score_0_8'],
            'score_below_0_8': results['score_below_0_8'],
            'timeout': results['timeout'],
            'error': results['error']
        }
        
        # Save results
        save_results_to_json([results], overall_stats, output_file)
    else:
        # Check all countries
        all_results, overall_stats = check_all_countries(address_dict)
        
        # Save results
        save_results_to_json(all_results, overall_stats, output_file)

if __name__ == "__main__":
    main()
