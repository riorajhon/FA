#!/usr/bin/env python3
"""
JSON Converter
Reads low_score_countries.json and creates a simple country names list
"""

import json
import os

def convert_low_score_countries():
    """Convert low_score_countries.json to simple country names list"""
    
    input_file = 'final/low_score_countries.json'
    output_file = 'final/country_names_only.json'
    
    # Check if input file exists
    if not os.path.exists(input_file):
        print(f"Error: {input_file} not found")
        return
    
    try:
        # Read the input file
        with open(input_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Extract country names
        country_names = []
        
        # Handle both formats: list of objects or list of strings
        for item in data:
            if isinstance(item, dict) and 'country' in item:
                # Format: {"country": "Afghanistan", "count": 5}
                country_names.append(item['country'])
            elif isinstance(item, str):
                # Format: "Afghanistan"
                country_names.append(item)
            else:
                print(f"Warning: Unexpected item format: {item}")
        
        # Save country names only
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(country_names, f, indent=2, ensure_ascii=False)
        
        print(f"✅ Converted {len(country_names)} countries")
        print(f"📄 Input: {input_file}")
        print(f"📄 Output: {output_file}")
        
        # Show first few countries as preview
        if country_names:
            print(f"\n📋 Preview (first 5 countries):")
            for i, country in enumerate(country_names[:5]):
                print(f"  {i+1}. {country}")
            if len(country_names) > 5:
                print(f"  ... and {len(country_names) - 5} more")
        
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in {input_file}: {e}")
    except Exception as e:
        print(f"Error: {e}")

def main():
    """Main function"""
    print("🔄 Converting low_score_countries.json to country names list...")
    convert_low_score_countries()

if __name__ == "__main__":
    main()