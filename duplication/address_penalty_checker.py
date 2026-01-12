#!/usr/bin/env python3
"""
Address Penalty Checker
Loops through address_dictionary.json and calculates penalty scores using penalty.py
Prints and saves results with country name and penalty score

Requirements: None (uses local penalty.py)
Usage: python address_penalty_checker.py
"""

import json
import sys
import os

# Import penalty calculation function
from penalty import calculate_address_duplicates_penalty

class AddressPenaltyChecker:
    """Checks address penalty scores from dictionary using penalty calculation"""
    
    def __init__(self):
        self.dictionary_file = 'address_dictionary.json'  # File is in basic folder
        self.output_file = 'penalty_results.json'
        self.markdown_file = 'penalty_results.md'
        
    def load_address_dictionary(self):
        """Load addresses from dictionary file"""
        try:
            with open(self.dictionary_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"Error loading address dictionary: {e}")
            return {}
    
    def check_all_countries(self, limit=None):
        """Check penalty scores for all countries in dictionary"""
        print("🚀 Starting Address Penalty Check")
        print("=" * 60)
        
        # Load dictionary
        address_dict = self.load_address_dictionary()
        if not address_dict:
            print("❌ No addresses loaded")
            return []
        
        countries = list(address_dict.keys())
        if limit:
            countries = countries[:limit]
        
        print(f"📊 Checking {len(countries)} countries...")
        
        results = []
        
        for i, country_name in enumerate(countries, 1):
            print(f"\n🔍 {i}/{len(countries)}: {country_name}")
            
            # Get address array for this country
            address_array = address_dict[country_name]
            
            # Calculate penalty score using penalty.py function
            penalty_score = calculate_address_duplicates_penalty(address_array)
            
            # Create result
            result = {
                'country': country_name,
                'penalty_score': penalty_score,
                'address_count': len(address_array)
            }
            
            results.append(result)
            
            # Print result
            penalty_level = "✅ Good" if penalty_score <= 0.3 else "⚠️ Moderate" if penalty_score <= 0.7 else "❌ Bad"
            print(f"    Penalty Score: {penalty_score:.3f} {penalty_level}")
            print(f"    Address Count: {len(address_array)}")
        
        return results
    
    def save_results(self, results):
        """Save results to JSON and Markdown files"""
        # Save JSON
        try:
            with open(self.output_file, 'w', encoding='utf-8') as f:
                json.dump(results, f, indent=2, ensure_ascii=False)
            print(f"\n💾 Results saved to {self.output_file}")
        except Exception as e:
            print(f"Error saving JSON results: {e}")
        
        # Save Markdown
        try:
            self.save_markdown_results(results)
            print(f"📄 Markdown report saved to {self.markdown_file}")
        except Exception as e:
            print(f"Error saving Markdown results: {e}")
    
    def save_markdown_results(self, results):
        """Save results to a beautiful Markdown file"""
        # Sort results by penalty score (lower is better)
        sorted_results = sorted(results, key=lambda x: x['penalty_score'])
        
        with open(self.markdown_file, 'w', encoding='utf-8') as f:
            # Header
            f.write("# Address Penalty Report\n\n")
            f.write("This report shows penalty scores for address duplicates by country.\n")
            f.write("**Lower penalty scores are better** (less duplicates).\n\n")
            
            # Statistics
            total_countries = len(results)
            good_countries = len([r for r in results if r['penalty_score'] <= 0.3])
            moderate_countries = len([r for r in results if 0.3 < r['penalty_score'] <= 0.7])
            bad_countries = len([r for r in results if r['penalty_score'] > 0.7])
            avg_penalty = sum(r['penalty_score'] for r in results) / len(results)
            
            f.write("## Summary Statistics\n\n")
            f.write(f"- **Total Countries**: {total_countries}\n")
            f.write(f"- **Average Penalty**: {avg_penalty:.3f}\n")
            f.write(f"- **Good Countries** (≤0.3): {good_countries} ({good_countries/total_countries*100:.1f}%) ✅\n")
            f.write(f"- **Moderate Countries** (0.3-0.7): {moderate_countries} ({moderate_countries/total_countries*100:.1f}%) ⚠️\n")
            f.write(f"- **Bad Countries** (>0.7): {bad_countries} ({bad_countries/total_countries*100:.1f}%) ❌\n\n")
            
            # Top 10 Best Countries
            f.write("## 🏆 Top 10 Best Countries (Lowest Penalty)\n\n")
            f.write("| Rank | Country | Penalty Score | Status | Address Count |\n")
            f.write("|------|---------|---------------|--------|---------------|\n")
            
            for i, result in enumerate(sorted_results[:10], 1):
                status = "✅ Good" if result['penalty_score'] <= 0.3 else "⚠️ Moderate" if result['penalty_score'] <= 0.7 else "❌ Bad"
                f.write(f"| {i} | {result['country']} | {result['penalty_score']:.3f} | {status} | {result['address_count']} |\n")
            
            # Bottom 10 Worst Countries
            f.write("\n## 📉 Bottom 10 Worst Countries (Highest Penalty)\n\n")
            f.write("| Rank | Country | Penalty Score | Status | Address Count |\n")
            f.write("|------|---------|---------------|--------|---------------|\n")
            
            worst_10 = sorted_results[-10:]
            for i, result in enumerate(worst_10, 1):
                status = "✅ Good" if result['penalty_score'] <= 0.3 else "⚠️ Moderate" if result['penalty_score'] <= 0.7 else "❌ Bad"
                f.write(f"| {i} | {result['country']} | {result['penalty_score']:.3f} | {status} | {result['address_count']} |\n")
            
            # All Countries Table
            f.write("\n## 📊 All Countries (Sorted by Penalty Score)\n\n")
            f.write("| Rank | Country | Penalty Score | Status | Address Count |\n")
            f.write("|------|---------|---------------|--------|---------------|\n")
            
            for i, result in enumerate(sorted_results, 1):
                status = "✅ Good" if result['penalty_score'] <= 0.3 else "⚠️ Moderate" if result['penalty_score'] <= 0.7 else "❌ Bad"
                f.write(f"| {i} | {result['country']} | {result['penalty_score']:.3f} | {status} | {result['address_count']} |\n")
            
            # Legend
            f.write("\n## Legend\n\n")
            f.write("- **✅ Good** (≤0.3): Low penalty, minimal duplicates\n")
            f.write("- **⚠️ Moderate** (0.3-0.7): Some duplicates present\n")
            f.write("- **❌ Bad** (>0.7): High penalty, many duplicates\n\n")
            
            # Footer
            f.write("---\n")
            f.write("*Report generated by Address Penalty Checker*\n")
    
    def print_summary(self, results):
        """Print summary of results"""
        if not results:
            return
        
        print(f"\n{'='*60}")
        print("📊 PENALTY SUMMARY")
        print(f"{'='*60}")
        
        # Sort by penalty score (lower is better)
        sorted_results = sorted(results, key=lambda x: x['penalty_score'])
        
        print(f"\n🏆 TOP 10 COUNTRIES (LOWEST PENALTY - BEST):")
        for i, result in enumerate(sorted_results[:10], 1):
            penalty_level = "✅" if result['penalty_score'] <= 0.3 else "⚠️" if result['penalty_score'] <= 0.7 else "❌"
            print(f"  {i:2d}. {result['country']:<25} Penalty: {result['penalty_score']:.3f} {penalty_level}")
        
        print(f"\n📉 BOTTOM 10 COUNTRIES (HIGHEST PENALTY - WORST):")
        for i, result in enumerate(sorted_results[-10:], 1):
            penalty_level = "✅" if result['penalty_score'] <= 0.3 else "⚠️" if result['penalty_score'] <= 0.7 else "❌"
            print(f"  {i:2d}. {result['country']:<25} Penalty: {result['penalty_score']:.3f} {penalty_level}")
        
        # Statistics
        total_countries = len(results)
        good_countries = len([r for r in results if r['penalty_score'] <= 0.3])
        moderate_countries = len([r for r in results if 0.3 < r['penalty_score'] <= 0.7])
        bad_countries = len([r for r in results if r['penalty_score'] > 0.7])
        
        avg_penalty = sum(r['penalty_score'] for r in results) / len(results)
        
        print(f"\n📊 OVERALL STATISTICS:")
        print(f"    Total countries: {total_countries}")
        print(f"    Average penalty: {avg_penalty:.3f}")
        print(f"    Good (≤0.3): {good_countries} ({good_countries/total_countries*100:.1f}%) ✅")
        print(f"    Moderate (0.3-0.7): {moderate_countries} ({moderate_countries/total_countries*100:.1f}%) ⚠️")
        print(f"    Bad (>0.7): {bad_countries} ({bad_countries/total_countries*100:.1f}%) ❌")

def main():
    """Main function"""
    print("Address Penalty Checker")
    print("Using penalty.py calculate_address_duplicates_penalty function")
    
    # Check command line arguments
    limit = None
    if len(sys.argv) > 1:
        try:
            limit = int(sys.argv[1])
            print(f"Limiting to first {limit} countries")
        except ValueError:
            print("Invalid limit argument, checking all countries")
    
    checker = AddressPenaltyChecker()
    
    try:
        # Check all countries
        results = checker.check_all_countries(limit=limit)
        
        if results:
            # Save results to JSON
            checker.save_results(results)
            
            # Print summary
            checker.print_summary(results)
            
            print(f"\n✅ Penalty check completed for {len(results)} countries!")
        else:
            print("❌ No results to save")
        
    except KeyboardInterrupt:
        print("\n⏹️  Check interrupted by user")
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()