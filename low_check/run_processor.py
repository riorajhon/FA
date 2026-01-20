#!/usr/bin/env python3
"""
Low Score Processor Runner
Convenient script to run the low score processor with different options

Usage:
    python run_processor.py test           # Test mode (3 addresses)
    python run_processor.py run 50        # Process 50 addresses
    python run_processor.py run           # Process all addresses
"""

import sys
import os
import subprocess

def run_test():
    """Run test processor"""
    print("🧪 Running test processor...")
    script_path = os.path.join(os.path.dirname(__file__), 'test_processor.py')
    subprocess.run([sys.executable, script_path])

def run_processor(limit=None):
    """Run main processor"""
    script_path = os.path.join(os.path.dirname(__file__), 'low_score_processor.py')
    
    if limit:
        print(f"🚀 Running processor with limit: {limit}")
        subprocess.run([sys.executable, script_path, str(limit)])
    else:
        print("🚀 Running processor (no limit)")
        subprocess.run([sys.executable, script_path])

def show_help():
    """Show usage help"""
    print("""
Low Score Processor Runner

Usage:
    python run_processor.py test           # Test mode (3 addresses)
    python run_processor.py run [limit]    # Process addresses with optional limit
    python run_processor.py help          # Show this help

Examples:
    python run_processor.py test          # Test with 3 sample addresses
    python run_processor.py run 10        # Process 10 addresses
    python run_processor.py run           # Process all addresses with score <= 0
    """)

def main():
    if len(sys.argv) < 2:
        show_help()
        return
    
    command = sys.argv[1].lower()
    
    if command == "test":
        run_test()
    elif command == "run":
        limit = None
        if len(sys.argv) > 2:
            try:
                limit = int(sys.argv[2])
            except ValueError:
                print("❌ Invalid limit. Must be a number.")
                return
        run_processor(limit)
    elif command == "help":
        show_help()
    else:
        print(f"❌ Unknown command: {command}")
        show_help()

if __name__ == "__main__":
    main()