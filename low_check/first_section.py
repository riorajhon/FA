#!/usr/bin/env python3
"""
First Section Extractor
Extracts the first section from an address using the same logic as penalty.py
Input: address string
Output: normalized first section

Usage: python first_section_extractor.py
"""

import sys
import unicodedata

def remove_disallowed_unicode(text: str, preserve_comma: bool = False) -> str:
    """Remove disallowed unicode characters (from penalty.py)"""
    allowed = []
    allowed_chars = " ,0123456789" if preserve_comma else " 0123456789"

    for c in text:
        codepoint = ord(c)

        # Exclude phonetic + Latin Extended-D blocks
        if (
            0x1D00 <= codepoint <= 0x1D7F or
            0x1D80 <= codepoint <= 0x1DBF or
            0xA720 <= codepoint <= 0xA7FF
        ):
            continue

        cat = unicodedata.category(c)
        if cat.startswith("L"):       # Letters (any language)
            allowed.append(c)
        elif cat.startswith("M"):     # Diacritics
            allowed.append(c)
        elif c in allowed_chars:      # digits, space, comma
            allowed.append(c)

    return "".join(allowed)

def extract_first_section(address: str) -> str:
    """
    Extract and normalize the first section from an address
    Uses the same logic as penalty.py first-section duplicate penalty
    
    Args:
        address: Full address string
        
    Returns:
        Normalized first section string
    """
    if not address or not address.strip():
        return ""

    # Remove unicode junk but keep commas
    addr = remove_disallowed_unicode(address, preserve_comma=True)
    if not addr.strip():
        return ""

    # Remove leading commas/spaces
    normalized_addr = addr.strip().lstrip(",").strip()
    if not normalized_addr:
        return ""

    # Split on commas
    parts = normalized_addr.split(",")

    first_section = parts[0].strip()

    # If too short, merge with second part
    if len(first_section) < 4 and len(parts) > 1:
        first_section = (parts[0].strip() + " " + parts[1].strip()).strip()

    # Normalize first section
    words = first_section.split()
    filtered_words = [w for w in words if len(w) > 2]
    normalized_first = " ".join(filtered_words).lower().strip()

    return normalized_first

def main():
    """Main function for interactive or command line usage"""
    if len(sys.argv) > 1:
        # Command line usage
        address = " ".join(sys.argv[1:])
        first_section = extract_first_section(address)
        print(f"Address: {address}")
        print(f"First Section: {first_section}")
    else:
        # Interactive usage
        print("First Section Extractor")
        print("=" * 40)
        print("Enter addresses to extract first sections (Ctrl+C to exit)")
        print()
        
        try:
            while True:
                address = input("Address: ").strip()
                if not address:
                    continue
                
                first_section = extract_first_section(address)
                print(f"First Section: {first_section}")
                print("-" * 40)
                
        except KeyboardInterrupt:
            print("\n\nGoodbye!")

if __name__ == "__main__":
    address = "31, Street 103, Tall Al Zaatar, Dekwaneh, Matn District, Mount Lebanon Governorate, 2703, Lebanon'"
    print(extract_first_section(address))