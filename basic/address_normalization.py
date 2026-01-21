#!/usr/bin/env python3
"""
Address Normalization for Deduplication

This module contains the exact address normalization functions used by the MIID validator
for detecting duplicate addresses. Use this to test your addresses before submission
to avoid duplication penalties.

CRITICAL: These functions are copied exactly from MIID/validator/cheat_detection.py
to ensure identical behavior with the validator.
"""

import re
import unicodedata
from typing import List, Set
from unidecode import unidecode


def remove_disallowed_unicode(text: str, preserve_comma: bool = False) -> str:
    """Remove disallowed Unicode characters from text, keeping only:
    - Letters (any language)
    - Marks (diacritics)
    - ASCII digits and space
    - Comma (if preserve_comma=True)
    
    This removes currency symbols (like £), emoji, math operators, etc.
    Also excludes phonetic small-cap blocks AND Latin Extended-D block (U+A720 to U+A7FF)
    which includes characters like ꞎ, ꞙ, ꞟ and similar extended Latin characters.
    
    Args:
        text: The text to clean
        preserve_comma: If True, preserves commas in the output. Defaults to False.
    """
    allowed = []
    
    # Determine which characters to allow based on preserve_comma
    allowed_chars = " ,0123456789" if preserve_comma else " 0123456789"
    
    for c in text:
        codepoint = ord(c)
        
        # ✅ Updated exclusion: phonetic small-cap blocks + Latin Extended-D block
        # Latin Extended-D (U+A720 to U+A7FF) includes characters like ꞎ, ꞙ, ꞟ
        if (
            0x1D00 <= codepoint <= 0x1D7F or  # Phonetic Extensions
            0x1D80 <= codepoint <= 0x1DBF or  # Phonetic Extensions Supplement
            0xA720 <= codepoint <= 0xA7FF      # Latin Extended-D (includes ꞎ, ꞙ, ꞟ)
        ):
            continue
        
        cat = unicodedata.category(c)
        if cat.startswith("L"):       # ✓ Letter (any language)
            allowed.append(c)
        elif cat.startswith("M"):     # ✓ Mark (diacritics)
            allowed.append(c)
        elif c in allowed_chars:      # ✓ ASCII digits, space, and optionally comma
            allowed.append(c)
        else:
            # everything else (symbols, emoji, currency signs, math operators)
            # gets removed
            pass
    return "".join(allowed)


def normalize_address_for_deduplication(addr: str) -> str:
    
    if not addr or not addr.strip():
        return ""
    
    # Step 0: Remove disallowed Unicode characters (currency symbols like £, emoji, etc.)
    text = remove_disallowed_unicode(addr)
    
    # Step 1: Apply Nominatim-style normalization (NFKD + diacritic removal)
    # Unicode normalization (NFKD)
    text = unicodedata.normalize("NFKD", text)
    # Remove diacritics
    text = "".join(c for c in text if not unicodedata.combining(c))
    # Lowercase
    text = text.lower()
    # Replace punctuation and symbols with space (like Nominatim)
    text = re.sub(r"[-:,.;!?(){}\[\]\"'""''/\\|*_=+<>@#^&]", " ", text)
    # Collapse whitespace
    text = re.sub(r"\s+", " ", text)
    # Trim
    text = text.strip(" -:")
    
    # Step 2: Transliterate all non-ASCII characters to ASCII
    # This converts Arabic, Cyrillic, Chinese, etc. to their ASCII equivalents
    text = unidecode(text)
    
    # Step 2.5: Remove numbers
    text = re.sub(r'\d+', ' ', text)
    
    # Step 3: Apply existing deduplication logic
    # Replace commas with spaces (if any remain)
    cleaned = text.replace(",", " ")
    parts = [p for p in cleaned.split(" ") if p]
    # Filter out words that are 1, 2, or 3 characters long
    parts = [p for p in parts if len(p) > 3]
    unique_words = set(parts)
    dedup_text = " ".join(unique_words)
    # Extract letters (non-word, non-digit), excluding specific Unicode chars and lowercase
    letters = re.findall(r'[^\W\d]', dedup_text, flags=re.UNICODE)
    letters = [c.lower() for c in letters if c not in ['\u02BB', '\u02BC']]
    # Sort and join
    normalized = ''.join(sorted(letters))
    
    return normalized

# Example usage and test cases
if __name__ == "__main__":
    address = "علاولدین مارکیت, 123, Kabul Mazar Highway, Puli Khumri, Pul-e Khumri, Baghlan Province, 3601, Afghanistan"
    print(normalize_address_for_deduplication(address))