import unicodedata


def normalize_address(addr_str):
    if not addr_str:
        return ""
    normalized = " ".join(addr_str.split()).lower()
    normalized = normalized.replace(",", " ").replace(";", " ").replace("-", " ")
    normalized = " ".join(normalized.split())
    return normalized


def remove_disallowed_unicode(text: str, preserve_comma: bool = False) -> str:
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


def calculate_address_duplicates_penalty(address_variations):
    address_duplicates_penalty = 0.0

    # -----------------------------
    # 1) Full normalized duplicates
    # -----------------------------
    normalized_addresses = [
        normalize_address(addr)
        for addr in address_variations
        if addr and addr.strip()
    ]

    duplicates_addresses = len(normalized_addresses) - len(set(normalized_addresses))

    if duplicates_addresses > 0:
        address_duplicates_penalty += duplicates_addresses * 0.05


    # ----------------------------------
    # 2) First-section duplicate penalty
    # ----------------------------------
    first_sections = []

    for addr in address_variations:
        if not addr or not addr.strip():
            continue

        # Remove unicode junk but keep commas
        addr = remove_disallowed_unicode(addr, preserve_comma=True)
        if not addr.strip():
            continue

        # Remove leading commas/spaces
        normalized_addr = addr.strip().lstrip(",").strip()
        if not normalized_addr:
            continue

        # Split on commas
        parts = normalized_addr.split(",")

        first_section = parts[0].strip()

        # If too short, merge with second
        if len(first_section) < 4 and len(parts) > 1:
            first_section = (parts[0].strip() + " " + parts[1].strip()).strip()

        # Normalize first section
        words = first_section.split()
        filtered_words = [w for w in words if len(w) > 2]
        normalized_first = " ".join(filtered_words).lower().strip()

        if normalized_first:
            first_sections.append(normalized_first)
    print(first_sections)
    if first_sections:
        first_section_counts = {}
        for section in first_sections:
            first_section_counts[section] = first_section_counts.get(section, 0) + 1
        
        duplicate_first_sections = sum(
            count - 1 for count in first_section_counts.values()
            if count > 1
        )

        if duplicate_first_sections > 0:
            address_duplicates_penalty += duplicate_first_sections * 0.05

    return address_duplicates_penalty

if __name__ == "__main__":
    address = [
    '6794 Jesus Orchard, Sanfordville, Quebec N7R2R6, Canada',
    '6847 Hannah Parkways, Markhaven, Quebec M9K1L9, Canada',
    '18236 Tiffany Road, Shelleyland, Prince Edward Island J5Y 5N5, Canada' ,    
    '41488 Smith Islands, South Angel, Prince Edward Island K3P 4E3, Canada',    
    '06415 Richard Lodge, West Davidmouth, Prince Edward Island E3L 9N8, Canada',
    '340 Daniel Fords, Terrystad, Alberta E8A4H7, Canada',
    '02316 Romero Via, Juliechester, Nova Scotia T9P8H5, Canada',
    '9801 Lowery Pines, Lake Charlesburgh, Manitoba C9E 8V9, Canada',
    '470 Travis Valley, East Jenniferfort, Nunavut E1M2N6, Canada',
    '7348 Duffy Lock, Gravesberg, New Brunswick C1A 1Y2, Canada'
    ]
    print(calculate_address_duplicates_penalty(address))