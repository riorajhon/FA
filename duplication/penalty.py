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
    "Гостиница Холидэй ИНН, 189, 天津街, Qingniwaqiao Subdistrict, Zhongshan District, Ganjingzi, Liaoning, 116001, China",
    "1944, 119, Shuguang Road, 白沙泉, Beishan Subdistrict, Xihu District, Hangzhou City, Zhejiang, 310007, China",
    "479, 文三路, Cuiyuan, Xihu District, Hangzhou City, Zhejiang, 310012, China",
    "444, Middle Jiangxi Road, Waitanyuan, Waitan Subdistrict, Shanghai, Huangpu District, Shanghai, 200002, China",
    "Coco Club, 266, Wantang Road, Cuiyuan, Xihu District, Hangzhou City, Zhejiang, 310012, China",
    "113, Shuguang Road, 白沙泉, Beishan Subdistrict, Xihu District, Hangzhou City, Zhejiang, 310007, China",
    "302, Wenhui Road, Zhaohui, Gongshu District, Hangzhou City, Zhejiang, 310014, China",
    "348, Wenhui Road, Zhaohui, Gongshu District, Hangzhou City, Zhejiang, 310014, China",
    "108, Wenhui Road, 打铁关社区, Wenhui, Gongshu District, Hangzhou City, Zhejiang, 310014, China",
    "196, Shangtang Road, Zhaohui, Gongshu District, Hangzhou City, Zhejiang, 310014, China",
    "圣路易葡国餐厅(高新店), 25, Keji Road, Zhangba, Yanta District, Xi'an, Shaanxi, 710000, China",
    "Mislead, 168, Baochu Road, Beishan Subdistrict, Xihu District, Hangzhou City, Zhejiang, 310028, China",
    "247, 文三路, Cuiyuan, Xihu District, Hangzhou City, Zhejiang, 310028, China",
    "浙江省旅游投资集团有限公司, 555, 文三路, Cuiyuan, Xihu District, Hangzhou City, Zhejiang, 310012, China",
    "德珈超市, 371, Wen'er Road West, 德加社区, 文新街道, Xihu District, Hangzhou City, Zhejiang, 310013, China"
    ]
    print(calculate_address_duplicates_penalty(address))