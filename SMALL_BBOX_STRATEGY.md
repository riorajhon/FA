# Strategy for Finding Small Bbox (<100m²) Addresses

## Problem
You need to find more addresses with small bounding boxes (<100m²) from OSM files. Current filter (`addr:housenumber`) is not enough.

## Solutions

### 1. **Skip Already Validated Addresses** ⭐ MOST IMPORTANT
**File:** `osm_optimized.py`

Before processing OSM file:
- Load all `osm_id` from `validated_addresses` collection for the country
- Store in a Set for O(1) lookup
- Skip any OSM element already in the set

**Benefits:**
- Avoid re-processing millions of addresses
- Focus only on new/unprocessed addresses
- Dramatically faster processing

### 2. **Enhanced Filtering Strategies**

Instead of just checking `addr:housenumber`, use multiple strategies:

#### Strategy A: Buildings with Streets (Best for small bbox)
```python
if 'building' in tags and 'addr:street' in tags:
    return True
```

#### Strategy B: Small Places with Streets
```python
if 'place' in tags and 'addr:street' in tags:
    place_type = tags['place']
    if place_type in ['neighbourhood', 'suburb', 'quarter', 'hamlet', 'isolated_dwelling']:
        return True
```

#### Strategy C: Amenities with Addresses
```python
if 'amenity' in tags and 'addr:street' in tags:
    return True  # shops, restaurants, cafes, etc.
```

#### Strategy D: Commercial Buildings
```python
if 'shop' in tags and 'addr:street' in tags:
    return True
```

#### Strategy E: Tourism & Leisure
```python
if 'tourism' in tags and 'addr:street' in tags:
    return True
if 'leisure' in tags and 'addr:street' in tags:
    return True
```

#### Strategy F: Office Buildings
```python
if 'office' in tags and 'addr:street' in tags:
    return True
```

### 3. **Additional Filters to Consider**

#### Option 1: Check for addr:city or addr:place
```python
has_location = 'addr:city' in tags or 'addr:place' in tags
if 'building' in tags and 'addr:street' in tags and has_location:
    return True
```

#### Option 2: Exclude large administrative areas
```python
# Skip if it's a large administrative boundary
if 'boundary' in tags and tags['boundary'] == 'administrative':
    admin_level = tags.get('admin_level', '99')
    if int(admin_level) <= 6:  # Skip country/state/province level
        return False
```

#### Option 3: Prioritize elements with postcodes
```python
if 'addr:postcode' in tags and 'addr:street' in tags:
    return True  # Postcodes usually indicate precise locations
```

### 4. **Two-Pass Processing Strategy**

For countries with many addresses:

**Pass 1:** Process only high-confidence small bbox addresses
- Buildings + street + housenumber
- Buildings + street + postcode

**Pass 2:** Process broader criteria
- Buildings + street (no housenumber)
- Amenities + street
- Places + street

### 5. **Database Optimization**

Create index on validated_addresses for faster lookup:
```javascript
db.validated_addresses.createIndex({ "country": 1, "osm_id": 1 })
```

## Usage

### Using the Optimized Script
```bash
# Process with skip logic
python osm_optimized.py yemen YE Yemen

# Force JSON output
python osm_optimized.py yemen YE Yemen json
```

### Update Your Existing osm.py
Replace the `check()` method in `osm.py` with the enhanced version from `osm_optimized.py`.

## Expected Results

With these strategies:
- **Skip validated:** 50-90% reduction in processing time
- **Enhanced filters:** 2-5x more small bbox addresses found
- **Better quality:** More precise addresses with streets

## Monitoring Progress

The optimized script shows:
```
Progress: 45.2% | File: 123.4MB/273.1MB | New: 15,234 | Skipped: 145,678 | Batches: 152
```

- **New:** Addresses found that weren't validated yet
- **Skipped:** Already validated addresses (not re-processed)
- **Batches:** Number of batches saved to database

## Next Steps

1. ✅ Use `osm_optimized.py` for new processing
2. Create database index for faster lookups
3. Monitor which strategies find the most small bbox addresses
4. Adjust filters based on results
5. Consider two-pass processing for large countries
