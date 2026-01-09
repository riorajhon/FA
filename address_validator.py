#!/usr/bin/env python3
"""
Address Validator - Processes OSM IDs from database using Nominatim
Fetches address batches, validates addresses, and saves results

Requirements: pip install pymongo requests
Usage: python address_validator.py <country_name> <limit>
"""

import os
import sys
import json
import time
import requests
from typing import Dict, List, Optional
from pymongo import MongoClient
import logging

# Import validation functions from basic module
sys.path.append('basic')
from address_check import looks_like_address, validate_address_region, compute_bounding_box_areas_meters

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

class AddressValidator:
    """Validates OSM addresses using Nominatim API"""
    
    def __init__(self, mongodb_uri="mongodb://admin:fjkfjrj!20020415@localhost:27017/?authSource=admin"):
        self.client = MongoClient(mongodb_uri)
        self.db = self.client.osm_addresses
        self.batches_collection = self.db.address_batches
        self.addresses_collection = self.db.validated_addresses
        self.nominatim_base_url = "https://nominatim.openstreetmap.org"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'OSM Address Validator/1.0',
            'Accept-Language': 'en'
        })
        
    def get_address_batches(self, country_name: str, limit: int) -> List[Dict]:
        """Get address batches from database"""
        query = {
            'country_name': country_name,
            'status': 'origin'
        }
        
        batches = list(self.batches_collection.find(query).limit(limit))
        logger.info(f"Found {len(batches)} batches for {country_name}")
        return batches
    
    def update_batch_status(self, batch_id, status: str):
        """Update batch status in database"""
        self.batches_collection.update_one(
            {'_id': batch_id},
            {'$set': {'status': status}}
        )
    
    def query_nominatim(self, osm_id: str, max_retries: int = 3) -> Optional[Dict]:
        """Query Nominatim API for OSM ID with retry logic"""
        try:
            # Extract type and ID (N123 -> type=N, id=123)
            osm_type = osm_id[0].lower()
            osm_number = osm_id[1:]
            
            # Map OSM types
            type_mapping = {'n': 'node', 'w': 'way', 'r': 'relation'}
            if osm_type not in type_mapping:
                return None
            
            url = f"{self.nominatim_base_url}/lookup"
            params = {
                'osm_ids': f"{osm_type.upper()}{osm_number}",
                'format': 'json',
                'addressdetails': 1,
                'extratags': 1,
                'accept-language': 'en'
            }
            
            # Retry logic
            for attempt in range(max_retries):
                try:
                    response = self.session.get(url, params=params, timeout=10)
                    response.raise_for_status()
                    
                    results = response.json()
                    return results[0] if results else None
                    
                except (requests.exceptions.RequestException, requests.exceptions.Timeout) as e:
                    if attempt < max_retries - 1:
                        wait_time = (attempt + 1) * 2  # 2, 4, 6 seconds
                        logger.warning(f"Nominatim request failed for {osm_id}, retrying in {wait_time}s... (attempt {attempt + 1}/{max_retries})")
                        time.sleep(wait_time)
                        continue
                    else:
                        logger.warning(f"Nominatim query failed for {osm_id} after {max_retries} attempts: {e}")
                        return None
            
        except Exception as e:
            logger.warning(f"Nominatim query failed for {osm_id}: {e}")
            return None
    
    def extract_address_components(self, nominatim_result: Dict) -> Dict:
        """Extract city and street from Nominatim result"""
        address = nominatim_result.get('address', {})
        
        # Extract city (try multiple fields)
        city_fields = ['city', 'town', 'village', 'municipality', 'suburb', 'district']
        city = None
        for field in city_fields:
            if field in address:
                city = address[field]
                break
        
        # Extract street (try multiple fields)
        street_fields = ['road', 'street', 'pedestrian', 'path', 'footway']
        street = None
        for field in street_fields:
            if field in address:
                street = address[field]
                break
        
        return {
            'city': city,
            'street': street,
            'country': address.get('country'),
            'display_name': nominatim_result.get('display_name')
        }
    
    def calculate_score(self, bbox_list: List[str]) -> float:
        """Calculate score based on bounding box size"""
        try:
            bbox_area = compute_bounding_box_areas_meters(bbox_list)
            
            if bbox_area < 100:
                return 1.0
            elif bbox_area < 1000:
                return 0.9
            elif bbox_area < 10000:
                return 0.8
            elif bbox_area < 100000:
                return 0.7
            else:
                return 0.3
                
        except Exception as e:
            logger.warning(f"Bbox calculation failed: {e}")
            return 0.3
    
    def save_address(self, address_data: Dict):
        """Save validated address to database using upsert to handle duplicates"""
        try:
            # Use upsert with address field to handle duplicate addresses
            self.addresses_collection.update_one(
                {'address': address_data['address']},  # Filter by address text
                {'$set': address_data},                # Update/insert data
                upsert=True                            # Create if doesn't exist
            )
            print(f"\n{address_data['address']}")
        except Exception as e:
            logger.error(f"Error saving address {address_data.get('osm_id', 'unknown')}: {e}")
            raise
    
    def process_batch(self, batch: Dict, country_name: str) -> Dict:
        """Process a single batch of OSM IDs"""
        batch_id = batch['_id']
        ids_string = batch['ids']
        
        # Update status to 'checking'
        self.update_batch_status(batch_id, 'checking')
        
        # Split IDs and process
        osm_ids = ids_string.split(',')
        stats = {
            'total_ids': len(osm_ids),
            'saved': 0
        }
        
        processed_count = 0
        
        for osm_id in osm_ids:
            osm_id = osm_id.strip()
            if not osm_id:
                continue
            
            processed_count += 1
            
            # Update progress every 10 IDs
            if processed_count % 10 == 0 or processed_count == len(osm_ids):
                progress_pct = (processed_count / len(osm_ids)) * 100
                print(f"\r📊 Progress: {progress_pct:.1f}% | Processed: {processed_count}/{len(osm_ids)} | Saved: {stats['saved']}", end='', flush=True)
            
            # Query Nominatim
            result = self.query_nominatim(osm_id)
            if not result:
                continue
            
            display_name = result.get('display_name', '')
            
            # Clean special characters from display_name
            special_chars = ['`', ':', '%', '$', '@', '*', '^', '[', ']', '{', '}', '_', '«', '»']
            for char in special_chars:
                display_name = display_name.replace(char, ' ')
                
            #for Aruba, Curacao
            # display_name = display_name.replace(', Netherlands', ' ')
            # display_name = display_name.replace(', 0000 NA', ' ')
            
            #for Cape Verde -> Cabo Verde
            # display_name = display_name.replace(', Cape Verde', ', Cabo Verde')
            
            # for Palestinian Territories -> Palestinian Territory
            # display_name = display_name.replace('Palestinian Territories', 'Palestinian Territory')
            
            # for congo
            # display_name = display_name.replace('Congo-Brazzaville', 'Republic of the Congo')

            # for Netherlands
            # display_name = display_name.replace('Netherlands', 'Republic of the Congo')
            # Clean up multiple spaces
            
            #for timor
            display_name = display_name.replace('East Timor', 'Timor Leste')
            
            display_name = ' '.join(display_name.split())
            
            # Validate address format
            if not looks_like_address(display_name):
                # print("\n looks")
                continue
            
            # Validate region - use country from Nominatim result
            nominatim_country = result.get('address', {}).get('country', country_name)
            # nominatim_country = "Cabo Verde"
            # nominatim_country = "Palestinian Territory"
            # nominatim_country = "The Netherlands"
            # nominatim_country = "Timor Leste"
            
            if country_name.lower() in ["luhansk", "crimea", "donetsk"]
                nominatim_country = country_name
        
            if not validate_address_region(display_name, nominatim_country):
                # print(f"\n region {display_name} {nominatim_country} \n")
                continue
            
            # Check place_rank - only save if > 20
            place_rank = result.get('place_rank', 0)
            if place_rank <= 20:
                continue
            
            # Extract address components
            components = self.extract_address_components(result)
            
            # Calculate score from bounding box
            bbox = result.get('boundingbox', [])
            score = self.calculate_score(bbox) if len(bbox) == 4 else 0.3
            
            if score < 0.9:
                # print("\n score")
                continue
            
            # Save address
            address_data = {
                'osm_id': osm_id,
                # 'country': components['country'],
                'country': nominatim_country,
                'city': components['city'],
                'street': components['street'],
                'score': score,
                'status': 0,
                'address': display_name  # Add this field to satisfy the existing index
            }
            
            self.save_address(address_data)
            stats['saved'] += 1
            
            # Rate limiting - be nice to Nominatim
            time.sleep(1)
        
        # Final progress update
        print(f"\r📊 Progress: 100.0% | Processed: {len(osm_ids)}/{len(osm_ids)} | Saved: {stats['saved']}", flush=True)
        
        # Update status to 'checked' when finished
        self.update_batch_status(batch_id, 'checked')
        
        return stats
    
    def process_country(self, country_name: str, limit: int):
        """Process all batches for a country"""
        # Get batches
        batches = self.get_address_batches(country_name, limit)
        if not batches:
            return
        
        total_saved = 0
        
        for i, batch in enumerate(batches, 1):
            try:
                stats = self.process_batch(batch, country_name)
                total_saved += stats['saved']
            except Exception as e:
                logger.error(f"Error processing batch {batch['_id']}: {e}")
        
        print(f"Complete! Addresses saved: {total_saved}")
    
    def close(self):
        """Close database connection"""
        self.client.close()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        # print("Usage: python address_validator.py <country_name> <limit>")
        # print("Example: python address_validator.py Yemen 10")
        sys.exit(1)
    
    country_name = sys.argv[1]
    try:
        limit = int(sys.argv[2])
    except ValueError:
        print("Error: limit must be a number")
        sys.exit(1)
    
    validator = AddressValidator()
    
    try:
        validator.process_country(country_name, limit)
    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)
    finally:
        validator.close()