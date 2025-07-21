#!/usr/bin/env python3
"""
Brussels venues data wrangler and cleaner
Preprocesses cultural venues and sports facilities data before database import
"""

import pandas as pd
import numpy as np
import re
import logging
from pathlib import Path
from typing import Tuple, Optional, Dict, Any
from urllib.parse import parse_qs, urlparse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class VenueDataWrangler:
    def __init__(self, data_dir: Path):
        self.data_dir = data_dir
        self.venue_csv = data_dir / 'venues' / 'cultural_venues.csv'
        self.sports_csv = data_dir / 'venues' / 'sports_facilities.csv'
        
    def extract_coordinates_from_google_maps(self, url: str) -> Tuple[Optional[float], Optional[float]]:
        """Extract latitude and longitude from Google Maps URLs"""
        if pd.isna(url) or not url:
            return None, None
            
        try:
            # Pattern for Google Maps URLs with query parameter
            # https://www.google.com/maps/search/?api=1&query=50.85294373%2C4.35821104
            if 'query=' in url:
                parsed = urlparse(url)
                query_params = parse_qs(parsed.query)
                if 'query' in query_params:
                    coords = query_params['query'][0]
                    # Handle URL encoded coordinates
                    coords = coords.replace('%2C', ',')
                    if ',' in coords:
                        lat_str, lon_str = coords.split(',')
                        return float(lat_str.strip()), float(lon_str.strip())
            
            # Pattern for direct coordinate URLs
            # Look for lat,lon pattern in URL
            coord_pattern = r'([+-]?\d+\.?\d*),([+-]?\d+\.?\d*)'
            match = re.search(coord_pattern, url)
            if match:
                lat, lon = match.groups()
                return float(lat), float(lon)
                
        except (ValueError, IndexError, AttributeError) as e:
            logger.debug(f"Could not extract coordinates from URL: {url}, error: {e}")
            
        return None, None
    
    def normalize_pmr_accessibility(self, value: str) -> Optional[bool]:
        """Convert PMR accessibility flags to boolean"""
        if pd.isna(value) or not value:
            return None
            
        value = str(value).strip().lower()
        
        # Positive indicators
        if any(indicator in value for indicator in ['accessible pmr', 'accessible', 'oui', 'yes', 'true']):
            return True
        
        # Negative indicators  
        if any(indicator in value for indicator in ['pas accessible', 'non accessible', 'non', 'no', 'false', '?']):
            return False
            
        # Unknown/unclear indicators
        if value in ['?', 'unknown', 'unclear', '']:
            return None
            
        return None
    
    def clean_capacity(self, capacity: Any) -> int:
        """Clean and normalize capacity values"""
        if pd.isna(capacity):
            return 100  # default capacity
            
        try:
            # Convert to string and clean
            capacity_str = str(capacity).strip()
            
            # Remove any non-numeric characters except decimal points
            capacity_clean = re.sub(r'[^\d.]', '', capacity_str)
            
            if capacity_clean:
                capacity_num = float(capacity_clean)
                # Reasonable bounds for venue capacity
                if capacity_num < 1:
                    return 100
                elif capacity_num > 100000:  # Cap at reasonable maximum
                    return 100000
                return int(capacity_num)
            else:
                return 100
                
        except (ValueError, TypeError):
            return 100
    
    def clean_postal_code(self, postal_code: Any) -> str:
        """Clean and validate postal codes"""
        if pd.isna(postal_code):
            return ''
            
        postal_str = str(postal_code).strip()
        
        # Extract numeric part for Brussels postal codes (should be 4 digits starting with 1)
        numeric_part = re.findall(r'\d+', postal_str)
        if numeric_part:
            code = numeric_part[0]
            # Brussels postal codes are 1000-1299
            if len(code) == 4 and code.startswith('1'):
                return code
                
        return postal_str[:10]  # Fallback to truncated original
    
    def standardize_municipality(self, municipality: str) -> str:
        """Standardize municipality names"""
        if pd.isna(municipality):
            return 'Unknown'
            
        municipality = str(municipality).strip()
        
        # Common variations of Brussels
        municipality_lower = municipality.lower()
        if any(name in municipality_lower for name in ['bruxelles', 'brussels', 'brussel']):
            return 'Bruxelles'
            
        return municipality
    
    def clean_text_field(self, text: Any, max_length: int = 255) -> str:
        """Clean and truncate text fields"""
        if pd.isna(text):
            return ''
            
        text_str = str(text).strip()
        
        # Remove excessive whitespace
        text_str = re.sub(r'\s+', ' ', text_str)
        
        # Remove control characters
        text_str = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', text_str)
        
        # Truncate to max length
        return text_str[:max_length]
    
    def generate_venue_id(self, index: int, venue_type: str, name: str) -> str:
        """Generate standardized venue IDs for missing ones"""
        # Create a short hash from the name for uniqueness
        name_clean = re.sub(r'[^\w\s]', '', str(name).lower())
        name_short = ''.join(name_clean.split())[:10]
        return f"{venue_type}_{index+1:04d}_{name_short}"
    
    def clean_cultural_venues(self) -> pd.DataFrame:
        """Clean and preprocess cultural venues data"""
        logger.info("Cleaning cultural venues data...")
        
        try:
            df = pd.read_csv(self.venue_csv, on_bad_lines='skip')
            logger.info(f"Loaded {len(df)} cultural venues")
            
            # Create a copy for processing
            cleaned_df = df.copy()
            
            # Clean venue_id - generate if missing
            if 'venue_id' in cleaned_df.columns:
                mask = cleaned_df['venue_id'].isna() | (cleaned_df['venue_id'] == '')
                if mask.any():
                    for idx in cleaned_df[mask].index:
                        name = cleaned_df.loc[idx, 'venue_name']
                        cleaned_df.loc[idx, 'venue_id'] = self.generate_venue_id(idx, 'cultural', name)
            
            # Clean venue name - required field
            cleaned_df['venue_name'] = cleaned_df['venue_name'].apply(lambda x: self.clean_text_field(x, 255))
            cleaned_df = cleaned_df[cleaned_df['venue_name'] != '']  # Remove entries without names
            
            # Clean category
            if 'category' in cleaned_df.columns:
                cleaned_df['category'] = cleaned_df['category'].apply(lambda x: self.clean_text_field(x, 100))
            
            # Clean address
            if 'address_line1' in cleaned_df.columns:
                cleaned_df['address_line1'] = cleaned_df['address_line1'].apply(lambda x: self.clean_text_field(x, 255))
            
            # Clean postal code
            if 'postal_code' in cleaned_df.columns:
                cleaned_df['postal_code'] = cleaned_df['postal_code'].apply(self.clean_postal_code)
            
            # Standardize municipality
            if 'municipality' in cleaned_df.columns:
                cleaned_df['municipality'] = cleaned_df['municipality'].apply(self.standardize_municipality)
            
            # Convert PMR accessibility to boolean
            if 'pmr_accessible' in cleaned_df.columns:
                cleaned_df['pmr_accessible_bool'] = cleaned_df['pmr_accessible'].apply(self.normalize_pmr_accessibility)
                # Keep original as text for reference
                cleaned_df['pmr_accessible'] = cleaned_df['pmr_accessible'].apply(lambda x: self.clean_text_field(x, 50))
            
            # Extract coordinates from Google Maps URLs if lat/lon are missing or invalid
            if 'google_maps_url' in cleaned_df.columns:
                logger.info("Extracting coordinates from Google Maps URLs...")
                
                coords_extracted = cleaned_df['google_maps_url'].apply(self.extract_coordinates_from_google_maps)
                extracted_lats = [coord[0] for coord in coords_extracted]
                extracted_lons = [coord[1] for coord in coords_extracted]
                
                # Use extracted coordinates if original ones are missing or invalid
                if 'latitude' in cleaned_df.columns and 'longitude' in cleaned_df.columns:
                    # Fill missing coordinates
                    lat_mask = cleaned_df['latitude'].isna() | (cleaned_df['latitude'] == 0)
                    lon_mask = cleaned_df['longitude'].isna() | (cleaned_df['longitude'] == 0)
                    
                    cleaned_df.loc[lat_mask, 'latitude'] = pd.Series(extracted_lats, index=cleaned_df.index)[lat_mask]
                    cleaned_df.loc[lon_mask, 'longitude'] = pd.Series(extracted_lons, index=cleaned_df.index)[lon_mask]
                else:
                    # Add coordinate columns
                    cleaned_df['latitude'] = extracted_lats
                    cleaned_df['longitude'] = extracted_lons
                
                # Validate coordinate ranges (Brussels area roughly)
                lat_valid = (cleaned_df['latitude'] >= 50.7) & (cleaned_df['latitude'] <= 51.0)
                lon_valid = (cleaned_df['longitude'] >= 4.2) & (cleaned_df['longitude'] <= 4.6)
                coord_valid = lat_valid & lon_valid
                
                invalid_coords = (~coord_valid) & (~cleaned_df['latitude'].isna())
                if invalid_coords.any():
                    logger.warning(f"Found {invalid_coords.sum()} venues with coordinates outside Brussels area")
            
            # Clean capacity
            if 'capacity' in cleaned_df.columns:
                cleaned_df['capacity'] = cleaned_df['capacity'].apply(self.clean_capacity)
            
            # Clean timestamps
            timestamp_cols = ['published_at', 'updated_at']
            for col in timestamp_cols:
                if col in cleaned_df.columns:
                    cleaned_df[col] = pd.to_datetime(cleaned_df[col], errors='coerce')
            
            logger.info(f"Cleaned cultural venues: {len(cleaned_df)} venues remaining")
            return cleaned_df
            
        except Exception as e:
            logger.error(f"Error cleaning cultural venues: {e}")
            raise
    
    def clean_sports_facilities(self) -> pd.DataFrame:
        """Clean and preprocess sports facilities data"""
        logger.info("Cleaning sports facilities data...")
        
        try:
            df = pd.read_csv(self.sports_csv, on_bad_lines='skip')
            logger.info(f"Loaded {len(df)} sports facilities")
            
            # Create a copy for processing
            cleaned_df = df.copy()
            
            # Generate facility_id for missing ones
            if 'facility_id' in cleaned_df.columns:
                mask = cleaned_df['facility_id'].isna() | (cleaned_df['facility_id'] == '')
                if mask.any():
                    for idx in cleaned_df[mask].index:
                        name = cleaned_df.loc[idx, 'facility_name']
                        cleaned_df.loc[idx, 'facility_id'] = self.generate_venue_id(idx, 'sports', name)
            else:
                # Create facility_id column if it doesn't exist
                cleaned_df['facility_id'] = [self.generate_venue_id(idx, 'sports', row['facility_name']) 
                                           for idx, row in cleaned_df.iterrows()]
            
            # Clean facility name - required field
            cleaned_df['facility_name'] = cleaned_df['facility_name'].apply(lambda x: self.clean_text_field(x, 255))
            cleaned_df = cleaned_df[cleaned_df['facility_name'] != '']  # Remove entries without names
            
            # Clean type/category
            if 'type' in cleaned_df.columns:
                cleaned_df['type'] = cleaned_df['type'].apply(lambda x: self.clean_text_field(x, 100))
            
            # Clean address
            if 'address' in cleaned_df.columns:
                cleaned_df['address'] = cleaned_df['address'].apply(lambda x: self.clean_text_field(x, 255))
            
            # Clean postal code
            if 'postal_code' in cleaned_df.columns:
                cleaned_df['postal_code'] = cleaned_df['postal_code'].apply(self.clean_postal_code)
            
            # Standardize municipality
            if 'municipality' in cleaned_df.columns:
                cleaned_df['municipality'] = cleaned_df['municipality'].apply(self.standardize_municipality)
            
            # Clean contact fields
            contact_fields = ['phone', 'email', 'website', 'management']
            for field in contact_fields:
                if field in cleaned_df.columns:
                    max_len = 20 if field == 'phone' else 100 if field in ['email', 'management'] else 255
                    cleaned_df[field] = cleaned_df[field].apply(lambda x: self.clean_text_field(x, max_len))
            
            # Extract coordinates from Google Maps URLs
            if 'google_maps_url' in cleaned_df.columns:
                logger.info("Extracting coordinates from Google Maps URLs...")
                
                coords_extracted = cleaned_df['google_maps_url'].apply(self.extract_coordinates_from_google_maps)
                extracted_lats = [coord[0] for coord in coords_extracted]
                extracted_lons = [coord[1] for coord in coords_extracted]
                
                # Use extracted coordinates if original ones are missing or invalid
                if 'latitude' in cleaned_df.columns and 'longitude' in cleaned_df.columns:
                    # Fill missing coordinates
                    lat_mask = cleaned_df['latitude'].isna() | (cleaned_df['latitude'] == 0)
                    lon_mask = cleaned_df['longitude'].isna() | (cleaned_df['longitude'] == 0)
                    
                    cleaned_df.loc[lat_mask, 'latitude'] = pd.Series(extracted_lats, index=cleaned_df.index)[lat_mask]
                    cleaned_df.loc[lon_mask, 'longitude'] = pd.Series(extracted_lons, index=cleaned_df.index)[lon_mask]
                else:
                    # Add coordinate columns
                    cleaned_df['latitude'] = extracted_lats
                    cleaned_df['longitude'] = extracted_lons
            
            # Clean capacity
            if 'capacity' in cleaned_df.columns:
                cleaned_df['capacity'] = cleaned_df['capacity'].apply(self.clean_capacity)
            
            # Clean timestamps
            timestamp_cols = ['last_update', 'updated_at']
            for col in timestamp_cols:
                if col in cleaned_df.columns:
                    cleaned_df[col] = pd.to_datetime(cleaned_df[col], errors='coerce')
            
            logger.info(f"Cleaned sports facilities: {len(cleaned_df)} facilities remaining")
            return cleaned_df
            
        except Exception as e:
            logger.error(f"Error cleaning sports facilities: {e}")
            raise
    
    def remove_unused_columns(self, cultural_df: pd.DataFrame, sports_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Remove unused columns to optimize database schema"""
        
        # Essential columns for cultural venues (based on SQL query analysis)
        cultural_essential = [
            'venue_id',           # Primary key
            'venue_name',         # Required for display
            'category',           # Used for venue categorization in queries
            'capacity',           # Used in accessibility scoring
            'municipality',       # Used for grouping
            'latitude',           # Essential for distance calculations
            'longitude',          # Essential for distance calculations
            'pmr_accessible_bool' # Useful accessibility info
        ]
        
        # Essential columns for sports facilities
        sports_essential = [
            'facility_id',        # Primary key
            'facility_name',      # Required for display
            'type',               # Used for categorization (subcategory in all_venues view)
            'capacity',           # Used in accessibility scoring
            'municipality',       # Used for grouping
            'latitude',           # Essential for distance calculations
            'longitude'           # Essential for distance calculations
        ]
        
        # Filter to essential columns only
        cultural_clean = cultural_df[cultural_essential].copy()
        sports_clean = sports_df[sports_essential].copy()
        
        # Log removed columns
        removed_cultural = set(cultural_df.columns) - set(cultural_essential)
        removed_sports = set(sports_df.columns) - set(sports_essential)
        
        if removed_cultural:
            logger.info(f"Removed unused cultural venue columns: {', '.join(sorted(removed_cultural))}")
        if removed_sports:
            logger.info(f"Removed unused sports facility columns: {', '.join(sorted(removed_sports))}")
        
        return cultural_clean, sports_clean
    
    def save_cleaned_data(self, cultural_df: pd.DataFrame, sports_df: pd.DataFrame):
        """Save cleaned data to new CSV files"""
        output_dir = self.data_dir / 'venues' / 'cleaned'
        output_dir.mkdir(exist_ok=True)
        
        # Remove unused columns before saving
        cultural_clean, sports_clean = self.remove_unused_columns(cultural_df, sports_df)
        
        cultural_output = output_dir / 'cultural_venues_cleaned.csv'
        sports_output = output_dir / 'sports_facilities_cleaned.csv'
        
        cultural_clean.to_csv(cultural_output, index=False)
        sports_clean.to_csv(sports_output, index=False)
        
        logger.info(f"Saved cleaned cultural venues to: {cultural_output}")
        logger.info(f"Saved cleaned sports facilities to: {sports_output}")
        
        return cultural_output, sports_output
    
    def generate_data_quality_report(self, cultural_df: pd.DataFrame, sports_df: pd.DataFrame):
        """Generate a data quality report"""
        logger.info("=== DATA QUALITY REPORT ===")
        
        # Cultural venues report
        logger.info("CULTURAL VENUES:")
        logger.info(f"  Total venues: {len(cultural_df)}")
        logger.info(f"  Venues with coordinates: {(~cultural_df['latitude'].isna()).sum()}")
        logger.info(f"  Venues with capacity: {(cultural_df['capacity'] > 0).sum()}")
        if 'pmr_accessible_bool' in cultural_df.columns:
            pmr_true = cultural_df['pmr_accessible_bool'] == True
            pmr_false = cultural_df['pmr_accessible_bool'] == False
            logger.info(f"  PMR accessible: {pmr_true.sum()}")
            logger.info(f"  PMR not accessible: {pmr_false.sum()}")
            logger.info(f"  PMR unknown: {cultural_df['pmr_accessible_bool'].isna().sum()}")
        
        if 'category' in cultural_df.columns:
            top_categories = cultural_df['category'].value_counts().head(5)
            logger.info("  Top categories:")
            for cat, count in top_categories.items():
                logger.info(f"    {cat}: {count}")
        
        # Sports facilities report
        logger.info("\nSPORTS FACILITIES:")
        logger.info(f"  Total facilities: {len(sports_df)}")
        logger.info(f"  Facilities with coordinates: {(~sports_df['latitude'].isna()).sum()}")
        logger.info(f"  Facilities with capacity: {(sports_df['capacity'] > 0).sum()}")
        
        if 'type' in sports_df.columns:
            top_types = sports_df['type'].value_counts().head(5)
            logger.info("  Top types:")
            for type_name, count in top_types.items():
                logger.info(f"    {type_name}: {count}")
    
    def run_cleaning_pipeline(self) -> Tuple[Path, Path]:
        """Run the complete data cleaning pipeline"""
        logger.info("Starting venue data cleaning pipeline...")
        
        # Clean both datasets
        cultural_df = self.clean_cultural_venues()
        sports_df = self.clean_sports_facilities()
        
        # Generate quality report
        self.generate_data_quality_report(cultural_df, sports_df)
        
        # Save cleaned data
        cultural_output, sports_output = self.save_cleaned_data(cultural_df, sports_df)
        
        logger.info("Data cleaning pipeline completed successfully!")
        return cultural_output, sports_output

def main():
    """Main entry point for the data wrangler"""
    script_dir = Path(__file__).parent
    project_root = script_dir.parent.parent
    data_dir = project_root / "src" / "data" / "additional_datasets"
    
    wrangler = VenueDataWrangler(data_dir)
    cultural_output, sports_output = wrangler.run_cleaning_pipeline()
    
    print(f"✅ Data cleaning completed!")
    print(f"📁 Cleaned cultural venues: {cultural_output}")
    print(f"📁 Cleaned sports facilities: {sports_output}")

if __name__ == "__main__":
    main()