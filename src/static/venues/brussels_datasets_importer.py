import requests
import pandas as pd
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BrusselsDatasetImporter:
    def __init__(self):
        self.base_dir = Path("../data/additional_datasets")
        self.setup_directories()
    
    def estimate_venue_capacity(self, category: str, facility_type: str) -> int:
        """Estimate venue capacity based on category and type"""
        category_lower = category.lower() if category else ""
        type_lower = facility_type.lower() if facility_type else ""
        
        # Cultural venues
        if any(keyword in category_lower for keyword in ['musée', 'museum', 'galerie', 'gallery']):
            return 200  # Typical museum/gallery capacity
        elif any(keyword in category_lower for keyword in ['théâtre', 'theater', 'theatre']):
            return 400  # Theater capacity
        elif any(keyword in category_lower for keyword in ['cinéma', 'cinema']):
            return 150  # Cinema capacity
        elif any(keyword in category_lower for keyword in ['concert', 'music', 'salle']):
            return 800  # Concert hall
        elif any(keyword in category_lower for keyword in ['bar', 'pub', 'café']):
            return 80   # Bar/pub capacity
        elif any(keyword in category_lower for keyword in ['club', 'nightlife']):
            return 300  # Nightclub capacity
        elif any(keyword in category_lower for keyword in ['restaurant']):
            return 60   # Restaurant capacity
        elif any(keyword in category_lower for keyword in ['église', 'church', 'cathedral']):
            return 500  # Church capacity
        
        # Sports facilities
        elif any(keyword in type_lower for keyword in ['stadium']):
            return 30000  # Sports field/stadium
        elif any(keyword in type_lower for keyword in ['terrain de sport', 'sportterrein']):
            return 500
        elif any(keyword in type_lower for keyword in ['piscine', 'zwembad', 'pool']):
            return 200   # Swimming pool
        elif any(keyword in type_lower for keyword in ['salle de sport', 'sportzaal', 'gym']):
            return 100   # Gym/sports hall
        elif any(keyword in type_lower for keyword in ['tennis']):
            return 50    # Tennis court
        
        return 100  # Default capacity
        
    def setup_directories(self):
        """Create necessary directories for additional dataset storage"""
        self.base_dir.mkdir(parents=True, exist_ok=True)
        
        # Create subdirectories for different data types
        for subdir in ['population', 'schools', 'venues', 'transport', 'offices']:
            (self.base_dir / subdir).mkdir(exist_ok=True)
    
    def download_cultural_venues(self) -> Optional[pd.DataFrame]:
        """Download cultural venues, tourist sites, and event locations"""
        try:
            logger.info("Downloading cultural venues dataset...")
            url = "https://opendata.brussels.be/explore/dataset/lieux_culturels_touristiques_evenementiels_visitbrussels_vbx/download"
            
            # Download as JSON to get better field access
            params = {"format": "json", "rows": 1000}
            response = requests.get(url, params=params)
            response.raise_for_status()
            
            # Parse JSON
            data = response.json()
            
            # Convert to DataFrame
            venues = []
            for record in data:
                fields = record.get('fields', {})
                geom = record.get('geometry', {})
                coords = geom.get('coordinates', [None, None]) if geom else [None, None]
                
                # Extract name (prefer French, fallback to Dutch/English)
                venue_name = (fields.get('translations_fr_name', '') or 
                            fields.get('translations_nl_name', '') or 
                            fields.get('translations_en_name', '') or 
                            'Unknown Venue')
                
                # Extract address
                address_line1 = (fields.get('translations_fr_address_line1', '') or 
                               fields.get('translations_nl_address_line1', ''))
                postal_code = fields.get('translations_fr_address_zip', '')
                municipality = (fields.get('add_municipality_fr', '') or 
                              fields.get('add_municipality_nl', ''))
                
                # Extract category
                category = (fields.get('visit_category_fr_multi', '') or 
                          fields.get('visit_category_nl_multi', '') or 
                          fields.get('visit_category_en_multi', ''))
                
                venues.append({
                    'venue_id': fields.get('id', ''),
                    'venue_name': venue_name,
                    'category': category,
                    'address_line1': address_line1,
                    'postal_code': postal_code,
                    'municipality': municipality,
                    'pmr_accessible': fields.get('pmr_fr', ''),
                    'google_maps_url': fields.get('google_maps', ''),
                    'published_at': fields.get('published_at', ''),
                    'capacity': self.estimate_venue_capacity(category, ''),
                    'longitude': coords[0] if coords and coords[0] else None,
                    'latitude': coords[1] if coords and coords[1] else None,
                    'updated_at': datetime.now().isoformat()
                })
            
            df = pd.DataFrame(venues)
            
            # Save to CSV and Parquet
            csv_path = self.base_dir / 'venues' / 'cultural_venues.csv'
            parquet_path = self.base_dir / 'venues' / 'cultural_venues.parquet'
            
            df.to_csv(csv_path, index=False)
            df.to_parquet(parquet_path, index=False)
            
            logger.info(f"Downloaded {len(df)} cultural venues")
            return df
            
        except Exception as e:
            logger.error(f"Error downloading cultural venues: {e}")
            return None
    
    def download_villo_stations(self) -> Optional[pd.DataFrame]:
        """Download Villo bike sharing stations"""
        try:
            logger.info("Downloading Villo bike sharing stations...")
            url = "https://data.opendatasoft.com/api/explore/v2.1/catalog/datasets/stations-villo-bruxelles-rbc@bruxellesdata/records"
            
            params = {
                "limit": 1000,
                "offset": 0
            }
            
            response = requests.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            results = data.get('results', [])
            
            stations = []
            for record in results:
                fields = record.get('record', {}).get('fields', {})
                geom = record.get('record', {}).get('geometry', {})
                coords = geom.get('coordinates', [None, None])
                
                stations.append({
                    'station_id': fields.get('id', ''),
                    'station_name': fields.get('name', ''),
                    'status': fields.get('status', ''),
                    'available_bikes': fields.get('available_bikes', 0),
                    'available_docks': fields.get('available_docks', 0),
                    'total_docks': fields.get('bike_stands', 0),
                    'longitude': coords[0] if coords[0] else None,
                    'latitude': coords[1] if coords[1] else None,
                    'updated_at': datetime.now().isoformat()
                })
            
            df = pd.DataFrame(stations)
            
            # Save to CSV and Parquet
            csv_path = self.base_dir / 'transport' / 'villo_stations.csv'
            parquet_path = self.base_dir / 'transport' / 'villo_stations.parquet'
            
            df.to_csv(csv_path, index=False)
            df.to_parquet(parquet_path, index=False)
            
            logger.info(f"Downloaded {len(df)} Villo stations")
            return df
            
        except Exception as e:
            logger.error(f"Error downloading Villo stations: {e}")
            return None
    
    def download_sports_facilities(self) -> Optional[pd.DataFrame]:
        """Download sports halls and stadiums"""
        try:
            logger.info("Downloading sports facilities...")
            url = "https://opendata.brussels.be/explore/dataset/infrastructures-sportives-gerees-par-la-ville-de-bruxelles/download"
            
            params = {"format": "json", "rows": 100}
            response = requests.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            
            facilities = []
            for record in data:
                fields = record.get('fields', {})
                geom = record.get('geometry', {})
                coords = geom.get('coordinates', [None, None]) if geom else [None, None]
                
                # Extract name (prefer French, fallback to Dutch)
                facility_name = (fields.get('name_fr', '') or 
                               fields.get('name_nl', '') or 
                               'Unknown Facility')
                
                # Extract address
                address = (fields.get('address_fr', '') or 
                          fields.get('address_nl', ''))
                
                # Extract facility type
                facility_type = (fields.get('data_fr', '') or 
                               fields.get('data_nl', ''))
                
                facilities.append({
                    'facility_id': fields.get('recordid', '')[:20],  # Truncate long ID
                    'facility_name': facility_name,
                    'type': facility_type,
                    'address': address,
                    'postal_code': fields.get('postalcode', ''),
                    'municipality': (fields.get('municipality_fr', '') or fields.get('municipality_nl', '')),
                    'phone': fields.get('phone', ''),
                    'email': fields.get('email', ''),
                    'website': (fields.get('url_fr', '') or fields.get('url_nl', '')),
                    'management': (fields.get('management_fr', '') or fields.get('management_nl', '')),
                    'google_maps_url': fields.get('google_maps', ''),
                    'last_update': fields.get('last_update', ''),
                    'capacity': self.estimate_venue_capacity('', facility_type),
                    'longitude': coords[0] if coords and coords[0] else None,
                    'latitude': coords[1] if coords and coords[1] else None,
                    'updated_at': datetime.now().isoformat()
                })
            
            df = pd.DataFrame(facilities)
            
            # Save to CSV and Parquet
            csv_path = self.base_dir / 'venues' / 'sports_facilities.csv'
            parquet_path = self.base_dir / 'venues' / 'sports_facilities.parquet'
            
            df.to_csv(csv_path, index=False)
            df.to_parquet(parquet_path, index=False)
            
            logger.info(f"Downloaded {len(df)} sports facilities")
            return df
            
        except Exception as e:
            logger.error(f"Error downloading sports facilities: {e}")
            return None
    
    def download_population_density(self) -> Optional[pd.DataFrame]:
        """Download population density data from Statbel"""
        try:
            logger.info("Downloading population density data...")
            # Note: This URL might need adjustment based on actual Statbel API
            url = "https://statbel.fgov.be/sites/default/files/files/opendata/deelgemeenten-sectors/TF_POPULATION_2024.xlsx"
            
            # Download the Excel file
            response = requests.get(url)
            response.raise_for_status()
            
            # Save temporarily and read with pandas
            temp_file = self.base_dir / 'population' / 'temp_population.xlsx'
            with open(temp_file, 'wb') as f:
                f.write(response.content)
            
            # Read Excel file
            df = pd.read_excel(temp_file)
            
            # Filter for Brussels region (assuming Brussels postal codes 1000-1210)
            brussels_df = df[df['CD_MUNTY_REFNIS'].str.startswith('21')]  # Brussels region code
            
            # Clean up the data
            population_data = []
            for _, row in brussels_df.iterrows():
                population_data.append({
                    'municipality_code': row.get('CD_MUNTY_REFNIS', ''),
                    'municipality_name': row.get('TX_MUNTY_DESCR_NL', ''),
                    'sector_code': row.get('CD_SECTOR', ''),
                    'total_population': row.get('MS_POPULATION', 0),
                    'density_per_km2': row.get('MS_POPULATION', 0) / max(row.get('MS_AREA_KM2', 1), 0.01),
                    'area_km2': row.get('MS_AREA_KM2', 0),
                    'updated_at': datetime.now().isoformat()
                })
            
            df_clean = pd.DataFrame(population_data)
            
            # Save to CSV and Parquet
            csv_path = self.base_dir / 'population' / 'population_density.csv'
            parquet_path = self.base_dir / 'population' / 'population_density.parquet'
            
            df_clean.to_csv(csv_path, index=False)
            df_clean.to_parquet(parquet_path, index=False)
            
            # Clean up temp file
            temp_file.unlink()
            
            logger.info(f"Downloaded population data for {len(df_clean)} sectors")
            return df_clean
            
        except Exception as e:
            logger.error(f"Error downloading population data: {e}")
            return None
    
    def create_sample_schools_data(self) -> pd.DataFrame:
        """Create sample schools data for Brussels (replace with real API when available)"""
        logger.info("Creating sample schools data...")
        
        # Sample data for Brussels schools (to be replaced with real data)
        sample_schools = [
            {'school_id': 'SCH001', 'school_name': 'Athénée Royal Victor Horta', 'type': 'secondary', 'enrollment': 800, 'latitude': 50.8263, 'longitude': 4.3633},
            {'school_id': 'SCH002', 'school_name': 'Lycée Français Jean Monnet', 'type': 'secondary', 'enrollment': 650, 'latitude': 50.8176, 'longitude': 4.4096},
            {'school_id': 'SCH003', 'school_name': 'International School of Brussels', 'type': 'international', 'enrollment': 1200, 'latitude': 50.8571, 'longitude': 4.4278},
            {'school_id': 'SCH004', 'school_name': 'Vrije Universiteit Brussel', 'type': 'university', 'enrollment': 15000, 'latitude': 50.8198, 'longitude': 4.3942},
            {'school_id': 'SCH005', 'school_name': 'Université Libre de Bruxelles', 'type': 'university', 'enrollment': 25000, 'latitude': 50.8118, 'longitude': 4.3847}
        ]
        
        df = pd.DataFrame(sample_schools)
        df['updated_at'] = datetime.now().isoformat()
        
        # Save to CSV and Parquet
        csv_path = self.base_dir / 'schools' / 'schools.csv'
        parquet_path = self.base_dir / 'schools' / 'schools.parquet'
        
        df.to_csv(csv_path, index=False)
        df.to_parquet(parquet_path, index=False)
        
        logger.info(f"Created {len(df)} sample school records")
        return df
    
    def create_sample_offices_data(self) -> pd.DataFrame:
        """Create sample office buildings data for Brussels (replace with real API when available)"""
        logger.info("Creating sample office buildings data...")
        
        # Sample data for Brussels office buildings
        sample_offices = [
            {'office_id': 'OFF001', 'building_name': 'European Parliament', 'employees': 8000, 'type': 'government', 'latitude': 50.8398, 'longitude': 4.3776},
            {'office_id': 'OFF002', 'building_name': 'European Commission', 'employees': 32000, 'type': 'government', 'latitude': 50.8434, 'longitude': 4.3814},
            {'office_id': 'OFF003', 'building_name': 'NATO Headquarters', 'employees': 4000, 'type': 'international', 'latitude': 50.8798, 'longitude': 4.4253},
            {'office_id': 'OFF004', 'building_name': 'Brussels World Trade Center', 'employees': 3500, 'type': 'commercial', 'latitude': 50.8606, 'longitude': 4.3616},
            {'office_id': 'OFF005', 'building_name': 'Tour Madou', 'employees': 2000, 'type': 'commercial', 'latitude': 50.8448, 'longitude': 4.3713}
        ]
        
        df = pd.DataFrame(sample_offices)
        df['updated_at'] = datetime.now().isoformat()
        
        # Save to CSV and Parquet
        csv_path = self.base_dir / 'offices' / 'offices.csv'
        parquet_path = self.base_dir / 'offices' / 'offices.parquet'
        
        df.to_csv(csv_path, index=False)
        df.to_parquet(parquet_path, index=False)
        
        logger.info(f"Created {len(df)} sample office records")
        return df
    
    def import_venues_only(self):
        """Import only venues and facilities datasets"""
        logger.info("Starting Brussels venues and facilities import")
        
        # Download real venue datasets
        cultural_df = self.download_cultural_venues()
        sports_df = self.download_sports_facilities()
        
        logger.info("Brussels venues and facilities import completed")
        return cultural_df, sports_df

if __name__ == "__main__":
    importer = BrusselsDatasetImporter()
    importer.import_venues_only()