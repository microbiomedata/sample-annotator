import osmnx as ox
import pandas as pd
import time
from typing import List, Dict, Any

# Path to the TSV file containing coordinates
tsv_file: str = "../downloads/Marina Nieto-Caballero air study proposed local scale - merged and curated (1).tsv"

output_filename: str = "../local/osm_all_features_results.tsv"

# Threshold for flagging uncommon features (e.g., less than 20% of samples)
uncommon_threshold: float = 0.2

# Read the TSV file
df: pd.DataFrame = pd.read_csv(tsv_file, sep='\t')


def fetch_osm_features(lat: float, lon: float, dist: int = 1000) -> pd.DataFrame:
    """
    Fetch OpenStreetMap (OSM) features near a specified latitude and longitude.

    Args:
        lat (float): Latitude of the location.
        lon (float): Longitude of the location.
        dist (int): Search radius in meters (default is 1000).

    Returns:
        pd.DataFrame: A DataFrame containing the fetched OSM features.
    """
    tags: Dict[str, bool] = {
        'aeroway': True,
        'amenity': True,
        'barrier': True,
        'boundary': True,
        'building': True,
        'emergency': True,
        'geological': True,
        'highway': True,
        'historic': True,
        'landuse': True,
        'leisure': True,
        'man_made': True,
        'military': True,
        'natural': True,
        'place': True,
        'surface': True,
        'tourism': True,
        'water': True,
        'waterway': True,
        'wetland': True
    }
    features: pd.DataFrame = ox.features_from_point((lat, lon), tags=tags, dist=dist)
    return features


# Prepare a list to collect results
results: List[Dict[str, Any]] = []

# Iterate over each coordinate and fetch OSM features
for idx, row in df.iterrows():
    lat, lon = row['lat val'], row['lon val']
    sample_name = row['sample name']

    print(f"Processing {idx + 1}/{len(df)}: Sample '{sample_name}' at ({lat}, {lon})")

    try:
        features: pd.DataFrame = fetch_osm_features(lat, lon)

        if features.empty:
            print(f"No features found for Sample '{sample_name}' at ({lat}, {lon})")
            results.append({
                'sample_name': sample_name,
                'latitude': lat,
                'longitude': lon,
                'feature_info': 'No features found'
            })
        else:
            for _, feature in features.iterrows():
                feature_info: str = ", ".join(
                    f"{k}: {v}" for k, v in feature.items()
                    if pd.notnull(v) and k != 'geometry' and (
                            k in ['name', 'name:en', 'old_name', 'old_name:en'] or (
                            not k.startswith('name:') and not k.startswith('old_name:')
                    )
                    )
                )
                results.append({
                    'sample_name': sample_name,
                    'latitude': lat,
                    'longitude': lon,
                    'feature_info': feature_info if feature_info else 'No English name found'
                })
    except Exception as e:
        print(f"Error fetching data for Sample '{sample_name}': {e}")
        results.append({
            'sample_name': sample_name,
            'latitude': lat,
            'longitude': lon,
            'feature_info': f"Error fetching data: {e}"
        })

    time.sleep(1)  # 1-second delay between requests

# Convert results to DataFrame
results_df: pd.DataFrame = pd.DataFrame(results)

# Identify and flag uncommon features across samples
feature_counts: pd.Series = results_df['feature_info'].value_counts()
total_samples: int = len(df)

dynamic_column_name: str = f"lt_{int(uncommon_threshold * 100)}_pct_of_samples"
results_df[dynamic_column_name] = results_df['feature_info'].apply(
    lambda x: True if feature_counts[x] / total_samples < uncommon_threshold else False
)

# Save the results to a TSV file
results_df.to_csv(output_filename, sep='\t', index=False)

print(f"OSM feature extraction completed. Results saved to '{output_filename}'.")
