import osmnx as ox
import pandas as pd
import time

# Path to the TSV file containing coordinates
tsv_file = "../downloads/Marina Nieto-Caballero air study proposed local scale - merged and curated (1).tsv"

output_filename = "../local/osm_all_features_results.tsv"

# Threshold for flagging uncommon features (e.g., less than 20% of samples)
uncommon_threshold = 0.2

# Read the TSV file
df = pd.read_csv(tsv_file, sep='\t')

# Function to fetch all features from OSM with refined search
def fetch_osm_features(lat, lon, dist=1000):  # Reduced search distance to 1,000 meters
    # Define expanded tag filters to include natural and man-made features
    tags = {
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
        'wetland': True,
    }

    # Use bounding box search instead of circular distance
    west, south, east, north = ox.utils_geo.bbox_from_point((lat, lon), dist=dist)
    features = ox.features_from_bbox((west, south, east, north), tags)
    return features

# Prepare a list to collect results
results = []

# Iterate over each coordinate and fetch OSM features
for idx, row in df.iterrows():
    lat, lon = row['lat val'], row['lon val']
    sample_name = row['sample name']

    print(f"Processing {idx + 1}/{len(df)}: Sample '{sample_name}' at ({lat}, {lon})")

    try:
        # Fetch features from OSM
        features = fetch_osm_features(lat, lon)

        if features.empty:
            print(f"No features found for Sample '{sample_name}' at ({lat}, {lon})")
            results.append({
                'Sample Name': sample_name,
                'Latitude': lat,
                'Longitude': lon,
                'Feature Info': 'No features found'
            })
        else:
            # Store relevant details in results without geometry info in Feature Info
            for _, feature in features.iterrows():
                # Exclude geometry data from feature info
                feature_info = ", ".join(f"{k}: {v}" for k, v in feature.items() if pd.notnull(v) and k != 'geometry')
                results.append({
                    'Sample Name': sample_name,
                    'Latitude': lat,
                    'Longitude': lon,
                    'Feature Info': feature_info
                })
    except Exception as e:
        print(f"Error fetching data for Sample '{sample_name}': {e}")
        # Explicitly record the error in the output
        results.append({
            'Sample Name': sample_name,
            'Latitude': lat,
            'Longitude': lon,
            'Feature Info': f"Error fetching data: {e}"
        })

    # Add delay to respect OSM API rate limits
    time.sleep(1)  # 1-second delay between requests

# Convert results to DataFrame
results_df = pd.DataFrame(results)

# Identify and flag uncommon features across samples
feature_counts = results_df['Feature Info'].value_counts()
total_samples = len(df)

# Apply flagging based on threshold
results_df['Is Uncommon Feature'] = results_df['Feature Info'].apply(
    lambda x: 'Yes' if feature_counts[x] / total_samples < uncommon_threshold else 'No'
)

# Save the results to a TSV file
results_df.to_csv(output_filename, sep='\t', index=False)

print(f"OSM feature extraction completed. Results saved to '{output_filename}'.")
