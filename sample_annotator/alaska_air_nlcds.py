import pandas as pd
import rasterio
from pyproj import Transformer

# NLCD Land Cover Classification Dictionary
NLCD_CLASSES = {
    11: "Open Water",
    12: "Perennial Ice/Snow",
    21: "Developed, Open Space",
    22: "Developed, Low Intensity",
    23: "Developed, Medium Intensity",
    24: "Developed, High Intensity",
    31: "Barren Land",
    41: "Deciduous Forest",
    42: "Evergreen Forest",
    43: "Mixed Forest",
    51: "Dwarf Scrub",
    52: "Shrub/Scrub",
    71: "Grassland/Herbaceous",
    72: "Sedge/Herbaceous",
    73: "Lichens",
    74: "Moss",
    81: "Pasture/Hay",
    82: "Cultivated Crops",
    90: "Woody Wetlands",
    95: "Emergent Herbaceous Wetlands"
}
# Path to the NLCD .img file
nlcd_file = "../downloads/NLCD_2016_Land_Cover_AK_20200724.img"

# Path to the TSV file containing coordinates
tsv_file = "../downloads/Marina Nieto-Caballero air study proposed local scale - merged and curated (1).tsv"

output_filename = "../local/nlcd_land_cover_results.tsv"

# Read the TSV file
df = pd.read_csv(tsv_file, sep='\t')

# Open the NLCD .img file
with rasterio.open(nlcd_file) as src:
    # Initialize coordinate transformer
    transformer = Transformer.from_crs("EPSG:4326", src.crs, always_xy=True)

    # Prepare lists to collect results
    sample_names = []
    nlcd_codes = []
    nlcd_descriptions = []

    # Iterate over the DataFrame rows
    for _, row in df.iterrows():
        lon, lat = row['lon val'], row['lat val']
        sample_name = row['sample name']

        # Transform coordinates to raster CRS
        transformed_coord = transformer.transform(lon, lat)

        # Extract NLCD land cover class
        land_cover_value = [val for val in src.sample([transformed_coord])][0][0]
        land_cover_description = NLCD_CLASSES.get(land_cover_value, "Unknown Class")

        # Store the results
        sample_names.append(sample_name)
        nlcd_codes.append(land_cover_value)
        nlcd_descriptions.append(land_cover_description)

# Create a results DataFrame
results_df = pd.DataFrame({
    'Sample Name': sample_names,
    'NLCD Code': nlcd_codes,
    'NLCD Description': nlcd_descriptions
})

# Save results to a new TSV file
results_df.to_csv(output_filename, sep='\t', index=False)

print(f"Land cover classification completed. Results saved to '{output_filename}'.")
