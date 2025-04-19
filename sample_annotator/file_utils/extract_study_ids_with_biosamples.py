#!/usr/bin/env python3
import sys
import warnings
from typing import Optional, Set

import click
import pandas as pd

warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl.styles.stylesheet")


def extract_study_ids_with_biosamples(excel_file: str, sheet_name: str = "Sequencing Project") -> Set[str]:
    """
    Extract Study GOLD IDs from the Sequencing Project sheet, filtering for rows that have Biosample GOLD IDs.

    Args:
        excel_file: Path to the GOLD data Excel file
        sheet_name: Name of the sheet to extract from (default: "Sequencing Project")

    Returns:
        Set of unique Study GOLD IDs that have associated Biosample GOLD IDs
    """
    try:
        # Read the Sequencing Project sheet
        df = pd.read_excel(excel_file, sheet_name=sheet_name)

        # Identify column names
        column_map = {}
        for col in df.columns:
            col_upper = col.upper().replace(" ", "_")
            if "STUDY_GOLD_ID" in col_upper:
                column_map["study_id"] = col
            elif "BIOSAMPLE_GOLD_ID" in col_upper:
                column_map["biosample_id"] = col
            elif "PROJECT_GOLD_ID" in col_upper:
                column_map["project_id"] = col

        if "study_id" not in column_map or "biosample_id" not in column_map:
            raise ValueError(f"Required columns not found in {sheet_name} sheet")

        # Print summary statistics
        print(f"# Rows in sheet: {len(df)}", file=sys.stderr)
        if "project_id" in column_map:
            print(f"# Unique Project IDs: {df[column_map['project_id']].nunique(dropna=True)}", file=sys.stderr)
        print(f"# Unique Study IDs: {df[column_map['study_id']].nunique(dropna=True)}", file=sys.stderr)
        print(f"# Unique Biosample IDs: {df[column_map['biosample_id']].nunique(dropna=True)}", file=sys.stderr)

        # Filter for rows with Biosample IDs
        mask = df[column_map["biosample_id"]].notna() & df[column_map["biosample_id"]].astype(str).str.len() > 0
        filtered_df = df[mask]

        # Extract valid Study IDs (must start with 'Gs')
        study_ids = set(filtered_df[column_map["study_id"]].unique())
        valid_study_ids = {str(study_id) for study_id in study_ids if str(study_id).startswith('Gs')}

        print(f"# Unique Study IDs linked to Biosamples: {len(valid_study_ids)}", file=sys.stderr)

        return valid_study_ids

    except Exception as e:
        print(f"Error processing Excel file: {e}", file=sys.stderr)
        return set()


@click.command()
@click.option("--excel-file", "-e", required=True, type=click.Path(exists=True),
              help="Path to the GOLD data Excel file (.xlsx)")
@click.option("--sheet-name", "-s", default="Sequencing Project",
              help="Name of the sheet containing sequencing project data")
@click.option("--output-file", "-o", default=None, type=click.Path(),
              help="Output file for study IDs (defaults to stdout)")
def main(excel_file: str, sheet_name: str, output_file: Optional[str]):
    """
    Extract Study GOLD IDs from rows in the Sequencing Project sheet that have Biosample GOLD IDs.
    """
    study_ids = extract_study_ids_with_biosamples(excel_file, sheet_name)

    if not study_ids:
        click.echo(f"No Study GOLD IDs with Biosample IDs found in {sheet_name} sheet", err=True)
        sys.exit(1)

    sorted_ids = sorted(study_ids)

    if output_file:
        with open(output_file, 'w') as f:
            for study_id in sorted_ids:
                f.write(f"{study_id}\n")
        click.echo(f"Extracted {len(sorted_ids)} Study GOLD IDs to '{output_file}'")
    else:
        for study_id in sorted_ids:
            click.echo(study_id)
        click.echo(f"Extracted {len(sorted_ids)} Study GOLD IDs", err=True)


if __name__ == "__main__":
    main()
