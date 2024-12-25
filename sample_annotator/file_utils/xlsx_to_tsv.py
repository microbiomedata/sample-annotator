import sys
from typing import Optional

import click
import pandas as pd


@click.command()
@click.option("--excel-file", "-e", required=True, type=click.Path(exists=True), help="Path to the Excel file (.xlsx)")
@click.option("--sheet-name", "-s", required=True, help="Name of the sheet to extract")
@click.option("--output-file", "-o", default=None, help="Output TSV file name (defaults to sheet name)")
def xlsx_to_tsv(excel_file: str, sheet_name: str, output_file: Optional[str] = None):
    """
    A simple Click CLI application to extract a sheet from an Excel file and save it as a CSV.
    """
    try:
        df = pd.read_excel(excel_file, sheet_name=sheet_name)

        if output_file is None:
            output_file = f"{sheet_name}.csv"

        df.to_csv(output_file, index=False, sep="\t")
        click.echo(f"Sheet '{sheet_name}' extracted to '{output_file}'")

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


if __name__ == "__main__":
    xlsx_to_tsv()
