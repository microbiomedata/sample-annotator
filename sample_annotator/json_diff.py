import json
from pathlib import Path

import click
from deepdiff import DeepDiff


def load_json(path: Path):
    with path.open() as f:
        return json.load(f)


def normalize(obj):
    """
    Recursively normalize JSON-like object:
    - Sort dict keys
    - Sort lists if homogeneous and sortable
    """
    if isinstance(obj, dict):
        return {k: normalize(obj[k]) for k in sorted(obj)}
    elif isinstance(obj, list):
        if all(isinstance(el, dict) for el in obj):
            return sorted((normalize(el) for el in obj), key=lambda x: json.dumps(x, sort_keys=True))
        try:
            return sorted(normalize(el) for el in obj)
        except TypeError:
            return [normalize(el) for el in obj]
    else:
        return obj


@click.command()
@click.option("--file1", type=click.Path(exists=True, path_type=Path), required=True,
              help="First JSON file to compare.")
@click.option("--file2", type=click.Path(exists=True, path_type=Path), required=True,
              help="Second JSON file to compare.")
@click.option("--show-diff", is_flag=True, help="Print the DeepDiff if files differ.")
def compare(file1: Path, file2: Path, show_diff: bool):
    """Compare two JSON files for semantic equivalence."""
    data1 = load_json(file1)
    data2 = load_json(file2)

    normalized1 = normalize(data1)
    normalized2 = normalize(data2)

    if normalized1 == normalized2:
        click.echo("✅ Files are semantically equivalent.")
    else:
        click.echo("❌ Files differ.")
        if show_diff:
            diff = DeepDiff(normalized1, normalized2, verbose_level=2)
            click.echo(diff.pretty())


if __name__ == "__main__":
    compare()
