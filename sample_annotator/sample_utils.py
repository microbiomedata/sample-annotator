import click
import yaml
import json
from typing import Optional, List, Set, Any

from .sample_annotator import SampleAnnotator
from .report_model import SAMPLE

KEY_RV = 'has_raw_value'


def create_tests(samples: List[SAMPLE]):
    """
    Takes normalized samples and uses this to create tests
    """
    test_samples = samples.copy()
    for s in test_samples:
        for k, v in s.copy().items():
            if KEY_RV in v:
                s[k] = v[KEY_RV]
    return test_samples


@click.group()
@click.option('-v', '--verbose', count=True)
@click.option('-q', '--quiet')
def main(verbose: int, quiet: bool):
    None


@main.command()
@click.argument('input')
def mktests(input: str):
    with open(input) as stream:
        samples = yaml.safe_load(stream)
    samples = create_tests(samples)
    print(json.dumps(samples, indent=4, sort_keys=True))


if __name__ == "__main__":
    main()
