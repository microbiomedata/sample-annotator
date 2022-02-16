import logging

import click
import click_log

# from typing import List, Optional, Dict, Any

# import pandas as pd
#
# import yaml
#
# from linkml_runtime.utils.schemaview import SchemaView
#
# import pprint

logger = logging.getLogger(__name__)
click_log.basic_config(logger)


@click.command()
@click_log.simple_verbosity_option(logger)
@click.option("--sqlite_path", type=click.Path(exists=True), required=True)
def cli(sqlite_path: str):
    """
    :param sqlite_path:
    :return:
    """

    logger.info(sqlite_path)


if __name__ == '__main__':
    cli()
