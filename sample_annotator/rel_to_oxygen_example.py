import logging

import click
import click_log
import pandas as pd

import sample_annotator.clients.biosample_sqlite_client as bsq

from typing import List
import re

# import ast

logger = logging.getLogger(__name__)
click_log.basic_config(logger)


@click.command()
@click_log.simple_verbosity_option(logger)
@click.option("--sqlite_path", type=click.Path(exists=True), required=True)
@click.option("--mixs_core_path", type=click.Path(exists=True), required=True)
def cli(sqlite_path: str, mixs_core_path: str):
    """
    :param sqlite_path:
    :return:
    """

    mixs_core_frame = pd.read_csv(mixs_core_path, sep="\t")

    rto_vs_str = mixs_core_frame.loc[
        mixs_core_frame["Structured comment name"].eq("rel_to_oxygen"), "Value syntax"
    ].squeeze()

    temp = mixs_enum_to_list(rto_vs_str)

    logger.info(temp)

    # # no, the list elements aren't quote enclosed!
    # rto_vs_list = ast.literal_eval(rto_vs_str)

    # logger.info(rto_vs_list)

    conn = bsq.create_connection(sqlite_path)
    r2o_count_q = """
    select
	rel_to_oxygen,
	count(1) as r2o_count
from
	harmonized_wide_sel_envs hw
group by
	rel_to_oxygen
	order by count(1) desc;
	"""
    r2o_count_res = bsq.q_to_frame(conn, r2o_count_q)

    logger.info(r2o_count_res)


if __name__ == "__main__":
    cli()


def mixs_enum_to_list(mixs_enum: str) -> List[str]:
    temp1 = re.sub(r"^\[", "", mixs_enum)
    temp2 = re.sub(r"]$", "", temp1)
    as_list = temp2.split("|")
    return as_list
