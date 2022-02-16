import logging
import sqlite3

import click
import click_log
import pandas as pd

logger = logging.getLogger(__name__)
click_log.basic_config(logger)


@click.command()
@click_log.simple_verbosity_option(logger)
@click.option("--sqlite_path", type=click.Path(exists=True), required=True)
@click.option("--tsv_out", type=click.Path(), required=True)
@click.option("--query", required=True)
def cli(sqlite_path: str, query: str, tsv_out: str):
    """
    :param sqlite_path:
    :return:
    """

    conn = create_connection(sqlite_path)

    res = q_to_frame(conn, query)

    logger.info(res)

    res.to_csv(tsv_out, sep="\t", index=False)


if __name__ == '__main__':
    cli()


def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by the db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Exception as e:
        print(e)

    return conn


def q_to_frame(conn, query):
    result_frame = pd.read_sql(query, conn, index_col=None, coerce_float=True, params=None, parse_dates=None,
                               columns=None,
                               chunksize=None)

    return result_frame
