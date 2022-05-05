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
# todo make one of these required
@click.option("--query_string")
@click.option("--query_file", type=click.Path(exists=True))
def cli(sqlite_path: str, query_string: str, query_file: str, tsv_out: str):
    """
    :param sqlite_path:
    :return:
    """

    print(f"opening {sqlite_path}")
    conn = create_connection(sqlite_path)
    print(conn)

    if query_file:
        file_obj = open(query_file)
        query_string = file_obj.read()
        print(query_string)

    if query_string:
        res = q_to_frame(conn, query_string)
        res.to_csv(tsv_out, sep="\t", index=False)


if __name__ == "__main__":
    cli()


def create_connection(db_file):
    """create a database connection to the SQLite database
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
    result_frame = pd.read_sql(
        query,
        conn,
        index_col=None,
        coerce_float=True,
        params=None,
        parse_dates=None,
        columns=None,
        chunksize=None,
    )

    return result_frame
