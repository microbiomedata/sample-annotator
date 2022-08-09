# -*- coding: utf-8 -*-
import os
import pprint
import sqlite3
import unittest
from sqlite3 import Error

# MODEL_DIR, INPUT_DIR, OUTPUT_DIR
from tests import INPUT_DIR

"""Test the ability to read enumerated biosamples from the SQLite biosmaple database."""

"""Run as follows to get see test-time printouts:"""

"""python -m pytest -sv  tests/test_capitalization.py"""

# INPUT_DIR comes from __init__.py
PWD = os.path.dirname(os.path.realpath(__file__))
TEST_DATA = os.path.join(INPUT_DIR, "test_sample_info.yaml")

biosmaple_sqlite_file = "/Users/MAM/Documents/biosample_basex.db"

requireds = ["id", "part_of", "sample_link", "env_broad_scale", "env_local_scale", "env_medium"]


class TestBiosampleSqlite(unittest.TestCase):
    """biosample instantiation test."""

    def test_biosample_sqlite(self):
        conn = None

        try:
            conn = sqlite3.connect(biosmaple_sqlite_file)
            conn.row_factory = sqlite3.Row
        except Error as e:
            print(e)

        cursor = conn.cursor()

        accession_list = ["SAMN00000002", "SAMN00000003"]

        accession_core = "', '".join(accession_list)

        accession_tidy = f"('{accession_core}')"

        query = f"SELECT * FROM harmonized_wide hw join non_attribute_metadata nam on hw.raw_id = nam.raw_id where accession in {accession_tidy}"

        cursor.execute(query)

        rows = cursor.fetchall()

        rows_list = []
        for row in rows:
            row_dict = {}
            for r in requireds:
                try:
                    row_dict[r] = row[r]
                except Exception as e:
                    print(e)
            rows_list.append(row_dict)

        pprint.pprint(rows_list)

