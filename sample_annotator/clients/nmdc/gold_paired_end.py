import os

import pandas as pd

from sample_annotator.clients.gold_client import GoldClient

from tests import INPUT_DIR

# path to GOLD API credentials file
KEYPATH = os.path.join(INPUT_DIR, "gold-key.txt")

if __name__ == "__main__":

    # path to sheet1 of paired end EMP500 data
    emp500_pe_sheet1_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "input",
        "EMP500_paired_end_summary_sheet1.tsv",
    )

    # load in csv into pandas df
    pe_summary_df1 = pd.read_csv(emp500_pe_sheet1_path, sep="\t")

    # remove underscore on Biosamples
    pe_summary_df1["Biosample"] = pe_summary_df1["Biosample"].apply(
        lambda x: x.split("_")[0]
    )

    # path to sheet2 of paired end EMP500 data
    emp500_pe_sheet2_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "input",
        "EMP500_paired_end_summary_sheet2.tsv",
    )

    # load in csv into pandas df
    pe_summary_df2 = pd.read_csv(emp500_pe_sheet2_path, sep="\t", header=0)

    # create instance of GoldClient()
    gc = GoldClient()

    gc.clear_cache()

    # check if API credentials file exists
    if os.path.exists(KEYPATH):
        gc.load_key(KEYPATH)
    else:
        raise FileNotFoundError(f"GOLD API credentials not found at: {KEYPATH}")

    # fetch all projects from EMP500 study on GOLD
    projects = gc.fetch_projects_by_study(id="gold:Gs0154244")

    # convert list of dicts into pandas df
    projects_df = pd.DataFrame(projects)

    # left merge on pe_summary_df1 to append columns from
    # common data between pe_summary_df1 and projects_df
    pe_gold_df1 = pd.merge(
        pe_summary_df1,
        projects_df,
        how="left",
        right_on=["ncbiBioSampleAccession"],
        left_on=["Biosample"],
    )

    # path to merged output for sheet1
    emp500_pe_merged_sheet1 = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "output",
        "EMP500_paired_end_merged_sheet1.csv",
    )

    cols_for_sheet1 = [
        "Biosample",
        "colA",
        "colB",
        "colC",
        "projectGoldId",
        "biosampleGoldId",
        "sequencingStrategy",
    ]

    pe_gold_df1.to_csv(emp500_pe_merged_sheet1, columns=cols_for_sheet1, index=False)

    # path to merged output for sheet2
    emp500_pe_merged_sheet2 = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "output",
        "EMP500_paired_end_merged_sheet2.csv",
    )

    cols_for_sheet2 = [
        "Biosample",
        "Reads",
        "Bases",
        "projectGoldId",
        "biosampleGoldId",
        "sequencingStrategy",
    ]

    # left merge on pe_summary_df2 to append columns from
    # common data between pe_summary_df2 and projects_df
    pe_gold_df2 = pd.merge(
        pe_summary_df2,
        projects_df,
        how="left",
        right_on=["ncbiBioSampleAccession"],
        left_on=["Biosample"],
    )

    pe_gold_df2.to_csv(emp500_pe_merged_sheet2, columns=cols_for_sheet2, index=False)
