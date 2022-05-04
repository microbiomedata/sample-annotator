# knowledge from MIxS (legacy?) Google Sheet
# quick and dirty with request for CSV
# could also use a utility like cogs or a package like pygsheets

# add typing

import pandas as pd
from quantulum3 import parser


# also implement something similar for getting slot attributes from LinkML
def get_df_from_gsheets_csv(sheet_id, tab_gid):
    gsheets_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&id={sheet_id}&gid={tab_gid}"
    df = pd.read_csv(gsheets_url)
    return df


def get_sqlite_colnames(con, table, do_sort=True):
    df = pd.read_sql_query(f"SELECT * from {table} limit 1", con)
    df_cols = list(df.columns)
    if do_sort:
        df_cols.sort()
    return df_cols


def do_q3_one_col(col_name, con, table):
    print(col_name)
    raw_df = pd.read_sql_query(f"SELECT {col_name} from {table}", con)

    raw_df_vc = raw_df[col_name].value_counts()
    raw_df_strings = list(raw_df_vc.index)
    raw_df_strings.sort()

    if len(raw_df_strings) > 0:

        q3_lod = []
        for i in raw_df_strings:
            q3_res = parser.parse(i)
            q3_hits = len(q3_res)
            if q3_hits == 1:
                q3h0 = q3_res[0]
                current_dict = {
                    "raw": i,
                    "surface": q3h0.surface,
                    "value": q3h0.value,
                    "unit_name": q3h0.unit.name,
                    "hit_count": 1,
                    "uncertainty": q3h0.uncertainty,
                }
                if q3h0.uncertainty:
                    current_dict["calc_min"] = q3h0.value - q3h0.uncertainty
                    current_dict["calc_max"] = q3h0.value + q3h0.uncertainty
                if q3h0.surface:
                    char_coverage = 1 - (abs(len(i) - len(q3h0.surface)) / len(i))
                else:
                    char_coverage = 0
                current_dict["char_coverage"] = char_coverage
                current_dict["column"] = col_name
            else:
                current_dict = {
                    "raw": i,
                    "hit_count": q3_hits,
                    "char_coverage": 0,
                    "column": col_name,
                }
            q3_lod.append(current_dict)
        q3_df = pd.DataFrame(q3_lod)

        vc_df = raw_df_vc.rename_axis("raw").reset_index(name="col_row_count")

        merged = vc_df.merge(right=q3_df, on="raw")

        return merged
