#!/usr/bin/env python
# /// script
# requires-python = ">=3.14"
# dependencies = [
#   "duckdb",
#   "pandas[performance]",
#   "spinedb_api",
# ]
# ///
from argparse import ArgumentParser
import contextlib
import json
import re
import sqlite3

import pandas as pd
from spinedb_api import DatabaseMapping
from spinedb_api.dataframes import to_dataframe


query = """
SELECT
  pv.id,
  pd.name parameter,
  e.name entity,
  ec.name entity_class,
  pv.value
FROM parameter_value pv
LEFT JOIN
  entity_class ec,
  entity e,
  parameter_definition pd
ON
  pv.entity_class_id = ec.id AND
  pv.entity_id = e.id AND
  pv.parameter_definition_id = pd.id
WHERE
  entity_class = "model";
"""

param_defs = [
    "cost_annualized",
    "cost_annualized2",
    "cost_t",
    "cost_t_arr",
    "cost_t_ts",
    "cost_t_ts_var",
    "cost_t_map",
    "cost_t_map2",
    # "cost_t_tp",
]


def read_pvs(dbfile: str):
    with contextlib.closing(sqlite3.connect(dbfile)) as con:
        df = pd.read_sql(query, con)
    return df


# Regex pattern to indentify numerical sequences encoded as string
SEQ_PAT = re.compile(r"^(t|p)([0-9]+)$")


def parse_time(df: pd.DataFrame) -> pd.DataFrame:
    """Parse 'time' or 'period' columns to integers for plotting."""
    for col, _type in df.dtypes.items():
        if _type in (object, pd.StringDtype()) and (
            groups := df[col].str.extract(SEQ_PAT)
        ).notna().all(axis=None):
            df[col] = groups[1].astype(int)
    return df


def to_parquet(name: str, df: pd.DataFrame):
    fname = f"{name}.parquet"
    df.drop(
        columns=[
            "entity_class_name",
            "model",
            "parameter_definition_name",
            "alternative_name",
        ]
    ).to_parquet(fname, index=False)
    return {"name": name, "file_type": "parquet", "file": fname}


def spine_to_parquet(dbfile: str):
    with DatabaseMapping(f"sqlite:///{dbfile}") as db:
        tbl = db.mapped_table("parameter_value")
        files = [
            to_parquet(
                param,
                parse_time(
                    to_dataframe(
                        db.find(
                            tbl,
                            entity_class_name="model",
                            parameter_definition_name=param,
                        )[0]
                    )
                ),
            )
            for param in param_defs
        ]
    return files


def file_metadata_only():
    for param in param_defs:
        meta = {"name": param, "file_type": "parquet", "file": f"{param}.parquet"}
        print(json.dumps(meta))


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("dbfile")  # data/egypt-national.sqlite
    parser.add_argument(
        "--file-meta-only",
        action="store_true",
        help="Only print parquet file metadata.",
    )
    opts = parser.parse_args()

    if opts.file_meta_only:
        file_metadata_only()
    else:
        files = spine_to_parquet(opts.dbfile)
        for meta in files:
            print(json.dumps(meta))
