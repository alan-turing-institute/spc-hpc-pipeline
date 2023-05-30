#!/usr/bin/env python
# coding: utf-8

import glob
import os
from pathlib import Path
from typing import List

import pandas as pd


def extract_features(fname: str):
    """Extracts a list of type, region and year from the file name."""
    split = fname.split("_")
    if fname.startswith("ass_hh"):
        return ["_".join(split[:2]), split[2], split[4]]
    elif fname.startswith("ass"):
        return [split[0], split[1], split[3]]
    elif fname.startswith("ssm_hh"):
        return ["_".join(split[:2]), split[2], split[4]]
    elif fname.startswith("ssm"):
        return [split[0], split[1], split[4]]


def get_file_list(paths: List[str]) -> pd.DataFrame:
    """Searches list of paths for all csv outputs."""
    fs = []
    for path in paths:
        path = f"{path}/*/*/*.csv"
        fs.extend(glob.glob(path))
    df_files = pd.DataFrame(fs, columns=["full_path"])
    df_files["file_name"] = df_files["full_path"].map(lambda f: Path(f).stem)
    df_files = df_files.join(
        pd.DataFrame(
            [extract_features(file) for file in df_files.loc[:, "file_name"]],
            columns=["type", "region", "year"],
        )
    )
    df_files["year"] = df_files["year"].astype(int)
    return df_files


# Years to consider outputs
YEARS = [2012, 2020, 2022, 2032, 2039]

# LAD 2020 concat for GB
new_lad_list = pd.concat(
    [
        pd.read_csv("data/new_lad_list_England.csv"),
        pd.read_csv("data/new_lad_list_Wales.csv"),
        pd.read_csv("data/new_lad_list_Scotland.csv"),
    ]
)


# Output paths
data_paths = [
    "/mnt/vmfileshare/SPENSER_Outputs/outputs/",
    "/mnt/vmfileshare/SPENSER_missing_lads/outputs/",
]
regions = ["England", "Wales", "Scotland"]

# Get file list
df_files = get_file_list(data_paths)


# Combine with new_lad list
combined = new_lad_list.merge(
    df_files, left_on="LAD20CD", right_on="region", how="left"
)


# Only consider the assigned outputs
combined = combined[combined["type"].str.startswith("ass")]


# Check none are missing
assert combined[combined["full_path"].isna()].shape[0] == 0


# All outputs are present: 5 years and 2 file types per region
assert new_lad_list.shape[0] * 5 * 2 == combined.shape[0]


# Check non-empty
assert ~combined["full_path"].map(os.path.getsize).eq(0).any()


# Check regions are distinct
df_files_1 = get_file_list(data_paths[:1])
df_files_2 = get_file_list(data_paths[1:])
assert (
    len(set(df_files_1["region"].to_list()) & set(df_files_2["region"].to_list())) == 0
)
