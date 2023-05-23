#!/usr/bin/env python
# coding: utf-8

import os
from argparse import ArgumentParser
from typing import Dict, List

import pandas as pd
from tqdm import tqdm


def get_spenser_codes(path: str) -> pd.DataFrame:
    """Returns a dataframe of 2016 LAD codes that SPENSER runs."""
    return pd.read_csv(f"{path}/spenser_lad_list.csv")


def get_new_lad_codes(path: str) -> pd.DataFrame:
    """Returns a dataframe of 2020 LAD codes for SPC."""
    return pd.concat(
        [
            pd.read_csv(f"{path}/new_lad_list_{region}.csv")
            for region in ["England", "Scotland", "Wales"]
        ]
    )


def get_lad_code_map(path: str) -> pd.DataFrame:
    """Returns a dataframe of changes in LAD codes 2009-2021."""
    return pd.read_csv(f"{path}/changes/old_to_new_mapping.csv")


def get_code_map(path: str) -> Dict[str, List[str]]:
    """Returns a dict of changes in LAD codes 2009-2021."""
    new_lad_codes = get_new_lad_codes(path)
    lad_code_map = get_lad_code_map(path)
    spenser_lad_codes = get_spenser_codes(path)
    codes_to_collate = new_lad_codes[
        ~new_lad_codes["LAD20CD"].isin(spenser_lad_codes["LAD code"])
    ]
    new_codes_from_old = {}
    for code in codes_to_collate["LAD20CD"]:
        new_codes_from_old[code] = lad_code_map[lad_code_map["Updated code"] == code][
            "Old code"
        ].to_list()
    return new_codes_from_old


def collate_ass(code_map: Dict[str, List[str]], args: ArgumentParser):
    """Collates ass prefix files from old to new LAD codes."""
    resolution = "MSOA11"
    for new_code, old_codes in tqdm(code_map.items()):
        for year in [2012, 2020, 2022, 2032, 2039]:
            old = [
                pd.read_csv(f"{args.data_in}/ass_{old_code}_{resolution}_{year}.csv")
                for old_code in old_codes
            ]
            combined = pd.concat(old)
            assert combined.shape[0] == sum([df.shape[0] for df in old])
            assert all([combined.shape[1] == df.shape[1] for df in old])
            if not args.dry_run:
                combined.to_csv(
                    f"{args.data_out}/ass_{new_code}_{resolution}_{year}.csv",
                    index=False,
                )


def collate_ass_hh(code_map: Dict[str, List[str]], args: ArgumentParser):
    """Collates ass_hh prefix files from old to new LAD codes."""
    resolution = "OA11"
    for new_code, old_codes in tqdm(code_map.items()):
        for year in [2012, 2020, 2022, 2032, 2039]:
            old = [
                pd.read_csv(f"{args.data_in}/ass_hh_{old_code}_{resolution}_{year}.csv")
                for old_code in old_codes
            ]
            combined = pd.concat(old)
            assert combined.shape[0] == sum([df.shape[0] for df in old])
            assert all([combined.shape[1] == df.shape[1] for df in old])
            if not args.dry_run:
                combined.to_csv(
                    f"{args.data_out}/ass_hh_{new_code}_{resolution}_{year}.csv",
                    index=False,
                )


def collate_ssm(code_map: Dict[str, List[str]], args: ArgumentParser):
    """Collates ssm prefix files from old to new LAD codes."""
    resolution = "MSOA11"
    for new_code, old_codes in tqdm(code_map.items()):
        for year in list(range(2011, 2040)):
            old = [
                pd.read_csv(
                    f"{args.data_in}/ssm_{old_code}_{resolution}_ppp_{year}.csv"
                )
                for old_code in old_codes
            ]
            combined = pd.concat(old)
            assert combined.shape[0] == sum([df.shape[0] for df in old])
            assert all([combined.shape[1] == df.shape[1] for df in old])
            if not args.dry_run:
                combined.to_csv(
                    f"{args.data_out}/ssm_{new_code}_{resolution}_ppp_{year}.csv",
                    index=False,
                )


def collate_ssm_hh(code_map: Dict[str, List[str]], args: ArgumentParser):
    """Collates ssm_hh prefix files from old to new LAD codes."""
    resolution = "OA11"
    for new_code, old_codes in tqdm(code_map.items()):
        for year in list(range(2011, 2040)):
            old = [
                pd.read_csv(f"{args.data_in}/ssm_hh_{old_code}_{resolution}_{year}.csv")
                for old_code in old_codes
            ]
            combined = pd.concat(old)
            assert combined.shape[0] == sum([df.shape[0] for df in old])
            assert all([combined.shape[1] == df.shape[1] for df in old])
            if not args.dry_run:
                combined.to_csv(
                    f"{args.data_out}/ssm_hh_{new_code}_{resolution}_{year}.csv",
                    index=False,
                )


def main(args: ArgumentParser):
    """Processes SPENSER outputs at old LADs and merges into new ones."""

    # Make out path
    os.makedirs(args.data_out, exist_ok=True)

    # Get codes
    new_lad_codes = get_new_lad_codes(args.data_lookup)
    spenser_lad_codes = get_spenser_codes(args.data_lookup)

    # Print regions to be updated
    print(new_lad_codes[~new_lad_codes["LAD20CD"].isin(spenser_lad_codes["LAD code"])])

    # Get code map
    code_map = get_code_map(args.data_lookup)

    # Perform collations
    print("Collating 'ass_*' outputs...")
    collate_ass(code_map, args)
    print("Collating 'ass_hh_*' outputs...")
    collate_ass_hh(code_map, args)
    print("Collating 'ssm_*' outputs...")
    collate_ssm(code_map, args)
    print("Collating 'ssm_hh_*' outputs...")
    collate_ssm_hh(code_map, args)


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument(
        "--data_in",
        default="/Volumes/vmfileshare/SPENSER_pipeline/microsimulation/data/",
    )
    parser.add_argument(
        "--data_out",
        default="/Volumes/vmfileshare/SPENSER_pipeline_new_lads/microsimulation/data/",
    )
    parser.add_argument("--data_lookup", default="~/spc-hpc-pipeline/data/")
    parser.add_argument("--dry_run", action="store_true")
    args, unknown = parser.parse_known_args()

    print(args.__dict__)

    main(args)
