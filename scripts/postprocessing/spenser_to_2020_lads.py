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



def check_combined_ssm(combined: pd.DataFrame, old: List[pd.DataFrame]):
    assert combined.shape[0] == sum([df.shape[0] for df in old])
    assert all([combined.shape[1] == df.shape[1] for df in old])
    if "HID" in combined.columns:
        assert combined["HID"].duplicated().sum() == 0
    if "PID" in combined.columns:
        assert combined["PID"].duplicated().sum() == 0
    assert combined.isna().any(axis=None) == False


def check_combined_ass_hh(combined: pd.DataFrame, old: List[pd.DataFrame]):
    assert combined.shape[0] == sum([df.shape[0] for df in old])
    assert all([combined.shape[1] == df.shape[1] for df in old])
    assert combined["HID"].duplicated().sum() == 0
    assert combined[combined["HRPID"]!=-1]["HRPID"].duplicated().sum() == 0
    # Only single area for a given HRPID code that is assigned should be present
    combined[combined["HRPID"]!=-1].groupby("HRPID")["Area"].nunique().eq(1).all()
    assert combined.isna().any(axis=None) == False

def check_combined_ass(combined: pd.DataFrame, old: List[pd.DataFrame]):
    assert combined.shape[0] == sum([df.shape[0] for df in old])
    assert all([combined.shape[1] == df.shape[1] for df in old])
    assert combined["PID"].duplicated().sum() == 0
    # Only single area for a given HID code that is assigned should be present
    assert combined[combined["HID"]!=-1].groupby("HID")["Area"].nunique().eq(1).all()
    assert combined.isna().any(axis=None) == False


def collate_ssm(code_map: Dict[str, List[str]], in_path: str, out_path: str):
    """Collate ssm outputs."""
    for new_code, old_codes in tqdm(code_map.items()):
        for year in list(range(2011, 2040)):
            ssm = [pd.read_csv(f"{in_path}/ssm_{old_code}_MSOA11_ppp_{year}.csv") for old_code in old_codes]
            ssm_hh = [pd.read_csv(f"{in_path}/ssm_hh_{old_code}_OA11_{year}.csv") for old_code in old_codes]
            (combined_ssm, combined_ssm_hh) = (ssm[0], ssm_hh[0])
            for (current_ssm, current_ssm_hh) in zip(ssm[1:], ssm_hh[1:]):
                # Make ID maps
                new_pid_start = combined_ssm["PID"].max() + 1
                new_hid_start = combined_ssm_hh["HID"].max() + 1
                pid_map = dict(zip(current_ssm["PID"].to_list(),list(range(new_pid_start, new_pid_start + current_ssm.shape[0]))))
                hid_map = dict(zip(current_ssm_hh["HID"].to_list(), list(range(new_hid_start, new_hid_start + current_ssm_hh.shape[0]))))
                hrpid_map = pid_map.copy()
                hrpid_map[-1] = -1

                # Update assignments IDS
                current_ssm["PID"] = current_ssm["PID"].map(pid_map)
                current_ssm_hh["HID"] = current_ssm_hh["HID"].map(hid_map)

                # Combine
                combined_ssm = pd.concat([combined_ssm, current_ssm])
                combined_ssm_hh = pd.concat([combined_ssm_hh, current_ssm_hh])
                

            # Write new outputs
            check_combined_ssm(combined_ssm, ssm)
            check_combined_ssm(combined_ssm_hh, ssm_hh)

            # NB. These assertions are not correct as assumed the data for housefolds was monotnic
            # in HID but the labels are not guranteed to be this way. Duplicated assertion
            # in `def check_combined()` is sufficient.
            # See this line of [static_hh.py](https://github.com/alan-turing-institute/microsimulation/blob/5897287756aadef002f34eca8349a3882870bfa4/microsimulation/static_h.py#L98-L103)
            #
            # assert list(combined_ssm["PID"].unique()) == list(range(0, combined_ssm.shape[0]))
            # assert list(combined_ssm_hh["HID"].unique()) == list(range(0, combined_ssm_hh.shape[0]))
            
            combined_ssm.to_csv(f"{out_path}/ssm_{new_code}_MSOA11_ppp_{year}.csv", index=False)
            combined_ssm_hh.to_csv(f"{out_path}/ssm_hh_{new_code}_OA11_{year}.csv", index=False)


def collate_ass(code_map: Dict[str, List[str]], in_path: str, out_path: str):
    """Collate ass outputs."""
    for new_code, old_codes in tqdm(code_map.items()):
        for year in [2012, 2020, 2022, 2032, 2039]:
            ssm = [pd.read_csv(f"{in_path}/ssm_{old_code}_MSOA11_ppp_{year}.csv") for old_code in old_codes]
            ssm_hh = [pd.read_csv(f"{in_path}/ssm_hh_{old_code}_OA11_{year}.csv") for old_code in old_codes]
            ass = [pd.read_csv(f"{in_path}/ass_{old_code}_MSOA11_{year}.csv") for old_code in old_codes]
            ass_hh = [pd.read_csv(f"{in_path}/ass_hh_{old_code}_OA11_{year}.csv") for old_code in old_codes]
            (combined_ssm, combined_ssm_hh, combined_ass, combined_ass_hh) = (ssm[0], ssm_hh[0], ass[0], ass_hh[0])
            for (current_ssm, current_ssm_hh, current_ass, current_ass_hh) in zip(ssm[1:], ssm_hh[1:], ass[1:], ass_hh[1:]):
                # Make ID maps using current max indices to guarantee unique ids
                # NB. Not guaranteed to be montonic as outputs from SPENSER (the first df)
                # are not.
                new_pid_start = combined_ssm["PID"].max() + 1
                new_hid_start = combined_ssm_hh["HID"].max() + 1
                pid_map = dict(zip(current_ssm["PID"].to_list(),list(range(new_pid_start, new_pid_start + current_ssm.shape[0]))))
                hid_map = dict(zip(current_ssm_hh["HID"].to_list(), list(range(new_hid_start, new_hid_start + current_ssm_hh.shape[0]))))
                hrpid_map = pid_map.copy()
                hrpid_map[-1] = -1

                # Update assignments IDS
                current_ssm["PID"] = current_ssm["PID"].map(pid_map)
                current_ssm_hh["HID"] = current_ssm_hh["HID"].map(hid_map)
                current_ass["PID"] = current_ass["PID"].map(pid_map)
                current_ass["HID"] = current_ass["HID"].map(hid_map)
                current_ass_hh["HID"] = current_ass_hh["HID"].map(hid_map)
                current_ass_hh["HRPID"] = current_ass_hh["HRPID"].map(hrpid_map)

                # Combine
                combined_ssm = pd.concat([combined_ssm, current_ssm])
                combined_ssm_hh = pd.concat([combined_ssm_hh, current_ssm_hh])
                combined_ass = pd.concat([combined_ass, current_ass])
                combined_ass_hh = pd.concat([combined_ass_hh, current_ass_hh])

            # Write new outputs
            check_combined_ssm(combined_ssm, ssm)
            check_combined_ssm(combined_ssm_hh, ssm_hh)
            check_combined_ass(combined_ass, ass)
            check_combined_ass_hh(combined_ass_hh, ass_hh)
            
            combined_ssm.to_csv(f"{out_path}/ssm_{new_code}_MSOA11_ppp_{year}_2.csv", index=False)
            combined_ssm_hh.to_csv(f"{out_path}/ssm_hh_{new_code}_OA11_{year}_2.csv", index=False)
            combined_ass.to_csv(f"{out_path}/ass_{new_code}_MSOA11_{year}.csv", index=False)
            combined_ass_hh.to_csv(f"{out_path}/ass_hh_{new_code}_OA11_{year}.csv", index=False)


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
    print("Collating 'ssm_*' outputs...")
    collate_ssm(code_map, args.data_in, args.data_out)
    print("Collating 'ass_*' outputs...")
    collate_ass(code_map, args.data_in, args.data_out)
    


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument(
        "--data_in",
        default="/Volumes/vmfileshare/SPENSER_pipeline/microsimulation/data/",
    )
    parser.add_argument(
        "--data_out",
        default="/Volumes/vmfileshare/SPENSER_pipeline_new_lads_2/microsimulation/data/",
    )
    parser.add_argument("--data_lookup", default="~/spc-hpc-pipeline/data/")
    parser.add_argument("--dry_run", action="store_true")
    args, unknown = parser.parse_known_args()

    print(args.__dict__)

    main(args)
