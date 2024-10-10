#!/usr/bin/env python

import os
import sys
import re
import json
from typing import List, Dict, Any
from collections import defaultdict

WARNING_COLOR = "\033[93m"
ENDC = "\033[0m"

META_COLUMNS = [
    "sample",
    "cram_path",
    "fastq_prefix",
    "sample_supplier_name",
    "library_type",
    "total_reads",
    "md5",
]


def parse_txt(filepath: str) -> Dict[str, Any]:
    """
    Parse metadata from imeta search saved in <filepath> file
    path (dict[str]): cram file metadata from irods
    """
    # read file with cram metadata
    with open(filepath, "r") as file:
        cram_path = (
            file.readline().strip(":\n").split(" ")[-1]
        )  # first line includes the path of cram file
        metadata_raw = file.read()

    # Here match the following pattern in metadata:
    ### attribute: ([\w:]+)
    ### value: ([\w\d\S ^b]+)

    pattern = re.compile(r"attribute: ([\w:]+)\nvalue: ([\w\d\S ^b]+)")
    meta_parsed = dict(pattern.findall(metadata_raw))

    # add the path of cram file to the meta
    meta_parsed["cram_path"] = cram_path
    return meta_parsed


def make_fastqprefix(metadata_list: List[Dict[str, Any]]) -> None:
    """
    Make a name for fastq file according to the CellRanger's naming convention
    https://www.10xgenomics.com/support/software/cell-ranger/latest/analysis/inputs/cr-specifying-fastqs
    meta (List[Dict[str]]): A metadata list for all cram files that are available for particular sample
    """
    # function that gets run_id + lane combination
    get_runid_lane = lambda meta: f"{meta['id_run']}_{meta.get('lane', '1')}"

    # sort metadata list
    sorted_metadata = sorted(metadata_list, key=get_runid_lane)

    # a dict to write unique tags for each unique runid_lane
    cram_dict = defaultdict(list)

    # make unique prefix
    for meta in sorted_metadata:
        # parse cram name to get run_id, lane and tag_id
        runid_lane, tag_id = get_runid_lane(meta), meta["tag_index"]
        # add runid_lane + tag_id combination to the dict
        cram_dict[runid_lane].append(tag_id)
        # make a name for fastq file
        sample_name, sample_idx, lane_idx = (
            meta["sample"],
            len(cram_dict[runid_lane]),
            str(len(cram_dict)),
        )
        meta["fastq_prefix"] = f"{sample_name}_S{sample_idx}_L{lane_idx.zfill(3)}"


def main() -> None:
    # read positional argument with file path
    dirpath = sys.argv[1].rstrip("/")

    # parse data for files in <dirpath>
    metadata_list = list()
    for filename in os.listdir(dirpath):
        filepath = f"{dirpath}/{filename}"
        metadata_list.append(parse_txt(filepath))

    # make unique fastq names
    make_fastqprefix(metadata_list)

    # parse the metadata
    for meta in metadata_list:
        # get a basename to save data
        basename = os.path.basename(meta["cram_path"]).rstrip(".cram")
        # filter metadata
        meta_filtered = {col: meta.get(col, "NaN") for col in META_COLUMNS}
        # Dump processed meta to json
        with open(f"{basename}.json", "w") as json_file:
            json.dump(meta_filtered, json_file)


if __name__ == "__main__":
    main()
