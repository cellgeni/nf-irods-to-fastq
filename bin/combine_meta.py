#!/usr/bin/env python

import os
import sys
import json
import csv
import re
from typing import Set, List, Dict, Any
from collections import defaultdict
import argparse


WARNING_COLOR = "\033[93m"
INFO_COLOR = "\n\033[94m"
ENDC = "\033[0m"

PARSER = argparse.ArgumentParser(
    description="Reads metadata from a set of .json files and combines everything to a .tsv file"
)
PARSER.add_argument(
    "dir",
    metavar="dir",
    type=str,
    help="specify a path to the directory with a set of .json files you want to combine",
)
PARSER.add_argument(
    "-a",
    "--validate_all",
    help="if specified runs all validation steps, if not runs library type validation only",
    action="store_true",
)


def get_sampleindex(meta_list: List[Dict[str, Any]]) -> Dict[str, int]:
    """
    Get an indexes of all unique samples
    meta_list (List[Dict[str, Any]]): a list containing metadata for all samples
    return (Dict[str, int]): indexes of all unique samples in the list
    """
    sample_index = defaultdict(list)
    for i, sample_meta in enumerate(meta_list):
        sample_index[sample_meta["sample"]].append(i)
    return sample_index


def validate_filenames(meta_list: List[Dict[str, Any]]) -> None:
    """
    Check if there are duplicated filenames of fastq files in the metadata list
    meta_list (List[Dict[str, Any]]): a list containing metadata for all samples
    """
    # get duplicated filenames
    filenames = [meta["fastq_name"] for meta in meta_list]
    duplicated_filenames = {name for name in filenames if filenames.count(name) > 1}
    # raise a warning
    if len(duplicated_filenames) != 0:
        print(f"{WARNING_COLOR}WARNING! There are duplicated filenames: {ENDC}")
        print(*duplicated_filenames, sep="\n")


def validate_consistency(
    meta_list: List[Dict[str, Any]], column: str, warning_messages: List[str]
) -> None:
    """
    Check if there are multiple values in `column`
    meta_list (List[Dict[str, Any]]): a list containing metadata for all files of a particular sample
    column (str): a column of interest in `meta_list`
    warning_messages (List[str]): a list of warning for particular sample
    """
    # get unique values
    unique_values = {meta.get(column, "NaN") for meta in meta_list}
    if len(unique_values) > 1:
        # make a warning message
        warning_title = f"{WARNING_COLOR}WARNING! There are multiple values of {column} available: {ENDC}"
        warning_message = warning_title + ",".join(unique_values)
        warning_messages.append(warning_message)


def validate_readcounts(
    meta_list: List[Dict[str, Any]], warning_messages: List[str]
) -> None:
    """
    Check if there IRODS total_counts equals samtools output
    meta_list (List[Dict[str, Any]]): a list containing metadata for all files of a particular sample
    warning_messages (List[str]): a list of warning for particular sample
    """
    # get samples with inconsistent total number of reads
    warning_list = [
        cram_meta["cram_path"]
        for cram_meta in meta_list
        if cram_meta["total_reads"] != cram_meta["num_reads_processed"]
    ]
    if warning_list:
        # make a warning message
        warning_title = f"{WARNING_COLOR}WARNING! IRODS total_count != num_reads_processed for files: {ENDC}"
        warning_message = warning_title + ",".join(warning_list)
        warning_messages.append(warning_message)


def validate_atac(meta_list: List[Dict[str, Any]], warning_messages: List[str]) -> None:
    """
    Check if there IRODS total_counts equals samtools output
    meta_list (List[Dict[str, Any]]): a list containing metadata for all files of a particular sample
    warning_messages (List[str]): a list of warning for particular sample
    """
    warning_list = [
        cram_meta["cram_path"]
        for cram_meta in meta_list
        if "atac" in cram_meta["library_type"].lower() and cram_meta["i2len"] == "24"
    ]
    if warning_list:
        # make a warning message
        warning_title = f"{WARNING_COLOR}WARNING! The following files are suspected to be 10X ATAC. They were renamed according to CellRanger naming convention : {ENDC}"
        warning_message = warning_title + ",".join(warning_list)
        warning_messages.append(warning_message)


def validate_metalist(meta_list: List[Dict[str, Any]], validate_all: bool) -> None:
    """
    Validates metadata values in a list of columns
    meta_list (List[Dict[str, Any]]): a list containing metadata for all files of a particular sample
    warning_messages (List[str]): a list of warning for particular sample
    """
    # get sample indexes
    sample_index = get_sampleindex(meta_list)

    # check if there are dulicated filenames
    validate_filenames(meta_list)

    # validate cram files for each sample
    for sample, indexes in sample_index.items():
        warning_messages = list()
        # subsample metadata list
        subsample_metalist = [meta_list[idx] for idx in indexes]
        # validate metadata
        validate_consistency(subsample_metalist, "library_type", warning_messages)
        if validate_all:
            validate_readcounts(subsample_metalist, warning_messages)
            validate_consistency(subsample_metalist, "r1len", warning_messages)
            validate_consistency(subsample_metalist, "r2len", warning_messages)
            validate_atac(subsample_metalist, warning_messages)

        # raise warnings
        if warning_messages:
            print(f"{INFO_COLOR}INFO: Sample {sample}:{ENDC}")
            print(*warning_messages, sep="\n")


def main() -> None:
    # parse arguments
    args = PARSER.parse_args()

    # read positional argument with filedir path
    dirpath = args.dir.strip("/")

    # read all json files to meta_list
    meta_list = list()

    for filename in os.listdir(dirpath):
        with open(f"{dirpath}/{filename}", "r") as file:
            # reading the json file
            sample_meta = json.load(file)
            meta_list.append(sample_meta)

    # save the field names
    fieldnames = sample_meta.keys()

    # sort the the data by sample name
    meta_list = sorted(meta_list, key=lambda x: x["sample"])

    # validate metadata
    validate_metalist(meta_list, args.validate_all)

    # write all metadata to csv
    with open("metadata.tsv", mode="w") as csv_file:
        # create writer object
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames, delimiter="\t")

        # write the data
        writer.writeheader()
        for sample_meta in meta_list:
            writer.writerow(sample_meta)


if __name__ == "__main__":
    main()
