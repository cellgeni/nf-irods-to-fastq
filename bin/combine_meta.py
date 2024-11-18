#!/usr/bin/env python3

import os
import sys
import json
import csv
import logging
import argparse
from colored_logger import setup_logging
from collections import defaultdict
from typing import Set, List, Dict, Any


def init_parser() -> argparse.ArgumentParser:
    """
    Initialise argument parser for the script
    """
    parser = argparse.ArgumentParser(
        description="Reads metadata from a set of .json files and combines everything to a .tsv file"
    )
    parser.add_argument(
        "dir",
        metavar="dir",
        type=str,
        help="specify a path to the directory with a set of .json files you want to combine",
    )
    parser.add_argument(
        "-a",
        "--validate_all",
        help="if specified runs all validation steps, if not runs library type validation only",
        action="store_true",
    )
    parser.add_argument(
        "--logfile",
        metavar="<file>",
        type=str,
        help="Specify a log file name",
        default="getmetadata.log",
    )
    parser.add_argument(
        "--filename",
        metavar="<file>",
        type=str,
        help="Specify a name for metadata file",
        default="metadata.tsv",
    )
    return parser


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
    filenames = [meta["fastq_prefix"] for meta in meta_list]
    duplicated_filenames = {name for name in filenames if filenames.count(name) > 1}
    # raise a warning
    if len(duplicated_filenames) != 0:
        message = "There are duplicated filenames:\n" + "\n".join(duplicated_filenames)
        logging.warning(message)


def raise_sample_warning(sample: str, warning_messages: List[str]) -> None:
    """
    Checks if there were already any warning for a sample and prints a HEADER if there was none
    sample (str): a sample for which the warnings are raised
    warning_messages (List[str]): a list of warning for particular sample
    """
    if warning_messages:
        # print a header with sample name
        logging.info(f"Sample {sample}:")
        # raise all warnings
        for message in warning_messages:
            logging.warning(message)


def validate_consistency(
    sample: str,
    meta_list: List[Dict[str, Any]],
    column: str,
    warning_messages: List[str],
) -> None:
    """
    Check if there are multiple values in `column`
    sample (str): sample name
    meta_list (List[Dict[str, Any]]): a list containing metadata for all files of a particular sample
    column (str): a column of interest in `meta_list`
    warning_messages: (List[str]): a list of warning messages from previous validation steps
    """
    # get unique values
    unique_values = {meta.get(column, "NaN") for meta in meta_list}
    if len(unique_values) > 1:
        # make a warning message
        warning_message = (
            f"There are multiple values of {column} available:"
            + ",".join(unique_values)
        )
        # save a warning message to the list
        warning_messages.append(warning_message)


def validate_readcounts(
    sample: str, meta_list: List[Dict[str, Any]], warning_messages: List[str]
) -> None:
    """
    Check if there IRODS total_counts equals samtools output
    sample (str): sample name
    meta_list (List[Dict[str, Any]]): a list containing metadata for all files of a particular sample
    warning_messages: (List[str]): a list of warning messages from previous validation steps
    """
    # get samples with inconsistent total number of reads
    warning_list = [
        cram_meta["cram_path"]
        for cram_meta in meta_list
        if cram_meta["total_reads"] != cram_meta["num_reads_processed"]
    ]
    if warning_list:
        # make a warning message
        warning_message = (
            "IRODS total_count != num_reads_processed for files:"
            + ",".join(warning_list)
        )
        # save a warning message to the list
        warning_messages.append(warning_message)


def validate_atac(
    sample: str, meta_list: List[Dict[str, Any]], warning_messages: List[str]
) -> None:
    """
    Check if there IRODS total_counts equals samtools output
    sample (str): sample name
    meta_list (List[Dict[str, Any]]): a list containing metadata for all files of a particular sample
    warning_messages: (List[str]): a list of warning messages from previous validation steps
    """
    warning_list = [
        cram_meta["cram_path"]
        for cram_meta in meta_list
        if "atac" in cram_meta["library_type"].lower() and cram_meta["i2len"] == "24"
    ]
    if warning_list:
        # make a warning message
        warning_title = f"The following files are suspected to be 10X ATAC. They were renamed according to CellRanger naming convention :"
        warning_message = warning_title + ",".join(warning_list)
        # save a warning message to the list
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
        validate_consistency(
            sample, subsample_metalist, "library_type", warning_messages
        )
        if validate_all:
            validate_readcounts(sample, subsample_metalist, warning_messages)
            validate_consistency(sample, subsample_metalist, "r1len", warning_messages)
            validate_consistency(sample, subsample_metalist, "r2len", warning_messages)
            validate_atac(sample, subsample_metalist, warning_messages)
        # raise all warning messages
        raise_sample_warning(sample, warning_messages)


def main() -> None:
    # parse script arguments
    parser = init_parser()
    args = parser.parse_args()

    # set up logger
    setup_logging(args.logfile)

    # read positional argument with filedir path
    dirpath = args.dir.rstrip("/")

    # read all json files to meta_list
    meta_list = list()

    for filename in os.listdir(dirpath):
        with open(f"{dirpath}/{filename}", "r") as file:
            # reading the json file
            sample_meta = json.load(file)
            # drop md5 column if it is final metadata
            if args.validate_all:
                del sample_meta["md5"]
            # save to list
            meta_list.append(sample_meta)

    # save the field names
    fieldnames = sample_meta.keys()

    # sort the the data by sample name
    meta_list = sorted(meta_list, key=lambda x: x["sample"])

    # validate metadata
    validate_metalist(meta_list, args.validate_all)

    # write all metadata to csv
    with open(args.filename, mode="w") as csv_file:
        # create writer object
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames, delimiter="\t")

        # write the data
        writer.writeheader()
        for sample_meta in meta_list:
            writer.writerow(sample_meta)


if __name__ == "__main__":
    main()
