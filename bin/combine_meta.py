#!/usr/bin/env python

import os
import sys
import json
import csv
from typing import Set, List, Dict, Any
from collections import defaultdict
import logging
import argparse


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


class ColoredFormatter(logging.Formatter):
    blue = "\n\033[94m"
    yellow = "\033[93m"
    reset = "\033[0m"
    format = "%(levelname)s: %(message)s"

    FORMATS = {
        logging.INFO: blue + format + reset,
        logging.WARNING: yellow + format + reset,
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)


def setup_logging():
    """
    Setup logging configuration of the script
    """
    # a basic config to save logs to metadata.log
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s: %(message)s",
        filename="metadata.log",
        filemode="w",
    )

    # define a Handler which writes INFO messages or higher to the sys.stderr
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.INFO)
    # tell the handler to use colored format
    console.setFormatter(ColoredFormatter())
    # add the handler to the root logger
    logging.getLogger("").addHandler(console)


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


def raise_sample_warning(sample: str, print_samplename: bool):
    """
    Checks if there were already any warning for a sample and prints a HEADER if there was none
    sample (str): a sample for which the warnings are raised
    warning_messages (List[str]): a list of warning for particular sample
    """
    if print_samplename:
        logging.info(f"Sample {sample}:")


def validate_consistency(
    sample: str, meta_list: List[Dict[str, Any]], column: str, print_samplename: bool
) -> bool:
    """
    Check if there are multiple values in `column`
    sample (str): sample name
    meta_list (List[Dict[str, Any]]): a list containing metadata for all files of a particular sample
    column (str): a column of interest in `meta_list`
    print_samplename: (bool): whether we should print a header with sample name
    """
    # get unique values
    unique_values = {meta.get(column, "NaN") for meta in meta_list}
    if len(unique_values) > 1:
        # print a header
        raise_sample_warning(sample, print_samplename)
        # make a warning message
        warning_message = (
            f"There are multiple values of {column} available:"
            + ",".join(unique_values)
        )
        logging.warning(warning_message)
        print_samplename = False
    return print_samplename


def validate_readcounts(
    sample: str, meta_list: List[Dict[str, Any]], print_samplename: bool
) -> bool:
    """
    Check if there IRODS total_counts equals samtools output
    sample (str): sample name
    meta_list (List[Dict[str, Any]]): a list containing metadata for all files of a particular sample
    print_samplename: (bool): whether we should print a header with sample name
    """
    # get samples with inconsistent total number of reads
    warning_list = [
        cram_meta["cram_path"]
        for cram_meta in meta_list
        if cram_meta["total_reads"] != cram_meta["num_reads_processed"]
    ]
    if warning_list:
        # print a header
        raise_sample_warning(sample, print_samplename)
        # make a warning message
        warning_message = (
            "IRODS total_count != num_reads_processed for files:"
            + ",".join(warning_list)
        )
        logging.warning(warning_message)
        print_samplename = False
    return print_samplename


def validate_atac(
    sample: str, meta_list: List[Dict[str, Any]], print_samplename: bool
) -> bool:
    """
    Check if there IRODS total_counts equals samtools output
    sample (str): sample name
    meta_list (List[Dict[str, Any]]): a list containing metadata for all files of a particular sample
    print_samplename: (bool): whether we should print a header with sample name
    """
    warning_list = [
        cram_meta["cram_path"]
        for cram_meta in meta_list
        if "atac" in cram_meta["library_type"].lower() and cram_meta["i2len"] == "24"
    ]
    if warning_list:
        # print a header
        raise_sample_warning(sample, print_samplename)
        # make a warning message
        warning_title = f"The following files are suspected to be 10X ATAC. They were renamed according to CellRanger naming convention :"
        warning_message = warning_title + ",".join(warning_list)
        logging.warning(warning_message)
        print_samplename = False
    return print_samplename


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
        print_samplename = True
        # subsample metadata list
        subsample_metalist = [meta_list[idx] for idx in indexes]
        # validate metadata
        print_samplename = validate_consistency(
            sample, subsample_metalist, "library_type", print_samplename
        )
        if validate_all:
            print_samplename = validate_readcounts(
                sample, subsample_metalist, print_samplename
            )
            print_samplename = validate_consistency(
                sample, subsample_metalist, "r1len", print_samplename
            )
            print_samplename = validate_consistency(
                sample, subsample_metalist, "r2len", print_samplename
            )
            print_samplename = validate_atac(
                sample, subsample_metalist, print_samplename
            )


def main() -> None:
    # set up logging
    setup_logging()

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
