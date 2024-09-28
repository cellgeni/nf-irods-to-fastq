#!/usr/bin/env python

import os
import sys
import json
import csv
import re
from typing import Set, List, Dict, Any
from collections import defaultdict


WARNING_COLOR = "\033[93m"
INFO_COLOR = "\n\033[94m"
ENDC = "\033[0m"


def make_unique_names(
    sample_fastq_name: str, unique_tags: Set[int], fastq_names: List[str]
) -> str:
    """
    Creates a unique filename
    sample_fastq_name (str): Current fastq file name
    unique_tags (set[int]): A set of tags that are available for use
    fastq_names (list[str]): A list of fastq_names that are currently in use
    return (str): a new unique basename for a fastq file
    """
    # get tag id from fastq file name
    sample_tag = re.search("_S(\d+)_", sample_fastq_name).group(1)

    # check if filename is already in use
    if sample_fastq_name in fastq_names:
        unique_tag = unique_tags.pop()
        sample_fastq_name = sample_fastq_name.replace(
            f"_S{sample_tag}_", f"_S{unique_tag}_"
        )
        print(
            f"Filename changed from {sample_fastq_name.replace(f'_S{unique_tag}_', f'_S{sample_tag}_')} to {sample_fastq_name}"
        )

    # add filename to the list and remove sample_tag from taglist
    fastq_names.append(sample_fastq_name)
    unique_tags = unique_tags - {int(sample_tag)}
    return sample_fastq_name


def add_read_type(library_type: str, i2len: str) -> Dict[str, str]:
    """
    Adds readtype names for all files
    library_type (str): a sequencing library type information
    i2len (str): an average length of index 2
    return (dict[str]): a dict of read types for a fastq file
    """
    if "atac" in library_type.lower() and i2len == "24":
        return {"I1": "I1", "I2": "R2", "R1": "R1", "R2": "R3"}
    else:
        return {"I1": "I1", "I2": "I2", "R1": "R1", "R2": "R2"}


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
        warning_title = f"{WARNING_COLOR}WARNING! IRODS total_count != num_reads_processed for samples: {ENDC}"
        warning_message = warning_title + ",".join(warning_list)
        warning_messages.append(warning_message)


def validate_metalist(meta_list: List[Dict[str, Any]]) -> None:
    """
    Validates metadata values in a list of columns
    meta_list (List[Dict[str, Any]]): a list containing metadata for all files of a particular sample
    warning_messages (List[str]): a list of warning for particular sample
    """
    # get sample indexes
    sample_index = get_sampleindex(meta_list)

    # validate cram files for each sample
    for sample, indexes in sample_index.items():
        warning_messages = list()
        # subsample metadata list
        subsample_metalist = [meta_list[idx] for idx in indexes]
        # validate metadata
        validate_consistency(subsample_metalist, "library_type", warning_messages)
        validate_readcounts(subsample_metalist, warning_messages)
        validate_consistency(subsample_metalist, "r1len", warning_messages)
        validate_consistency(subsample_metalist, "r2len", warning_messages)

        # raise warnings
        if warning_messages:
            print(f"{INFO_COLOR}INFO: Sample {sample}:{ENDC}")
            print(*warning_messages, sep="\n")


def main() -> None:
    # read positional argument with filedir path
    dirpath = sys.argv[1].strip("/")
    number_of_samples = len(os.listdir("./input/"))

    # read all json files to meta_list
    meta_list = list()
    fastq_names = list()
    unique_tags = set(range(1, number_of_samples))

    for filename in os.listdir(dirpath):
        with open(f"{dirpath}/{filename}", "r") as file:
            # reading the json file
            sample_meta = json.load(file)
            # making fastq_name unique
            if "num_reads_processed" in sample_meta.keys():
                sample_meta.update(
                    add_read_type(sample_meta["library_type"], sample_meta["i2len"])
                )
            else:
                sample_meta["fastq_name"] = make_unique_names(
                    sample_meta["fastq_name"], unique_tags, fastq_names
                )
            meta_list.append(sample_meta)

    # save the field names
    fieldnames = sample_meta.keys()

    # sort the the data by sample name
    meta_list = sorted(meta_list, key=lambda x: x["sample"])

    # validate metadata
    if "num_reads_processed" in sample_meta.keys():
        validate_metalist(meta_list)

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
