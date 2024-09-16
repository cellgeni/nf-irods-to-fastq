#!/usr/bin/env python

import os
import sys
import json
import csv
import re
from typing import Set, List


def make_unique_names(
    sample_fastq_name: str, unique_tags: Set[int], fastq_names: List[str]
) -> str:
    """
    Creates a unique filename
    sample_fastq_name (str): Current fastq file name
    unique_tags (set[int]): A set of tags that are available for use
    fastq_names (list[str]): A list of fastq_names that are currently in use
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


def main():
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
            sample_meta["fastq_name"] = make_unique_names(
                sample_meta["fastq_name"], unique_tags, fastq_names
            )
            meta_list.append(sample_meta)

    # save the field names
    fieldnames = sample_meta.keys()

    # sort the the data by sample name
    meta_list = sorted(meta_list, key=lambda x: x["sample"])

    # write all metadata to csv
    with open("metadata.csv", mode="w") as csv_file:
        # create writer object
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

        # write the data
        writer.writeheader()
        for sample_meta in meta_list:
            writer.writerow(sample_meta)


if __name__ == "__main__":
    main()
